<?php
/**
 * /var/www/review-monitor/cron/send-negative-events.php
 *
 * Sends pending negative_review_events into HubSpot by:
 * 1) finding/creating a Company (matched by google_place_id)
 * 2) updating company properties (to trigger workflows)
 * 3) marking the event as sent/failed in DB
 */

declare(strict_types=1);

date_default_timezone_set('UTC');


$BASE_DIR = dirname(__DIR__);

// Load .env file
$envPath = $BASE_DIR . '/.env';
if (file_exists($envPath)) {
    foreach (file($envPath, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) as $line) {
        if (str_starts_with(trim($line), '#') || !str_contains($line, '=')) {
            continue;
        }
        [$key, $value] = explode('=', $line, 2);
        $_ENV[trim($key)] = trim($value);
    }
}

$LOG_FILE = $BASE_DIR . '/logs/hubspot-events.log';

require_once $BASE_DIR . '/config/database.php';

$hubspotConfigPath = $BASE_DIR . '/config/hubspot.php';
if (!file_exists($hubspotConfigPath)) {
    logLine($LOG_FILE, "ERROR: Missing config file: {$hubspotConfigPath}");
    exit(1);
}

$hubConfig = require $hubspotConfigPath;
$dbConfig  = require $BASE_DIR . '/config/database.php';

$hubToken = (string)($hubConfig['access_token'] ?? '');
if ($hubToken === '') {
    logLine($LOG_FILE, "ERROR: HubSpot access_token missing");
    exit(1);
}

// Tuning
$BATCH_LIMIT = (int)($hubConfig['batch_limit'] ?? 20);
$MAX_ATTEMPTS_PER_EVENT = (int)($hubConfig['max_attempts'] ?? 5);

// HubSpot company property internal names
$PROP_GOOGLE_PLACE_ID = (string)($hubConfig['company_prop_google_place_id'] ?? 'google_place_id');
$PROP_LAST_NEG_RATING = (string)($hubConfig['company_prop_last_neg_rating'] ?? 'last_negative_review_rating');
$PROP_LAST_NEG_TEXT   = (string)($hubConfig['company_prop_last_neg_text'] ?? 'last_negative_review_text');
$PROP_LAST_NEG_DATE   = (string)($hubConfig['company_prop_last_neg_date'] ?? 'last_negative_review_date');
$PROP_LAST_NEG_URL    = (string)($hubConfig['company_prop_last_neg_url'] ?? 'last_negative_review_url');

logLine($LOG_FILE, "---- HubSpot sender run start ----");

try {
    $pdo = dbConnect($dbConfig);
} catch (Throwable $e) {
    logLine($LOG_FILE, "ERROR: DB connect failed: " . $e->getMessage());
    exit(1);
}

$events = fetchPendingEvents($pdo, $BATCH_LIMIT, $MAX_ATTEMPTS_PER_EVENT);
logLine($LOG_FILE, "Events fetched: " . count($events));

if (!$events) {
    logLine($LOG_FILE, "No events to send.");
    logLine($LOG_FILE, "---- HubSpot sender run end ----");
    exit(0);
}

$sent = 0;
$failed = 0;

foreach ($events as $ev) {

    $eventId      = (int)$ev['id'];
    $reviewId     = (string)$ev['review_id'];
    $rating       = (int)$ev['rating'];
    $snippet      = (string)($ev['review_snippet'] ?? '');
    $reviewDate   = (string)($ev['review_date'] ?? '');
    $reviewUrl    = (string)($ev['review_url'] ?? '');

    $businessName  = (string)($ev['business_name'] ?? '');
    $googlePlaceId = (string)($ev['place_id'] ?? '');

    logLine($LOG_FILE, "Sending event_id={$eventId} review_id={$reviewId} rating={$rating} business={$businessName}");
    logLine($LOG_FILE, "DEBUG: entered loop for event {$eventId}");

    incrementAttempt($pdo, $eventId);

    if ($businessName === '' || $googlePlaceId === '') {
        markFailed($pdo, $eventId);
        $failed++;
        continue;
    }

    // 1) Find or create company (STRICT by google_place_id)
    $companyId = hubspotFindCompanyIdByProperty(
        $hubToken,
        $PROP_GOOGLE_PLACE_ID,
        $googlePlaceId
    );
    
    

    if (!$companyId) {
        $companyId = hubspotCreateCompany($hubToken, [
            'name' => $businessName,
            $PROP_GOOGLE_PLACE_ID => $googlePlaceId,
        ]);

        if (!$companyId) {
            markFailed($pdo, $eventId);
            $failed++;
            continue;
        }
    }

    // 2) Update company properties
    $updateProps = [
        $PROP_LAST_NEG_RATING => $rating,
        $PROP_LAST_NEG_TEXT   => trim(mb_substr($snippet, 0, 1000)),
    ];

    $hsDateMs = toHubspotDatetimeMs($reviewDate);
    if ($hsDateMs !== null) {
        $updateProps[$PROP_LAST_NEG_DATE] = $hsDateMs;
    }

    if ($reviewUrl !== '') {
        $updateProps[$PROP_LAST_NEG_URL] = $reviewUrl;
    }

    $ok = hubspotUpdateCompany($hubToken, $companyId, $updateProps);

    if (!$ok) {
        markFailed($pdo, $eventId);
        $failed++;
        continue;
    }

    markSent($pdo, $eventId);
    $sent++;
}

logLine($LOG_FILE, "Run complete. Sent={$sent}, Failed={$failed}");
logLine($LOG_FILE, "---- HubSpot sender run end ----");

/* =========================
   DB Helpers
   ========================= */

function dbConnect(array $cfg): PDO {
    $dsn = sprintf(
        "mysql:host=%s;dbname=%s;charset=utf8mb4",
        $cfg['host'],
        $cfg['dbname']
    );

    return new PDO(
        $dsn,
        $cfg['user'],
        $cfg['pass'],
        [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::ATTR_EMULATE_PREPARES => false,
        ]
    );
}


function fetchPendingEvents(PDO $pdo, int $limit, int $maxAttempts): array
{
    // Force integer (SQL-safe)
    $limit = (int)$limit;

    $sql = "
        SELECT
            e.id,
            e.business_id,
            e.review_id,
            e.rating,
            e.review_snippet,
            e.review_date,
            e.source,
            e.review_url,
            e.send_status,
            e.send_attempts,
            e.sent_at,
            b.name     AS business_name,
            b.place_id AS place_id
        FROM negative_review_events e
        JOIN businesses b ON b.id = e.business_id
        WHERE e.send_status = 'pending'
          AND e.send_attempts < :maxAttempts
        ORDER BY e.id ASC
        LIMIT $limit
    ";

    $stmt = $pdo->prepare($sql);
    $stmt->bindValue(':maxAttempts', $maxAttempts, PDO::PARAM_INT);
    $stmt->execute();

    return $stmt->fetchAll() ?: [];
}


function incrementAttempt(PDO $pdo, int $eventId): void {
    $pdo->prepare(
        "UPDATE negative_review_events SET send_attempts = send_attempts + 1 WHERE id = :id"
    )->execute([':id' => $eventId]);
}

function markSent(PDO $pdo, int $eventId): void {
    $pdo->prepare(
        "UPDATE negative_review_events SET send_status='sent', sent_at=NOW() WHERE id=:id"
    )->execute([':id' => $eventId]);
}

function markFailed(PDO $pdo, int $eventId): void {
    $pdo->prepare(
        "UPDATE negative_review_events SET send_status='failed' WHERE id=:id"
    )->execute([':id' => $eventId]);
}

/* =========================
   HubSpot Helpers
   ========================= */

function hubspotFindCompanyIdByProperty(string $token, string $property, string $value): ?string {
    $resp = hubspotRequest(
        $token,
        'POST',
        'https://api.hubapi.com/crm/v3/objects/companies/search',
        [
            'filterGroups' => [[
                'filters' => [[
                    'propertyName' => $property,
                    'operator' => 'EQ',
                    'value' => $value
                ]]
            ]],
            'limit' => 1
        ]
    );



    return $resp['ok'] && isset($resp['body']['results'][0]['id'])
        ? (string)$resp['body']['results'][0]['id']
        : null;
}

function hubspotCreateCompany(string $token, array $properties): ?string {
    $resp = hubspotRequest(
        $token,
        'POST',
        'https://api.hubapi.com/crm/v3/objects/companies',
        ['properties' => $properties]
    );

    return $resp['ok'] ? (string)($resp['body']['id'] ?? null) : null;
}

function hubspotUpdateCompany(string $token, string $companyId, array $properties): bool {
    $resp = hubspotRequest(
        $token,
        'PATCH',
        'https://api.hubapi.com/crm/v3/objects/companies/' . rawurlencode($companyId),
        ['properties' => $properties]
    );

    return (bool)$resp['ok'];
}

function hubspotRequest(string $token, string $method, string $url, array $payload = null): array {
    $ch = curl_init($url);

    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_CUSTOMREQUEST  => $method,
        CURLOPT_HTTPHEADER     => [
            'Authorization: Bearer ' . $token,
            'Content-Type: application/json',
            'Accept: application/json'
        ],
        CURLOPT_TIMEOUT        => 30,
    ]);

    if ($payload !== null) {
        curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($payload));
    }

    $raw  = curl_exec($ch);
    $code = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);

    $body = json_decode((string)$raw, true);

    if ($code >= 400) {
        file_put_contents(
            __DIR__ . '/../logs/hubspot-debug.log',
            json_encode([
                'url' => $url,
                'payload' => $payload,
                'http' => $code,
                'response' => $body,
            ], JSON_PRETTY_PRINT) . PHP_EOL,
            FILE_APPEND
        );
    }    

    return [
        'ok'   => $code >= 200 && $code < 300,
        'http' => $code,
        'body' => $body,
    ];
}

/* =========================
   Utilities
   ========================= */

function toHubspotDatetimeMs(string $date): ?int {
    $date = trim($date);
    if ($date === '') return null;

    if (is_numeric($date)) {
        $ts = strlen($date) > 10 ? (int)($date / 1000) : (int)$date;
        $dt = new DateTime('@' . $ts);
    } else {
        $dt = new DateTime($date);
    }

    $dt->setTimezone(new DateTimeZone('UTC'));
    $dt->setTime(0, 0, 0);

    return $dt->getTimestamp() * 1000;
}

function logLine(string $file, string $msg): void {
    file_put_contents(
        $file,
        '[' . date('Y-m-d H:i:s') . '] ' . $msg . PHP_EOL,
        FILE_APPEND
    );
}

logLine($LOG_FILE, 'HS token loaded: ' . (strlen($hubToken) > 20 ? 'YES' : 'NO'));
