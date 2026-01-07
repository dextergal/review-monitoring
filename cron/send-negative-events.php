<?php

/**

 * /var/www/review-monitor/cron/send-negative-events.php

 *

 * Sends pending negative_review_events into HubSpot by:

 * 1) finding/creating a Company

 * 2) updating company properties (to trigger workflows)

 * 3) marking the event as sent/failed in DB

 *

 * NO array_is_list() used anywhere.

 */



declare(strict_types=1);



date_default_timezone_set('UTC');



$BASE_DIR = dirname(__DIR__); // /var/www/review-monitor

$LOG_FILE = $BASE_DIR . '/logs/hubspot-events.log';



require_once $BASE_DIR . '/config/database.php';

// Create this file if you don't have it yet:

$hubspotConfigPath = $BASE_DIR . '/config/hubspot.php';

if (!file_exists($hubspotConfigPath)) {

    logLine($LOG_FILE, "ERROR: Missing config file: {$hubspotConfigPath}");

    exit(1);

}

$hubConfig = require $hubspotConfigPath;



$dbConfig = require $BASE_DIR . '/config/database.php';



$hubToken = (string)($hubConfig['access_token'] ?? '');

if ($hubToken === '') {

    logLine($LOG_FILE, "ERROR: HubSpot access_token missing in config/hubspot.php");

    exit(1);

}



// Tuning

$BATCH_LIMIT = (int)($hubConfig['batch_limit'] ?? 20);

$MAX_ATTEMPTS_PER_EVENT = (int)($hubConfig['max_attempts'] ?? 5);



// Company property internal names (must exist in HubSpot)

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



// Fetch pending events

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

    $eventId    = (int)$ev['id'];

    $businessId = (int)$ev['business_id'];

    $reviewId   = (string)$ev['review_id'];

    $rating     = (int)$ev['rating'];

    $snippet    = (string)($ev['review_snippet'] ?? '');

    $reviewDate = (string)($ev['review_date'] ?? '');

    $source     = (string)($ev['source'] ?? 'Google');



    // From businesses table

    $businessName   = (string)($ev['business_name'] ?? '');

    $googlePlaceId  = (string)($ev['place_id'] ?? '');  // adjust column alias below if yours differs

    $businessCity   = (string)($ev['city'] ?? '');

    $businessState  = (string)($ev['state'] ?? '');

    $businessCountry= (string)($ev['country'] ?? '');

    $reviewUrl      = (string)($ev['review_url'] ?? '');



    logLine($LOG_FILE, "Sending event_id={$eventId} review_id={$reviewId} rating={$rating} business={$businessName}");



    // Mark attempt

    incrementAttempt($pdo, $eventId);



    // Basic sanity checks

    if ($businessName === '' || $googlePlaceId === '') {

        markFailed($pdo, $eventId, "Missing business_name or google place_id; cannot map to HubSpot company.");

        $failed++;

        continue;

    }



    // 1) Find or create company

    $companyId = null;



    // Try search by google_place_id first

    $companyId = hubspotFindCompanyIdByProperty(

        $hubToken,

        $PROP_GOOGLE_PLACE_ID,

        $googlePlaceId

    );



    // If not found, create

    if (!$companyId) {

        $createProps = [

            'name' => $businessName,

            $PROP_GOOGLE_PLACE_ID => $googlePlaceId,

        ];



        // Optional helpful fields

        if ($businessCity)    $createProps['city']    = $businessCity;

        if ($businessState)   $createProps['state']   = $businessState;

        if ($businessCountry) $createProps['country'] = $businessCountry;



        $companyId = hubspotCreateCompany($hubToken, $createProps);



        if (!$companyId) {

            markFailed($pdo, $eventId, "Failed to create company in HubSpot.");

            $failed++;

            continue;

        }

    }



    // 2) Update company properties to trigger workflow

    $hsDateMs = toHubspotDatetimeMs($reviewDate); // can be null if invalid

    $updateProps = [

        $PROP_LAST_NEG_RATING => $rating,

        $PROP_LAST_NEG_TEXT   => trim(mb_substr($snippet, 0, 1000)),

    ];

    if ($hsDateMs !== null) {

        $updateProps[$PROP_LAST_NEG_DATE] = $hsDateMs;

    }

    if ($reviewUrl !== '') {

        $updateProps[$PROP_LAST_NEG_URL] = $reviewUrl;

    }



    $ok = hubspotUpdateCompany($hubToken, $companyId, $updateProps);



    if (!$ok) {

        // Mark failed but allow retry (don’t mark non-retryable blindly)

        markFailed($pdo, $eventId, "Failed to update company properties in HubSpot.");

        $failed++;

        continue;

    }



    // 3) Mark as sent

    markSent($pdo, $eventId);

    $sent++;

}



logLine($LOG_FILE, "Run complete. Sent={$sent}, Failed={$failed}");

logLine($LOG_FILE, "---- HubSpot sender run end ----");



/* =========================================================

   DB Helpers

   ========================================================= */



function dbConnect(array $cfg): PDO

{

    // Accept multiple key styles to avoid "Undefined array key" issues

    $host = (string)($cfg['host'] ?? $cfg['db_host'] ?? 'localhost');

    $db   = (string)($cfg['database'] ?? $cfg['dbname'] ?? $cfg['db'] ?? '');

    $user = (string)($cfg['username'] ?? $cfg['user'] ?? '');

    $pass = (string)($cfg['password'] ?? $cfg['pass'] ?? '');



    if ($db === '' || $user === '') {

        throw new RuntimeException("database/user missing in config/database.php");

    }



    $dsn = "mysql:host={$host};dbname={$db};charset=utf8mb4";



    return new PDO($dsn, $user, $pass, [

        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,

        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,

        PDO::ATTR_EMULATE_PREPARES => false,

    ]);

}



function fetchPendingEvents(PDO $pdo, int $limit, int $maxAttempts): array

{

    // NOTE: adjust b.place_id column if your businesses table uses a different column name.

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

            b.name      AS business_name,

            b.place_id  AS place_id,

            b.location  AS location

        FROM negative_review_events e

        JOIN businesses b ON b.id = e.business_id

        WHERE e.send_status = 'pending'

          AND e.send_attempts < :maxAttempts

        ORDER BY e.id ASC

        LIMIT :lim

    ";



    $stmt = $pdo->prepare($sql);

    $stmt->bindValue(':maxAttempts', $maxAttempts, PDO::PARAM_INT);

    $stmt->bindValue(':lim', $limit, PDO::PARAM_INT);

    $stmt->execute();



    return $stmt->fetchAll() ?: [];

}



function incrementAttempt(PDO $pdo, int $eventId): void

{

    $stmt = $pdo->prepare("UPDATE negative_review_events SET send_attempts = send_attempts + 1 WHERE id = :id");

    $stmt->execute([':id' => $eventId]);

}



function markSent(PDO $pdo, int $eventId): void

{

    $stmt = $pdo->prepare("

        UPDATE negative_review_events

        SET send_status = 'sent',

            sent_at = NOW()

        WHERE id = :id

    ");

    $stmt->execute([':id' => $eventId]);

}



function markFailed(PDO $pdo, int $eventId, string $reason): void

{

    // Keep it simple: mark failed. You can add a send_error column later if you want.

    $stmt = $pdo->prepare("

        UPDATE negative_review_events

        SET send_status = 'failed'

        WHERE id = :id

    ");

    $stmt->execute([':id' => $eventId]);

}



/* =========================================================

   HubSpot API Helpers

   ========================================================= */



function hubspotFindCompanyIdByProperty(string $token, string $propertyName, string $value): ?string

{

    $url = 'https://api.hubapi.com/crm/v3/objects/companies/search';



    $payload = [

        'filterGroups' => [[

            'filters' => [[

                'propertyName' => $propertyName,

                'operator' => 'EQ',

                'value' => $value,

            ]]

        ]],

        'properties' => ['name', $propertyName],

        'limit' => 1,

    ];



    $resp = hubspotRequest($token, 'POST', $url, $payload);



    if (!$resp['ok']) {

        return null;

    }



    $body = $resp['body'];

    if (!isset($body['results'][0]['id'])) {

        return null;

    }



    return (string)$body['results'][0]['id'];

}



function hubspotCreateCompany(string $token, array $properties): ?string

{

    $url = 'https://api.hubapi.com/crm/v3/objects/companies';



    $payload = [

        'properties' => $properties,

    ];



    $resp = hubspotRequest($token, 'POST', $url, $payload);



    if (!$resp['ok']) {

        return null;

    }



    return isset($resp['body']['id']) ? (string)$resp['body']['id'] : null;

}



function hubspotUpdateCompany(string $token, string $companyId, array $properties): bool

{

    $url = 'https://api.hubapi.com/crm/v3/objects/companies/' . rawurlencode($companyId);



    $payload = [

        'properties' => $properties,

    ];



    $resp = hubspotRequest($token, 'PATCH', $url, $payload);



    return (bool)$resp['ok'];

}



/**

 * Generic HubSpot request with basic retry for 429/5xx

 */

function hubspotRequest(string $token, string $method, string $url, array $payload = null): array

{

    $maxTries = 3;



    for ($try = 1; $try <= $maxTries; $try++) {

        $ch = curl_init($url);



        $headers = [

            'Authorization: Bearer ' . $token,

            'Content-Type: application/json',

            'Accept: application/json',

        ];



        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);

        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $method);

        curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);

        curl_setopt($ch, CURLOPT_TIMEOUT, 30);



        if ($payload !== null) {

            curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($payload));

        }



        $raw = curl_exec($ch);

        $err = curl_error($ch);

        $code = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);

        curl_close($ch);



        if ($raw === false) {

            // cURL error – retry

            if ($try < $maxTries) {

                usleep(300000);

                continue;

            }

            return ['ok' => false, 'http' => 0, 'body' => null, 'raw' => null, 'error' => $err];

        }



        $decoded = json_decode($raw, true);



        // Retry on rate limit or transient errors

        if ($code === 429 || ($code >= 500 && $code <= 599)) {

            if ($try < $maxTries) {

                // Respect Retry-After if HubSpot gives it (not shown here), otherwise backoff

                usleep(500000 * $try);

                continue;

            }

        }



        // Success

        if ($code >= 200 && $code <= 299) {

            return ['ok' => true, 'http' => $code, 'body' => $decoded, 'raw' => $raw, 'error' => null];

        }



        // Non-2xx

        return ['ok' => false, 'http' => $code, 'body' => $decoded, 'raw' => $raw, 'error' => null];

    }



    return ['ok' => false, 'http' => 0, 'body' => null, 'raw' => null, 'error' => 'Unknown'];

}



/* =========================================================

   Utility

   ========================================================= */



function toHubspotDatetimeMs(string $date): ?int

{

    // Accept MySQL DATETIME 'YYYY-mm-dd HH:ii:ss' or ISO-ish

    $date = trim($date);

    if ($date === '') return null;



    $ts = strtotime($date);

    if ($ts === false) return null;



    return $ts * 1000; // HubSpot expects milliseconds

}



function logLine(string $file, string $msg): void

{

    $line = '[' . date('Y-m-d H:i:s') . '] ' . $msg . PHP_EOL;

    @file_put_contents($file, $line, FILE_APPEND);

}

