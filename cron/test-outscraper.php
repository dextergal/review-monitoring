<?php

/**

 * /var/www/review-monitor/cron/test-outscraper.php

 *

 * Fetch reviews from Outscraper for active businesses and insert into DB.

 * No array_is_list() anywhere.

 */



declare(strict_types=1);



ini_set('display_errors', '1');

error_reporting(E_ALL);



require_once __DIR__ . '/../src/OutscraperClient.php';



$dbConfig  = require __DIR__ . '/../config/database.php';

$outConfig = require __DIR__ . '/../config/outscraper.php';



/**

 * Get config value supporting multiple possible key names.

 */

function cfg(array $arr, array $keys, $default = null) {

    foreach ($keys as $k) {

        if (array_key_exists($k, $arr) && $arr[$k] !== null && $arr[$k] !== '') {

            return $arr[$k];

        }

    }

    return $default;

}



/**

 * Normalize date to MySQL DATETIME (Y-m-d H:i:s) or null.

 */

function toMysqlDateTime($value): ?string {

    if ($value === null || $value === '') return null;



    // If numeric timestamp (seconds)

    if (is_numeric($value)) {

        $ts = (int)$value;

        if ($ts > 0) return gmdate('Y-m-d H:i:s', $ts);

    }



    $ts = strtotime((string)$value);

    if ($ts === false) return null;



    // store UTC-ish; change to date('Y-m-d H:i:s', $ts) if you want server local time

    return gmdate('Y-m-d H:i:s', $ts);

}



/**

 * Extract reviews array from Outscraper response body.

 * Supports: body['data'][0]['reviews_data'] (typical), or direct list.

 */

function extractReviews($body): array {

    if (!is_array($body)) return [];



    // Typical Outscraper structure:

    // body['data'] = [ [ 'reviews_data' => [ ... ] ] ]

    if (isset($body['data']) && is_array($body['data']) && isset($body['data'][0]) && is_array($body['data'][0])) {

        $first = $body['data'][0];



        if (isset($first['reviews_data']) && is_array($first['reviews_data'])) {

            return $first['reviews_data'];

        }



        // sometimes it can be nested differently

        if (isset($first['reviews']) && is_array($first['reviews'])) {

            return $first['reviews'];

        }

    }



    // If body itself is a list of reviews

    $isList = true;

    $i = 0;

    foreach ($body as $k => $_v) {

        if ($k !== $i) { $isList = false; break; }

        $i++;

    }

    if ($isList) return $body;



    return [];

}



/**

 * Map one Outscraper review row into DB fields.

 */

function mapReviewRow(array $r): array {

    // IDs: prefer review_id, else reviews_id, else google_review_id

    $reviewId = cfg($r, ['review_id', 'reviews_id', 'google_review_id', 'id'], null);

    if ($reviewId === null) {

        // last resort: try review_link hash-ish

        $reviewId = cfg($r, ['review_link'], null);

    }



    $rating = (int)cfg($r, ['review_rating', 'rating', 'stars'], 0);



    $author = (string)cfg($r, ['author_title', 'author_name', 'author', 'user_name'], '');

    $text   = (string)cfg($r, ['review_text', 'text', 'review'], '');



    // Date fields used by Outscraper vary

    $dt = cfg($r, ['review_datetime_utc', 'review_datetime', 'review_date', 'review_created_at', 'review_timestamp_datetime_utc', 'review_timestamp'], null);

    $reviewDate = toMysqlDateTime($dt);



    $isNegative = ($rating >= 1 && $rating <= 3) ? 1 : 0;



    return [

        'review_id'    => (string)$reviewId,

        'rating'       => $rating,

        'author'       => $author,

        'review_text'  => $text,

        'review_date'  => $reviewDate,

        'is_negative'  => $isNegative,

        'raw_json'     => json_encode($r, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),

    ];

}



/* -----------------------

 * PDO connect

 * --------------------- */

$dbHost = (string)cfg($dbConfig, ['host', 'db_host', 'hostname'], 'localhost');

$dbName = (string)cfg($dbConfig, ['database', 'db', 'dbname', 'db_name', 'name'], '');

$dbUser = (string)cfg($dbConfig, ['username', 'user', 'db_user'], '');

$dbPass = (string)cfg($dbConfig, ['password', 'pass', 'db_pass'], '');



if ($dbName === '' || $dbUser === '') {

    echo "ERROR: database.php config keys not found.\n";

    echo "Expected something like host/database/username/password (or db_name/user/pass).\n";

    echo "Your database.php returned:\n";

    print_r($dbConfig);

    exit(1);

}



$dsn = sprintf('mysql:host=%s;dbname=%s;charset=utf8mb4', $dbHost, $dbName);



$pdo = new PDO($dsn, $dbUser, $dbPass, [

    PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,

    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,

]);



/* -----------------------

 * Fetch businesses

 * --------------------- */

$businesses = $pdo->query("SELECT id, name, place_id FROM businesses WHERE active = 1")->fetchAll();



if (!$businesses) {

    echo "No active businesses found.\n";

    exit(0);

}



$client = new OutscraperClient($outConfig);



/* -----------------------

 * Prepare insert (dedupe on unique review_id)

 * --------------------- */

$insertSql = "

INSERT INTO reviews

    (business_id, review_id, rating, author, review_text, review_date, is_negative)

VALUES

    (:business_id, :review_id, :rating, :author, :review_text, :review_date, :is_negative)

ON DUPLICATE KEY UPDATE

    rating = VALUES(rating),

    author = VALUES(author),

    review_text = VALUES(review_text),

    review_date = VALUES(review_date),

    is_negative = VALUES(is_negative)

";

$ins = $pdo->prepare($insertSql);



$totalInserted = 0;

$totalProcessed = 0;



foreach ($businesses as $biz) {

    $bizId   = (int)$biz['id'];

    $placeId = (string)$biz['place_id'];

    $name    = (string)$biz['name'];



    echo "\n=== BUSINESS #{$bizId} {$name} ===\n";

    echo "Place ID: {$placeId}\n";



    $resp = $client->fetchReviews($placeId);



    // If your fetchReviews returns wrapper with 'body'

    $body = $resp['body'] ?? $resp;



    $reviews = extractReviews($body);



    echo "Reviews found: " . count($reviews) . "\n";



    foreach ($reviews as $r) {

        if (!is_array($r)) continue;



        $mapped = mapReviewRow($r);



        if ($mapped['review_id'] === '' || $mapped['review_id'] === '0') {

            // skip if no stable ID

            continue;

        }



        $ins->execute([

            ':business_id' => $bizId,

            ':review_id'   => $mapped['review_id'],

            ':rating'      => $mapped['rating'],

            ':author'      => $mapped['author'],

            ':review_text' => $mapped['review_text'],

            ':review_date' => $mapped['review_date'],

            ':is_negative' => $mapped['is_negative'],

        ]);



        $totalInserted++;

        $totalProcessed++;

    }



    // optional: update last_checked_at

    $upd = $pdo->prepare("UPDATE businesses SET last_checked_at = NOW() WHERE id = :id");

    $upd->execute([':id' => $bizId]);

}



echo "\nDONE.\n";

echo "Total rows inserted/updated: {$totalInserted}\n";

echo "Total reviews processed: {$totalProcessed}\n";
