<?php
declare(strict_types=1);

/**
 * /var/www/review-monitor/cron/run-monitor.php
 */

$BASE_DIR = realpath(__DIR__ . '/..');
if ($BASE_DIR === false) {
    throw new RuntimeException('Failed to resolve BASE_DIR');
}

/**
 * Load configs
 */
$dbConfig         = require $BASE_DIR . '/config/database.php';
$outscraperConfig = require $BASE_DIR . '/config/outscraper.php';

/**
 * Load classes
 */
require_once $BASE_DIR . '/src/Database.php';
require_once $BASE_DIR . '/src/OutscraperClient.php';
require_once $BASE_DIR . '/src/ReviewProcessor.php';

/**
 * Bootstrap
 */
$db         = new Database($dbConfig);
$outscraper = new OutscraperClient($outscraperConfig);
$processor  = new ReviewProcessor($db);

/**
 * Run monitor
 */
$businesses = $db
    ->query("SELECT * FROM businesses WHERE active = 1")
    ->fetchAll();

foreach ($businesses as $business) {
    try {
        $response = $outscraper->fetchReviews($business['place_id']);
        $reviews  = $response['data'][0]['reviews'] ?? [];

        $processor->process($business, $reviews);

        $db->query(
            "UPDATE businesses SET last_checked_at = NOW() WHERE id = ?",
            [$business['id']]
        );
    } catch (Throwable $e) {
        error_log('[Review Monitor] Business ' . $business['id'] . ': ' . $e->getMessage());
    }
}
