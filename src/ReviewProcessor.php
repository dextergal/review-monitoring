<?php
declare(strict_types=1);

class ReviewProcessor
{
    private Database $db;

    public function __construct(Database $db)
    {
        $this->db = $db;
    }

    /**
     * Process reviews for a single business
     */
    public function process(array $business, array $reviews): void
    {
        foreach ($reviews as $review) {
            $reviewId = $review['review_id'] ?? null;

            if (!$reviewId) {
                continue; // skip malformed reviews
            }

            // Skip if we already processed this review
            $exists = $this->db
                ->query(
                    "SELECT id FROM reviews WHERE review_id = ? LIMIT 1",
                    [$reviewId]
                )
                ->fetch();

            if ($exists) {
                continue;
            }

            $rating     = (int) ($review['rating'] ?? 0);
            $text       = trim($review['review_text'] ?? '');
            $reviewTime = $review['review_datetime_utc'] ?? null;
            $reviewUrl  = $review['review_link'] ?? null;

            // Store review
            $this->db->query(
                "INSERT INTO reviews (
                    business_id,
                    review_id,
                    rating,
                    review_text,
                    review_url,
                    review_datetime,
                    source,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'google', NOW())",
                [
                    $business['id'],
                    $reviewId,
                    $rating,
                    mb_substr($text, 0, 1000),
                    $reviewUrl,
                    $reviewTime
                ]
            );

            // Create negative review event (1–3 stars)
            if ($rating >= 1 && $rating <= 3) {
                $this->db->query(
                    "INSERT INTO negative_review_events (
                        business_id,
                        review_id,
                        rating,
                        review_text,
                        source,
                        status,
                        created_at
                    ) VALUES (?, ?, ?, ?, 'google', 'pending', NOW())",
                    [
                        $business['id'],
                        $reviewId,
                        $rating,
                        mb_substr($text, 0, 500)
                    ]
                );
            }
        }
    }
}