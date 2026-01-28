<?php

return [

    // Read token from environment
    'access_token' => $_ENV['HUBSPOT_ACCESS_TOKEN'] ?? '',

    // Optional tuning
    'batch_limit'  => 20,
    'max_attempts' => 5,

    // Company property internal names (must exist in HubSpot)
    'company_prop_google_place_id' => 'google_place_id',
    'company_prop_last_neg_rating' => 'last_negative_review_rating',
    'company_prop_last_neg_text'   => 'last_negative_review_text',
    'company_prop_last_neg_date'   => 'last_negative_review_date',
    'company_prop_last_neg_url'    => 'last_negative_review_url',
];
