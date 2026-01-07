<?php



class OutscraperClient

{

    private string $apiKey;

    private string $endpoint;



    public function __construct(array $config)

    {

        if (empty($config['api_key']) || empty($config['endpoint'])) {

            throw new Exception('Outscraper config missing api_key or endpoint');

        }



        $this->apiKey = $config['api_key'];

        $this->endpoint = rtrim($config['endpoint'], '/');

    }



    /**

     * Fetch Google reviews by Place ID (async Outscraper flow)

     */

    public function fetchReviews(string $placeId): array

    {

        $params = http_build_query([

            'query'        => $placeId,

            'limit'        => 1,

            'reviewsLimit' => 20,

        ]);



        $url = $this->endpoint . '?' . $params;



        // 1) Kick off request

        $first = $this->getJson($url);



        // If not async, return immediately

        if (($first['http_code'] ?? 0) !== 202) {

            return $first;

        }



        $body = $first['body'] ?? [];

        $resultsUrl = $body['results_location'] ?? null;



        if (!$resultsUrl) {

            return [

                'error'   => true,

                'message' => 'Outscraper returned 202 but no results_location',

                'first'   => $first,

            ];

        }



        // 2) Poll results_location

        $maxAttempts = 12; // ~60s

        $sleepSeconds = 5;



        for ($i = 1; $i <= $maxAttempts; $i++) {

            sleep($sleepSeconds);



            $poll = $this->getJson($resultsUrl);



            // Ready

            if (($poll['http_code'] ?? 0) === 200) {

                return [

                    'http_code' => 200,

                    'body'      => $poll['body'],

                    'raw'       => $poll['raw'],

                    'url'       => $resultsUrl,

                    'attempts'  => $i,

                ];

            }



            // Failed state

            $status = $poll['body']['status'] ?? null;

            if ($status && strtolower($status) !== 'pending') {

                return [

                    'error'   => true,

                    'message' => 'Outscraper task did not complete successfully',

                    'status'  => $status,

                    'poll'    => $poll,

                    'url'     => $resultsUrl,

                ];

            }

        }



        return [

            'error'   => true,

            'message' => 'Timed out waiting for Outscraper results',

            'url'     => $resultsUrl,

            'first'   => $first,

        ];

    }



    /**

     * Helper: GET JSON with X-API-KEY

     */

    private function getJson(string $url): array

    {

        $ch = curl_init($url);



        curl_setopt_array($ch, [

            CURLOPT_RETURNTRANSFER => true,

            CURLOPT_HTTPGET        => true,

            CURLOPT_HTTPHEADER     => [

                'X-API-KEY: ' . $this->apiKey,

            ],

            CURLOPT_TIMEOUT        => 60,

        ]);



        $response = curl_exec($ch);

        $error = curl_error($ch);

        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);

        curl_close($ch);



        if ($response === false) {

            return [

                'error'     => true,

                'message'   => 'cURL error: ' . $error,

                'http_code' => $httpCode,

                'url'       => $url,

            ];

        }



        return [

            'http_code' => $httpCode,

            'body'      => json_decode($response, true),

            'raw'       => $response,

            'url'       => $url,

        ];

    }

}

