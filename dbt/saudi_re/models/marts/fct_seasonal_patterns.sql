/*
    Mart: fct_seasonal_patterns
    Analysis #4 — Which quarters consistently outperform across regions?

    Aggregates transaction count and value by year and quarter for each region.
*/

SELECT
    year,
    quarter,
    region_en,
    region_ar,
    COUNT(*) AS transaction_count,
    SUM(price) AS total_value,
    MEDIAN(price) AS median_price,
    MEDIAN(price_per_m2) AS median_price_per_m2,
    SUM(price) / COUNT(*) AS avg_deal_value
FROM {{ ref('stg_moj_sales') }}
GROUP BY year, quarter, region_en, region_ar
ORDER BY region_en, year, quarter
