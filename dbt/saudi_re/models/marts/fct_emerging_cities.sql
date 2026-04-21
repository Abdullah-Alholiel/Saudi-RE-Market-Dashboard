/*
    Mart: fct_emerging_cities
    Analysis #5 — Which cities show low base + high growth?

    Calculates Compound Annual Growth Rate (CAGR) for each city.
    Filters: min 10 transactions in base year, at least 2 years of data.
*/

WITH yearly_city AS (
    SELECT
        city,
        region_en,
        region_ar,
        year,
        COUNT(*) AS annual_transactions,
        SUM(price) AS annual_value,
        MEDIAN(price) AS median_price,
        MEDIAN(price_per_m2) AS median_price_per_m2
    FROM {{ ref('stg_moj_sales') }}
    GROUP BY city, region_en, region_ar, year
),

city_range AS (
    SELECT
        city,
        region_en,
        region_ar,
        MIN(year) AS base_year,
        MAX(year) AS latest_year,
        MAX(year) - MIN(year) AS year_span
    FROM yearly_city
    GROUP BY city, region_en, region_ar
    HAVING MAX(year) - MIN(year) >= 2
),

base_latest AS (
    SELECT
        cr.city,
        cr.region_en,
        cr.region_ar,
        cr.base_year,
        cr.latest_year,
        cr.year_span,
        base.annual_transactions AS base_transactions,
        latest.annual_transactions AS latest_transactions,
        base.annual_value AS base_value,
        latest.annual_value AS latest_value,
        latest.median_price AS latest_median_price,
        latest.median_price_per_m2 AS latest_median_price_per_m2
    FROM city_range cr
    JOIN yearly_city base
        ON cr.city = base.city AND cr.region_en = base.region_en AND cr.base_year = base.year
    JOIN yearly_city latest
        ON cr.city = latest.city AND cr.region_en = latest.region_en AND cr.latest_year = latest.year
    WHERE base.annual_transactions >= 10
)

SELECT
    *,
    ROUND(
        POWER(latest_transactions::DOUBLE / base_transactions, 1.0 / year_span) - 1,
        4
    ) AS transaction_cagr,
    ROUND(
        POWER(latest_value::DOUBLE / NULLIF(base_value, 0), 1.0 / year_span) - 1,
        4
    ) AS value_cagr,
    ROW_NUMBER() OVER (ORDER BY POWER(latest_transactions::DOUBLE / base_transactions, 1.0 / year_span) - 1 DESC) AS growth_rank
FROM base_latest
ORDER BY transaction_cagr DESC
