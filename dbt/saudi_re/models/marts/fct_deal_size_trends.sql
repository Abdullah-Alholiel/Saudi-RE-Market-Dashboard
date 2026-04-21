/*
    Mart: fct_deal_size_trends
    Analysis #6 — How are median deal values, areas, and price/m² trending?

    Computes median price, area, and price per m² by quarter, region, and property classification.
    Uses MEDIAN() (DuckDB native) for robust central tendency on skewed data.
*/

SELECT
    year_quarter,
    year,
    quarter,
    region_en,
    region_ar,
    property_classification AS classification_ar,
    CASE property_classification
        WHEN 'سكني' THEN 'Residential'
        WHEN 'تجاري' THEN 'Commercial'
        WHEN 'زراعي' THEN 'Agricultural'
        WHEN 'صناعي' THEN 'Industrial'
    END AS classification_en,
    COUNT(*) AS transaction_count,
    ROUND(MEDIAN(price), 0) AS median_price,
    ROUND(MEDIAN(area), 1) AS median_area,
    ROUND(MEDIAN(price_per_m2), 0) AS median_price_per_m2,
    ROUND(AVG(price), 0) AS avg_price,
    ROUND(AVG(area), 1) AS avg_area,
    ROUND(SUM(price), 0) AS total_value
FROM {{ ref('stg_moj_sales') }}
WHERE property_classification IN ('سكني', 'تجاري', 'زراعي', 'صناعي')
GROUP BY year_quarter, year, quarter, region_en, region_ar, property_classification
ORDER BY year_quarter, property_classification
