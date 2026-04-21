/*
    Staging model: stg_moj_sales
    Cleans and enriches raw MOJ sales data with English labels.
*/

WITH raw AS (
    SELECT
        region_ar,
        region_en,
        city,
        district,
        date_gregorian,
        year,
        quarter,
        year_quarter,
        property_classification,
        property_type,
        property_count,
        area,
        price,
        source_file
    FROM {{ source('raw', 'moj_sales') }}
    WHERE price > 0
      AND area > 0
      AND year IS NOT NULL
),

enriched AS (
    SELECT
        *,
        -- English classification labels
        CASE property_classification
            WHEN 'سكني' THEN 'Residential'
            WHEN 'تجاري' THEN 'Commercial'
            WHEN 'زراعي' THEN 'Agricultural'
            WHEN 'صناعي' THEN 'Industrial'
            ELSE 'Other'
        END AS classification_en,

        -- Arabic classification (keep as-is for bilingual)
        property_classification AS classification_ar,

        -- Calculated price per m²
        CASE
            WHEN area > 0 THEN price / area
            ELSE NULL
        END AS price_per_m2

    FROM raw
)

SELECT * FROM enriched
