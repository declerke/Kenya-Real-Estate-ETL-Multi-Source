-- Average price by county
SELECT 
    county,
    COUNT(*) as listing_count,
    AVG(price_kes) as avg_price,
    MIN(price_kes) as min_price,
    MAX(price_kes) as max_price,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_kes) as median_price
FROM cleaned_listings
WHERE price_kes IS NOT NULL
    AND county IS NOT NULL
GROUP BY county
ORDER BY listing_count DESC;

-- Average price per bedroom by county
SELECT 
    county,
    bedrooms,
    COUNT(*) as listing_count,
    AVG(price_kes) as avg_price,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_kes) as median_price
FROM cleaned_listings
WHERE price_kes IS NOT NULL
    AND county IS NOT NULL
    AND bedrooms IS NOT NULL
    AND bedrooms BETWEEN 1 AND 6
GROUP BY county, bedrooms
ORDER BY county, bedrooms;

-- Property type distribution
SELECT 
    property_type,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM cleaned_listings
WHERE property_type IS NOT NULL
GROUP BY property_type
ORDER BY count DESC;

-- Average price by property type
SELECT 
    property_type,
    COUNT(*) as listing_count,
    AVG(price_kes) as avg_price,
    MIN(price_kes) as min_price,
    MAX(price_kes) as max_price
FROM cleaned_listings
WHERE price_kes IS NOT NULL
    AND property_type IS NOT NULL
GROUP BY property_type
ORDER BY avg_price DESC;

-- Listings by source site
SELECT 
    source_site,
    COUNT(*) as count,
    MIN(scraped_at) as first_scraped,
    MAX(scraped_at) as last_scraped
FROM cleaned_listings
GROUP BY source_site
ORDER BY count DESC;

-- Top neighborhoods by listing count in Nairobi
SELECT 
    neighborhood,
    COUNT(*) as listing_count,
    AVG(price_kes) as avg_price,
    AVG(bedrooms) as avg_bedrooms
FROM cleaned_listings
WHERE county = 'Nairobi'
    AND neighborhood IS NOT NULL
GROUP BY neighborhood
ORDER BY listing_count DESC
LIMIT 20;

-- Price per square meter analysis
SELECT 
    county,
    property_type,
    COUNT(*) as listing_count,
    AVG(price_kes / NULLIF(area_sqm, 0)) as avg_price_per_sqm
FROM cleaned_listings
WHERE price_kes IS NOT NULL
    AND area_sqm IS NOT NULL
    AND area_sqm > 0
    AND county IS NOT NULL
    AND property_type IS NOT NULL
GROUP BY county, property_type
HAVING COUNT(*) >= 5
ORDER BY avg_price_per_sqm DESC;

-- Recent listings in the last 7 days
SELECT 
    source_site,
    title,
    price_kes,
    county,
    neighborhood,
    bedrooms,
    bathrooms,
    property_type,
    scraped_at
FROM cleaned_listings
WHERE scraped_at >= NOW() - INTERVAL '7 days'
ORDER BY scraped_at DESC
LIMIT 50;

-- Nairobi vs Mombasa comparison
SELECT 
    county,
    COUNT(*) as total_listings,
    AVG(price_kes) as avg_price,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_kes) as median_price,
    AVG(bedrooms) as avg_bedrooms,
    AVG(area_sqm) as avg_area_sqm
FROM cleaned_listings
WHERE county IN ('Nairobi', 'Mombasa')
    AND price_kes IS NOT NULL
GROUP BY county;

-- Listings with missing key fields
SELECT 
    source_site,
    COUNT(*) as total,
    SUM(CASE WHEN price_kes IS NULL THEN 1 ELSE 0 END) as missing_price,
    SUM(CASE WHEN county IS NULL THEN 1 ELSE 0 END) as missing_county,
    SUM(CASE WHEN bedrooms IS NULL THEN 1 ELSE 0 END) as missing_bedrooms,
    SUM(CASE WHEN property_type IS NULL THEN 1 ELSE 0 END) as missing_property_type
FROM cleaned_listings
GROUP BY source_site
ORDER BY total DESC;