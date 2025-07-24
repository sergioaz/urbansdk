-- Migration: Change geo_json column to geometry column with LINESTRING type
-- This migration assumes you have existing GeoJSON data that needs to be converted

-- Step 1: Add the new geometry column
ALTER TABLE link_info ADD COLUMN geometry GEOMETRY(LINESTRING, 4326);

-- Step 2: Convert existing GeoJSON data to PostGIS geometry
-- This handles both single LineString and MultiLineString GeoJSON
UPDATE link_info 
SET geometry = CASE 
    WHEN geo_json IS NOT NULL AND geo_json != '' THEN
        CASE 
            -- Handle MultiLineString by taking the first LineString and casting to LINESTRING
            WHEN ST_GeometryType(ST_GeomFromGeoJSON(geo_json)) = 'ST_MultiLineString' THEN
                ST_GeometryN(ST_GeomFromGeoJSON(geo_json), 1)::geometry(LINESTRING, 4326)
            -- Handle regular LineString
            WHEN ST_GeometryType(ST_GeomFromGeoJSON(geo_json)) = 'ST_LineString' THEN
                ST_GeomFromGeoJSON(geo_json)::geometry(LINESTRING, 4326)
            -- Handle other geometry types by attempting conversion
            ELSE 
                ST_GeomFromGeoJSON(geo_json)::geometry(LINESTRING, 4326)
        END
    ELSE 
        NULL
END
WHERE geo_json IS NOT NULL;

-- Step 3: Create spatial index on the new geometry column for better performance
CREATE INDEX idx_link_info_geometry ON link_info USING GIST (geometry);

-- Step 4: Drop the old geo_json column (uncomment when ready)
ALTER TABLE link_info DROP COLUMN geo_json;

-- Verification queries (run these to check the migration)
SELECT COUNT(*) as total_records FROM link_info;
SELECT COUNT(*) as records_with_geometry FROM link_info WHERE geometry IS NOT NULL;
SELECT COUNT(*) as records_with_geojson FROM link_info WHERE geo_json IS NOT NULL;
SELECT ST_GeometryType(geometry), COUNT(*) FROM link_info WHERE geometry IS NOT NULL GROUP BY ST_GeometryType(geometry);
