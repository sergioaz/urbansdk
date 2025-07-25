-- Urban SDK Table Creation
-- Run this AFTER connecting to the urbansdk database
-- Execute: psql -U postgres -d urbansdk -f create_tables.sql

-- Enable PostGIS extension for spatial data support
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- Table: public.link
-- Stores road segment information with spatial geometry

DROP TABLE IF EXISTS public.speed_record CASCADE;
DROP TABLE IF EXISTS public.link CASCADE;

CREATE TABLE public.link
(
    id SERIAL PRIMARY KEY,
    link_id INTEGER UNIQUE NOT NULL,
    road_name VARCHAR(255),
    geometry GEOMETRY(GEOMETRY, 4326),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create spatial index for geometry column for faster spatial queries
CREATE INDEX idx_link_geometry ON public.link USING GIST (geometry);

-- Create index on link_id for faster joins
CREATE INDEX idx_link_link_id ON public.link (link_id);

-- Table: public.speed_record
-- Stores speed measurements with temporal aggregation data

CREATE TABLE public.speed_record
(
    id SERIAL PRIMARY KEY,
    link_id INTEGER NOT NULL,
    date_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    average_speed NUMERIC(10, 2),
    day_of_week INTEGER,
    period INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT speed_record_link_id_fkey FOREIGN KEY (link_id)
        REFERENCES public.link (link_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
);

-- Create indexes for faster queries
CREATE INDEX idx_speed_record_link_id ON public.speed_record (link_id);
CREATE INDEX idx_speed_record_day_period ON public.speed_record (day_of_week, period);
CREATE INDEX idx_speed_record_date_time ON public.speed_record (date_time);

-- Composite index for common query patterns
CREATE INDEX idx_speed_record_link_day_period ON public.speed_record (link_id, day_of_week, period);

-- Set table ownership
ALTER TABLE public.link OWNER TO postgres;
ALTER TABLE public.speed_record OWNER TO postgres;

-- Add helpful comments
COMMENT ON TABLE public.link IS 'Road segment information with spatial geometry';
COMMENT ON COLUMN public.link.link_id IS 'Unique identifier for road segment';
COMMENT ON COLUMN public.link.road_name IS 'Name of the road';
COMMENT ON COLUMN public.link.geometry IS 'Spatial geometry of the road segment (LINESTRING)';

COMMENT ON TABLE public.speed_record IS 'Speed measurements with temporal data';
COMMENT ON COLUMN public.speed_record.link_id IS 'Reference to link table';
COMMENT ON COLUMN public.speed_record.date_time IS 'Timestamp of the speed measurement';
COMMENT ON COLUMN public.speed_record.average_speed IS 'Average speed in the measurement period';
COMMENT ON COLUMN public.speed_record.day_of_week IS 'Day of week (1=Sunday, 2=Monday, ..., 7=Saturday)';
COMMENT ON COLUMN public.speed_record.period IS 'Time period (1-7 representing different time slots)';

-- Create a view for easy data access with joined information
CREATE OR REPLACE VIEW public.link_speed_summary AS
SELECT 
    l.link_id,
    l.road_name,
    l.geometry,
    sr.day_of_week,
    sr.period,
    AVG(sr.average_speed) as avg_speed,
    COUNT(sr.id) as record_count,
    MIN(sr.date_time) as earliest_record,
    MAX(sr.date_time) as latest_record
FROM public.link l
LEFT JOIN public.speed_record sr ON l.link_id = sr.link_id
GROUP BY l.link_id, l.road_name, l.geometry, sr.day_of_week, sr.period;

COMMENT ON VIEW public.link_speed_summary IS 'Aggregated view of links with their speed statistics';

-- Grant permissions (uncomment and adjust as needed for your environment)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON public.link TO your_app_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON public.speed_record TO your_app_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO your_app_user;

SELECT 'Tables created successfully in urbansdk database!' as status;
