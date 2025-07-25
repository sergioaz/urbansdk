-- Urban SDK Database Creation
-- Run this first to create the urbansdk database
-- Execute: psql -U postgres -f create_database.sql

-- Drop existing database if it exists (be careful!)
DROP DATABASE IF EXISTS urbansdk;

-- Create the urbansdk database
CREATE DATABASE urbansdk
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_United States.1252'
    LC_CTYPE = 'English_United States.1252'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

-- Set search path for PostGIS
ALTER DATABASE urbansdk
    SET search_path TO "$user", public, topology, tiger;

SELECT 'Database urbansdk created successfully!' as status;
