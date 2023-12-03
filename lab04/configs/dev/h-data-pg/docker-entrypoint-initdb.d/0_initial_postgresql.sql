-- PostgreSQL
CREATE DATABASE h_core
    WITH OWNER = h_user
    ENCODING = 'UTF8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    LC_COLLATE ='en_US.UTF-8'
    LC_CTYPE ='en_US.UTF-8'
    TEMPLATE template0;

\c h_core
CREATE EXTENSION pgcrypto;
CREATE EXTENSION vector;

CREATE OR REPLACE FUNCTION h_concat_string_normalize(country_title VARCHAR DEFAULT NULL,
                                                     city_title VARCHAR DEFAULT NULL,
                                                     about VARCHAR DEFAULT NULL,
                                                     activities VARCHAR DEFAULT NULL,
                                                     books VARCHAR DEFAULT NULL,
                                                     games VARCHAR DEFAULT NULL,
                                                     interests VARCHAR DEFAULT NULL) RETURNS TEXT AS
$$
DECLARE
    result TEXT;
BEGIN
    result := CONCAT_WS(' ', country_title, city_title, about, activities, books, games, interests);
    result := REGEXP_REPLACE(result, '\s+', ' ');
    result := TRIM(result);
    result := LOWER(result);
    RETURN result;
END
$$ LANGUAGE plpgsql
    IMMUTABLE;