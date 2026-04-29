-- Bootstrap pg_partman in the default database. Flyway then creates the
-- application schema. The extension lives in the dedicated 'partman' schema
-- per the pg_partman convention.
CREATE SCHEMA IF NOT EXISTS partman;
CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;
