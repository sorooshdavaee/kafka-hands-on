-- Optional bootstrap (EF EnsureCreated also creates tables).
-- Keep wal_level=logical via postgres command in compose.
SELECT 'orders db ready for Debezium logical decoding' AS info;
