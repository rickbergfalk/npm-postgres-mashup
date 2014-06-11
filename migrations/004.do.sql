-- add processing bit to load_log
-- it will be used to determine what seq should be used to start with on resume

ALTER TABLE load_log ADD COLUMN processing BIT NULL;