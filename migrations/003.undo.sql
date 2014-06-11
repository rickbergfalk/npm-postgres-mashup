-- add columns back
-- not the best undo because if there was data in there its gone now

ALTER TABLE load_log ADD COLUMN delete_start TIMESTAMP;
ALTER TABLE load_log ADD COLUMN delete_finish TIMESTAMP;