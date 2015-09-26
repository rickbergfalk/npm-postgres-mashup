-- add column to keep track of where we left off
-- on the download counts
ALTER TABLE package ADD COLUMN last_download_count_day DATE;