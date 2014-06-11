-- remove columns - they aren't needed anymore

ALTER TABLE load_log 
    DROP COLUMN delete_start;
    
ALTER TABLE load_log
    DROP COLUMN delete_finish;