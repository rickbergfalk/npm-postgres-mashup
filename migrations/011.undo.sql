-- remove those additional version columns
ALTER TABLE version 
    DROP COLUMN version_type;