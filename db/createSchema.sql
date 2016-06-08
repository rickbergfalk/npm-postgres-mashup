
-- remove tables if they already exist for some reason
DROP TABLE IF EXISTS package_doc;
DROP TABLE IF EXISTS couch_seq_log;

-- create a new table that stored packages in jsonb
CREATE TABLE package_doc (
    package_name TEXT PRIMARY KEY,
    deleted BIT NOT NULL,
    doc JSONB NOT NULL
);

-- create seq_log table
-- this will be used to figure out where we left off instead of load_log
-- when npm2pg resumes, it'll grab the max(seq) from this table
CREATE TABLE couch_seq_log (
    seq                     INT NOT NULL,
    process_date            TIMESTAMP,
    PRIMARY KEY             (seq)
);

-- Add the get_base_version function
CREATE OR REPLACE FUNCTION get_base_version(v TEXT) RETURNS TEXT AS $$
    BEGIN
        RETURN (regexp_split_to_array(v, '[A-Z]|[a-z]|-'))[1];
    END;
$$ LANGUAGE plpgsql;

-- Build out the rest of semver the functions
DROP FUNCTION IF EXISTS get_version_major(text);
CREATE OR REPLACE FUNCTION get_version_major(v TEXT) RETURNS NUMERIC AS $$
    BEGIN
        RETURN CAST(split_part(get_base_version(v), '.',  1) AS NUMERIC);
    END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS get_version_minor(text);
CREATE OR REPLACE FUNCTION get_version_minor(v TEXT) RETURNS NUMERIC AS $$
    BEGIN 
        RETURN CAST(split_part(get_base_version(v), '.', 2) AS NUMERIC);
    END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS get_version_patch(text);
CREATE OR REPLACE FUNCTION get_version_patch(v TEXT) RETURNS NUMERIC AS $$
    BEGIN 
        RETURN CAST(split_part(get_base_version(v), '.', 3) AS NUMERIC);
    END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION get_version_label(v TEXT) RETURNS TEXT AS $$
    BEGIN
        -- to get the label we can...
        RETURN regexp_replace(v, ('^' || get_base_version(v)), '');
    END;
$$ LANGUAGE plpgsql;


-- table and functions for logging. this will be handy when testing for performance and whatnot
DROP TABLE IF EXISTS action_log;
CREATE TABLE action_log (
	id SERIAL PRIMARY KEY,
	action TEXT NOT NULL, 
	start_time TIMESTAMP DEFAULT Timeofday()::TIMESTAMP,
	finish_time TIMESTAMP NULL
);
CREATE INDEX IX_actionlog_action_fimishtime ON action_log (action, finish_time);



DROP FUNCTION IF EXISTS start_action(text);
CREATE OR REPLACE FUNCTION start_action(a TEXT) RETURNS VOID AS $$
    BEGIN 
        
        -- if an unfinished action exists remove it. It didn't finish
        DELETE FROM action_log 
        WHERE action = a AND finish_time IS NULL;
        
        -- create new record for action
        INSERT INTO action_log (action) VALUES (a);
        
        RETURN;
    
    END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS finish_action(text);
CREATE OR REPLACE FUNCTION finish_action(a TEXT) RETURNS VOID AS $$
    BEGIN 
        
        UPDATE action_log 
        SET finish_time = Timeofday()::TIMESTAMP
        WHERE action = a AND finish_time IS NULL;
        
        RETURN;
    
    END;
$$ LANGUAGE plpgsql;







-- Remove database objects we don't need anymore
-- These will be replaced with views and reporting tables or nothing
DROP FUNCTION IF EXISTS public.update_previous_versions(text);
DROP TABLE IF EXISTS load_log;
DROP TABLE IF EXISTS schemaversion;

DROP TABLE IF EXISTS version_script;
DROP TABLE IF EXISTS version_maintainer;
DROP TABLE IF EXISTS version_keyword;
DROP TABLE IF EXISTS version_dev_dependency;
DROP TABLE IF EXISTS version_dependency;
DROP TABLE IF EXISTS version_contributor;
DROP TABLE IF EXISTS version_bin;
DROP TABLE IF EXISTS version;
DROP TABLE IF EXISTS download_count;
DROP TABLE IF EXISTS package;
