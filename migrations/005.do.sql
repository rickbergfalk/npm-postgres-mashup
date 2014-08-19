-- add columns to version, that describe semver things about the version
ALTER TABLE version 
    ADD COLUMN version_major NUMERIC, 
    ADD COLUMN version_minor NUMERIC, 
    ADD COLUMN version_patch NUMERIC,
    ADD COLUMN version_label TEXT,
    ADD COLUMN version_is_stable BIT,
    ADD COLUMN version_previous_stable TEXT;