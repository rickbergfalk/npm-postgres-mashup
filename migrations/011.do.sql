-- add columns to version, that describe semver things about the version
ALTER TABLE version 
    ADD COLUMN version_type TEXT;