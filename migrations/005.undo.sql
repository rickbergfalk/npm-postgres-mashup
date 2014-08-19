-- remove those additional version columns
ALTER TABLE version 
    DROP COLUMN version_major,
    DROP COLUMN version_minor,
    DROP COLUMN version_patch,
    DROP COLUMN version_label,
    DROP COLUMN version_is_stable,
    DROP COLUMN version_previous_stable;