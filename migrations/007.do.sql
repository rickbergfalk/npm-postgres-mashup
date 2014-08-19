-- Build out the rest of semver the functions
CREATE OR REPLACE FUNCTION get_version_major(v TEXT) RETURNS NUMERIC AS $$
    BEGIN
        RETURN CAST(split_part(get_base_version(v), '.',  1) AS NUMERIC);
    END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION get_version_minor(v TEXT) RETURNS NUMERIC AS $$
    BEGIN 
        RETURN CAST(split_part(get_base_version(v), '.', 2) AS NUMERIC);
    END;
$$ LANGUAGE plpgsql;


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
