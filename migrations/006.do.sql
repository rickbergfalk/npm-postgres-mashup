-- Add the get_base_version function
CREATE OR REPLACE FUNCTION get_base_version(v TEXT) RETURNS TEXT AS $$
    BEGIN
        RETURN (regexp_split_to_array(v, '[A-Z]|[a-z]|-'))[1];
    END;
$$ LANGUAGE plpgsql;
