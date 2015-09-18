-- function for updating the previous versions for a package
CREATE OR REPLACE FUNCTION update_previous_versions(pn TEXT) RETURNS BOOLEAN AS $$
    BEGIN
        
        WITH parsed AS (
            SELECT 
                package_name,
                version,
                version_major AS major,
                version_minor AS minor,
                version_patch AS patch,
                time_created,
                RANK() OVER (PARTITION BY package_name ORDER BY version_major, version_minor, version_patch) AS seq
            FROM 
                version
            WHERE 
                version_is_stable = CAST(1 AS BIT)
                AND package_name = pn
        ),
        s AS (
            SELECT 
                p.package_name,
                p.version,
                previous.version AS previous_stable
            FROM 
                parsed p
                LEFT JOIN parsed previous ON p.seq = previous.seq + 1 AND p.package_name = previous.package_name
        )
        UPDATE version
        SET version_previous_stable = previous_stable
        FROM 
            s 
        WHERE 
            s.package_name = version.package_name 
            AND s.version = version.version;

        
        RETURN TRUE;
    END;
$$ LANGUAGE plpgsql;