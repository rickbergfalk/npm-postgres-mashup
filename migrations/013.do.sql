-- populate version type
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
),
s AS (
    SELECT 
        p.package_name,
        p.version,
        p.major,
        p.minor,
        p.patch,
        previous.major AS previous_major,
        previous.minor AS previous_minor,
        previous.patch AS previous_patch,
        previous.version AS previous_stable
    FROM 
        parsed p
        LEFT JOIN parsed previous ON p.seq = previous.seq + 1 AND p.package_name = previous.package_name
)
UPDATE version
SET 
    version_previous_stable = s.previous_stable,
    version_type = CASE
        WHEN version_major = 0 THEN 'unstable'
        WHEN version_major > 0 AND previous_major IS NULL THEN 'major'
        WHEN version_major <> previous_major THEN 'major'
        WHEN version_major = previous_major AND version_minor <> previous_minor THEN 'minor'
        WHEN version_major = previous_major AND version_minor = previous_minor AND version_patch <> previous_patch THEN 'patch'
        END
FROM 
    s 
WHERE 
    s.package_name = version.package_name 
    AND s.version = version.version;