-- update additional version data for versions that don't have it yet
UPDATE version
SET
    version_major = get_version_major(version),
    version_minor = get_version_minor(version),
    version_patch = get_version_patch(version),
    version_label = get_version_label(version),
    version_is_stable = CASE get_version_label(version) WHEN '' THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END
WHERE 
    version_major IS NULL