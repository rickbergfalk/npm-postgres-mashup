DROP TABLE IF EXISTS versions;

SELECT start_action('create table versions');

CREATE TABLE versions AS (
	SELECT 
		p.package_name,
		t.key AS version,
		t.value::timestamp AS version_timestamp,
		get_base_version(t.key) AS base_version,
		get_version_major(t.key) AS version_major,
		get_version_minor(t.key) AS version_minor,
		get_version_patch(t.key) AS version_patch,
		get_version_label(t.key) AS version_label,
		CASE get_version_label(t.key) WHEN '' THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS version_stable,
		''::TEXT AS publish_type, -- this will get filled in later (doing it now makes for a messy query)
		''::TEXT AS previous_version,
        ''::TEXT AS next_version
	FROM 
		package_doc p,
		jsonb_each_text(p.doc -> 'time') t
	WHERE 
		t.key NOT IN ('created', 'modified')
);

SELECT finish_action('create table versions');



-- Update publish_type
SELECT start_action('update version publish_type');

WITH parsed AS (
    SELECT 
        package_name,
        version,
        version_major AS major,
        version_minor AS minor,
        version_patch AS patch,
        ROW_NUMBER() OVER (PARTITION BY package_name ORDER BY version_major, version_minor, version_patch) AS seq
    FROM 
        versions
    WHERE 
        version_stable = 1::BIT
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
        previous.version AS previous_version,
        next.version AS next_version
    FROM 
        parsed p
        LEFT JOIN parsed previous ON p.seq = previous.seq + 1 AND p.package_name = previous.package_name
        LEFT JOIN parsed next on p.seq = next.seq - 1 AND p.package_name = next.package_name
)
UPDATE versions
SET 
    previous_version = s.previous_version,
    next_version = s.next_version,
    publish_type = CASE
        WHEN version_major = 0 THEN 'dev'
        WHEN version_major > 0 AND previous_major IS NULL THEN 'major'
        WHEN version_major <> previous_major THEN 'major'
        WHEN version_major = previous_major AND version_minor <> previous_minor THEN 'minor'
        WHEN version_major = previous_major AND version_minor = previous_minor AND version_patch <> previous_patch THEN 'patch'
        END
FROM 
    s 
WHERE 
    s.package_name = versions.package_name 
    AND s.version = versions.version;
    
SELECT start_action('update version publish_type');




-- From here on our we want to get the latest package version doc
DROP TABLE IF EXISTS latest_version_doc;

SELECT start_action('create table latest_version_doc');

CREATE TABLE latest_version_doc AS (
	SELECT 
		p.package_name,
		t.key AS latest_version, 
		--jsonb_pretty(t.value), 
		p.doc ->> 'readme' AS readme,
		t.value AS version_doc
	FROM 
		package_doc p,
		jsonb_each(p.doc -> 'versions') t,
		versions v
	WHERE 
		-- join in versions to get latest version
		v.package_name = p.package_name 
		AND v.version = t.key
		AND v.next_version IS NULL 
		AND v.version_stable = 1::BIT
);

SELECT finish_action('create table latest_version_doc');




-- latest dependencies
DROP TABLE IF EXISTS dependencies;

SELECT start_action('create table dependencies');

CREATE TABLE dependencies AS (
	SELECT 
		v.package_name,
		d.key AS dependency_name,
		d.value AS dependency_version
	FROM 
		latest_version_doc v,
		jsonb_each_text(v.version_doc -> 'dependencies') d
	WHERE 
		jsonb_typeof(v.version_doc -> 'dependencies') = 'object'			
);

SELECT finish_action('create table dependencies');


-- latest dev dependencies
DROP TABLE IF EXISTS dev_dependencies;

SELECT start_action('create table dev_dependencies');

CREATE TABLE dev_dependencies AS (
	SELECT 
		v.package_name,
		d.key AS dependency_name,
		d.value AS dependency_version
	FROM 
		latest_version_doc v,
		jsonb_each_text(v.version_doc -> 'devDependencies') d	
	WHERE 
		jsonb_typeof(v.version_doc -> 'devDependencies') = 'object'		
);

SELECT finish_action('create table dev_dependencies');


-- latest scripts
DROP TABLE IF EXISTS scripts;

SELECT start_action('create table scripts');

CREATE TABLE scripts AS (
	SELECT 
		v.package_name,
		d.key AS script_name,
		d.value AS script
	FROM 
		latest_version_doc v,
		jsonb_each_text(v.version_doc -> 'scripts') d
	WHERE 
		jsonb_typeof(v.version_doc -> 'scripts') = 'object'		
);

SELECT finish_action('create table scripts');


-- latest keywords
DROP TABLE IF EXISTS keywords;

SELECT start_action('create table keywords');

CREATE TABLE keywords AS (
	SELECT 
		v.package_name,
		k.value AS keyword
	FROM 
		latest_version_doc v,
		jsonb_array_elements_text(v.version_doc -> 'keywords') k
	WHERE 
		jsonb_typeof(v.version_doc -> 'keywords') = 'array'
);

SELECT finish_action('create table keywords');


-- latest maintainers
DROP TABLE IF EXISTS maintainers;

SELECT start_action('create table maintainers');

CREATE TABLE maintainers AS (
	SELECT 
		v.package_name, 
		m.value ->> 'name' AS maintainer_name,
		m.value ->> 'email' AS maintainer_email
	FROM 
		latest_version_doc v,
		jsonb_array_elements(v.version_doc -> 'maintainers') m
	WHERE 
		jsonb_typeof(v.version_doc -> 'maintainers') = 'array'
		AND jsonb_typeof(m.value) = 'object'
);

SELECT finish_action('create table maintainers');


-- latest contributors
DROP TABLE IF EXISTS contributors;

SELECT start_action('create table contributors');

CREATE TABLE contributors AS (
	SELECT 
		v.package_name, 
		m.value ->> 'name' AS contributor_name,
		m.value ->> 'email' AS contributor_email
	FROM 
		latest_version_doc v,
		jsonb_array_elements(v.version_doc -> 'contributors') m
	WHERE 
		jsonb_typeof(v.version_doc -> 'contributors') = 'array'
		AND jsonb_typeof(m.value) = 'object'
);

SELECT finish_action('create table contributors');




-- To get latest versioneach version doc
DROP TABLE IF EXISTS package_summary;

SELECT start_action('create table package_summary');

CREATE TABLE package_summary AS (
	WITH version_counts AS (
		SELECT 
			package_name,
			COUNT(*) AS version_count,
			COUNT(CASE WHEN version_stable = 1::BIT THEN version END) AS stable_version_count,
			COUNT(CASE WHEN version_stable = 0::BIT THEN version END) AS unstable_version_count,
			COUNT(CASE WHEN publish_type = 'dev' THEN version END) AS dev_version_count,
			COUNT(CASE WHEN publish_type = 'major' THEN version END) AS major_version_count,
			COUNT(CASE WHEN publish_type = 'minor' THEN version END) AS minor_version_count,
			COUNT(CASE WHEN publish_type = 'patch' THEN version END) AS patch_version_count
		FROM 
			versions 
		GROUP BY 
			package_name
	),
	keyword_count AS (
		SELECT 
			package_name,
			COUNT(*) AS keyword_count
		FROM 
			keywords
		GROUP BY 
			package_name
	),
	maintainer_count AS (
		SELECT 
			package_name,
			COUNT(*) AS maintainer_count
		FROM 
			maintainers
		GROUP BY 
			package_name 
	),
	contributor_count AS (
		SELECT 
			package_name,
			COUNT(*) AS contributor_count
		FROM 
			contributors
		GROUP BY 
			package_name 
	),
	dependency_count AS (
		SELECT 
			package_name,
			COUNT(*) AS dependency_count
		FROM 
			dependencies
		GROUP BY 
			package_name 
	),
	dev_dependency_count AS (
		SELECT 
			package_name,
			COUNT(*) AS dev_dependency_count
		FROM 
			dev_dependencies
		GROUP BY 
			package_name 
	),
	script_count AS (
		SELECT 
			package_name,
			COUNT(*) AS script_count
		FROM 
			scripts
		GROUP BY 
			package_name 
	)
	SELECT 
		v.package_name,
		v.latest_version, 
		v.readme AS readme,
		(p.doc -> 'time' ->> 'created')::TIMESTAMP AS created_date,
		(p.doc -> 'time' ->> 'modified')::TIMESTAMP AS modified_date,
		v.version_doc ->> 'description' AS description,
		v.version_doc -> 'repository' ->> 'url' AS repository,
		v.version_doc ->> 'license' AS license,
		v.version_doc ->> 'homepage' AS homepage,
		v.version_doc -> 'author' ->> 'name' AS author_name,
		v.version_doc -> 'author' ->> 'email' AS author_email,
		v.version_doc -> '_npmUser' ->> 'name' AS npm_user_name,
		v.version_doc -> '_npmUser' ->> 'email' AS npm_user_email,
		vc.version_count,
		vc.stable_version_count,
		vc.unstable_version_count,
		vc.dev_version_count,
		vc.major_version_count,
		vc.minor_version_count,
		vc.patch_version_count,
		kc.keyword_count,
		mc.maintainer_count,
		cc.contributor_count,
		dc.dependency_count,
		ddc.dev_dependency_count,
		sc.script_count
	FROM 
		latest_version_doc v
		JOIN version_counts vc ON v.package_name = vc.package_name
		JOIN package_doc p ON v.package_name = p.package_name
		LEFT JOIN keyword_count kc ON v.package_name = kc.package_name
		LEFT JOIN maintainer_count mc ON v.package_name = mc.package_name
		LEFT JOIN contributor_count cc ON v.package_name = cc.package_name
		LEFT JOIN dependency_count dc ON v.package_name = dc.package_name
		LEFT JOIN dev_dependency_count ddc ON v.package_name = ddc.package_name
		LEFT JOIN script_count sc ON v.package_name = sc.package_name
);

SELECT finish_action('create table package_summary');


