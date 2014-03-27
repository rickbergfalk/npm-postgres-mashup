DROP TABLE IF EXISTS version_script;
DROP TABLE IF EXISTS version_bin;
DROP TABLE IF EXISTS version_keyword;
DROP TABLE IF EXISTS version_dev_dependency;
DROP TABLE IF EXISTS version_dependency;
DROP TABLE IF EXISTS version_maintainer;
DROP TABLE IF EXISTS version_contributor;
DROP TABLE IF EXISTS version;
DROP TABLE IF EXISTS package;
DROP TABLE IF EXISTS keyword;

CREATE TABLE package (
	package_id 				INT NOT NULL,
	package_name 			TEXT NOT NULL,
	latest_version_id		INT,
	rc_version_id 			INT,
	_rev 					TEXT,
	readme 					TEXT,
	readme_filename 		TEXT,
	time_created 			TIMESTAMP,
	time_modified 			TIMESTAMP,
	PRIMARY KEY 			(package_id)
);

CREATE TABLE version (
	version_id 				INT NOT NULL,
	version 				TEXT NOT NULL,
	package_id 				INT NOT NULL,
	description 			TEXT,
	author_name 			TEXT,
	author_email 			TEXT,
	author_url 				TEXT,
	repository_type 		TEXT,
	repository_url 			TEXT,
	main 					TEXT, 
	license 				TEXT,
	homepage 				TEXT,
	bugs_url				TEXT,
	bugs_homepage 			TEXT,
	bugs_email 				TEXT,
	engine_node				TEXT,
	engine_npm 				TEXT,
	dist_shasum				TEXT,
	dist_tarball 			TEXT,
	_from 					TEXT,
	_resolved 				TEXT,
	_npm_version 			TEXT,
	_npm_user_name 			TEXT,
	_npm_user_email 		TEXT,
	time_created 			TIMESTAMP,
	PRIMARY KEY 			(version_id),
	FOREIGN KEY 			(package_id) REFERENCES package (package_id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE version_contributor (
	version_id				INT NOT NULL,
	name 					TEXT,
	email 					TEXT,
	FOREIGN KEY 			(version_id) REFERENCES version (version_id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE version_maintainer (
	version_id 				INT NOT NULL,
	name 					TEXT,
	email 					TEXT,
	FOREIGN KEY 			(version_id) REFERENCES version (version_id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE version_dependency (
	version_id 				INT NOT NULL,
	dependency_name 		TEXT NOT NULL,
	dependency_version 		TEXT,
	PRIMARY KEY 			(version_id, dependency_name),
	FOREIGN KEY 			(version_id) REFERENCES version (version_id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE version_dev_dependency (
	version_id 				INT NOT NULL,
	dev_dependency_name 	TEXT NOT NULL,
	dev_dependency_version 	TEXT,
	PRIMARY KEY 			(version_id, dev_dependency_name),
	FOREIGN KEY 			(version_id) REFERENCES version (version_id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE keyword (
	keyword_id 				INT NOT NULL,
	keyword 				TEXT NOT NULL,
	PRIMARY KEY 			(keyword_id)
);

CREATE TABLE version_keyword (
	version_id 				INT NOT NULL,
	keyword_id 				INT NOT NULL,
	FOREIGN KEY 			(version_id) REFERENCES version (version_id) ON DELETE CASCADE ON UPDATE CASCADE,
	FOREIGN KEY 			(keyword_id) REFERENCES keyword (keyword_id)
);

CREATE TABLE version_bin (
	version_id 				INT NOT NULL,
	bin_command 			TEXT NOT NULL,
	bin_file				TEXT,
	PRIMARY KEY 			(version_id, bin_command),
	FOREIGN KEY 			(version_id) REFERENCES version (version_id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE version_script (
	version_id 				INT NOT NULL,
	script_name				TEXT NOT NULL,
	script_text 			TEXT,
	PRIMARY KEY 			(version_id, script_name),
	FOREIGN KEY 			(version_id) REFERENCES version (version_id) ON DELETE CASCADE ON UPDATE CASCADE
);
