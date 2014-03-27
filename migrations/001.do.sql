CREATE TABLE package (
	package_name 			TEXT NOT NULL,
	version_latest			TEXT,
	version_rc 				TEXT,
	_rev 					TEXT,
	readme 					TEXT,
	readme_filename 		TEXT,
	time_created 			TIMESTAMP,
	time_modified 			TIMESTAMP,
	PRIMARY KEY 			(package_name)
);

CREATE TABLE version (
	package_name 			TEXT NOT NULL,
	version 				TEXT NOT NULL,
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
	PRIMARY KEY 			(package_name, version),
	FOREIGN KEY 			(package_name) REFERENCES package (package_name) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE version_contributor (
	package_name 			TEXT NOT NULL,
	version 				TEXT NOT NULL,
	name 					TEXT,
	email 					TEXT,
	FOREIGN KEY 			(package_name, version) REFERENCES version (package_name, version) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE version_maintainer (
	package_name 			TEXT NOT NULL,
	version 				TEXT NOT NULL,
	name 					TEXT,
	email 					TEXT,
	FOREIGN KEY 			(package_name, version) REFERENCES version (package_name, version) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE version_dependency (
	package_name 			TEXT NOT NULL,
	version 				TEXT NOT NULL,
	dependency_name 		TEXT NOT NULL,
	dependency_version 		TEXT,
	PRIMARY KEY 			(package_name, version, dependency_name),
	FOREIGN KEY 			(package_name, version) REFERENCES version (package_name, version) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE version_dev_dependency (
	package_name 			TEXT NOT NULL,
	version 				TEXT NOT NULL,
	dev_dependency_name 	TEXT NOT NULL,
	dev_dependency_version 	TEXT,
	PRIMARY KEY 			(package_name, version, dev_dependency_name),
	FOREIGN KEY 			(package_name, version) REFERENCES version (package_name, version) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE version_keyword (
	package_name 			TEXT NOT NULL,
	version 				TEXT NOT NULL,
	keyword 				TEXT NOT NULL,
	FOREIGN KEY 			(package_name, version) REFERENCES version (package_name, version) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE version_bin (
	package_name 			TEXT NOT NULL,
	version 				TEXT NOT NULL,
	bin_command 			TEXT NOT NULL,
	bin_file				TEXT,
	PRIMARY KEY 			(package_name, version, bin_command),
	FOREIGN KEY 			(package_name, version) REFERENCES version (package_name, version) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE version_script (
	package_name 			TEXT NOT NULL,
	version 				TEXT NOT NULL,
	script_name				TEXT NOT NULL,
	script_text 			TEXT,
	PRIMARY KEY 			(package_name, version, script_name),
	FOREIGN KEY 			(package_name, version) REFERENCES version (package_name, version) ON DELETE CASCADE ON UPDATE CASCADE
);
