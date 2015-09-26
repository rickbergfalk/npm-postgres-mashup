CREATE TABLE download_count (
    package_name        TEXT NOT NULL,
    download_date       DATE NOT NULL,
    download_count      INT NOT NULL,
    PRIMARY KEY         (package_name, download_date),
    FOREIGN KEY         (package_name) REFERENCES package (package_name) ON DELETE CASCADE ON UPDATE CASCADE
);