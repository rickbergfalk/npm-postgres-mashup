-- create index for download count query
CREATE INDEX idx_package_last_download_count_day ON package (last_download_count_day);