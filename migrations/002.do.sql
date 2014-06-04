CREATE TABLE load_log (
    seq                     INT NOT NULL,
    package_name            TEXT NOT NULL,
    version_latest          TEXT,
    delete_start            TIMESTAMP,
    delete_finish           TIMESTAMP,
    inserts_start           TIMESTAMP,
    inserts_finish          TIMESTAMP,
    PRIMARY KEY             (seq)
);