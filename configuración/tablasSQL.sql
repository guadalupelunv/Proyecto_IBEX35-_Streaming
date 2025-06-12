CREATE TABLE ibex_points (
    timestamp TIMESTAMP NOT NULL,
    company VARCHAR(100) NOT NULL,
    points NUMERIC(10, 4) NOT NULL
);

CREATE TABLE ibex_news (
    timestamp TIMESTAMP NOT NULL,
    company VARCHAR(100) NOT NULL,
    content TEXT,
    scoring NUMERIC(5, 2)
);

GRANT ALL PRIVILEGES ON DATABASE ibex_db TO hadoop;
ALTER USER hadoop WITH SUPERUSER;