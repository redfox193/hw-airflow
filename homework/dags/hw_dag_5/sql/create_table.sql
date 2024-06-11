CREATE TABLE IF NOT EXISTS humans (
    id              SERIAL PRIMARY KEY,
    first_name      VARCHAR(30)     NOT NULL,
    last_name       VARCHAR(30)     NOT NULL,
    gender          VARCHAR(1)      NOT NULL,
    email           VARCHAR(100)    NOT NULL
);