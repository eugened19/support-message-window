
-- Support Message Window: initial PostgreSQL schema and privileges
-- Run as PostgreSQL superuser (postgres) using psql.
-- Replace the password before running. Existing db_admin role is reused.
-- From project root:
--   sudo -u postgres psql -v ON_ERROR_STOP=1 -f sql/001_init_support_message_window.sql
-- This is a first-run bootstrap script, not an idempotent migration.

CREATE ROLE support_message_window WITH LOGIN PASSWORD 'REPLACE_WITH_STRONG_PASSWORD';

CREATE DATABASE support_message_window_db OWNER postgres;

GRANT CONNECT ON DATABASE support_message_window_db
    TO support_message_window, db_admin;

\connect support_message_window_db

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO support_message_window, db_admin;

CREATE TABLE agents (
    agent_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    agent_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE agent_messages (
    message_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    agent_id BIGINT NOT NULL REFERENCES agents(agent_id),
    received_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_agent_messages_agent_time
    ON agent_messages (agent_id, received_at);

GRANT SELECT, INSERT ON agents, agent_messages TO support_message_window;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO support_message_window;

GRANT SELECT ON agents, agent_messages TO db_admin;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    GRANT SELECT ON TABLES TO db_admin;

-- The application may SELECT/INSERT but cannot UPDATE/DELETE existing rows.
-- db_admin is read-only here only if it has no broader privileges
-- (e.g., SUPERUSER, ownership, or inherited grants).
