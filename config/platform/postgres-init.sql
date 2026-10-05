-- One-shot, idempotent and additive: the ghflow role and its operational database.
-- Never drops anything. Run by platform-db-init with `-v ghflow_password=...`.
--
-- Airflow keeps its metadata in the `airflow` database (POSTGRES_DB); ghflow's own operational
-- state (audit rows, replay checkpoints, DQ verdicts) lives in `ghflow_ops`, owned by `ghflow`.

SELECT format('CREATE ROLE ghflow LOGIN PASSWORD %L', :'ghflow_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ghflow')\gexec

-- Keep the role's password equal to .env (an update, never a drop).
ALTER ROLE ghflow PASSWORD :'ghflow_password';

SELECT 'CREATE DATABASE ghflow_ops OWNER ghflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'ghflow_ops')\gexec

\echo platform-db-init: role ghflow and database ghflow_ops ready
