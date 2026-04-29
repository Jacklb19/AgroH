-- Schema versioning table
-- Corrección 3.5.c (2026-04-29)
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     INT PRIMARY KEY,
    applied_at  TIMESTAMPTZ DEFAULT now(),
    description TEXT
);
INSERT INTO schema_migrations(version, description)
VALUES (0, 'Schema versioning table') ON CONFLICT DO NOTHING;
