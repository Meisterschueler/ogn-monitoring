--- create read-only user for grafana
--DROP OWNED BY grafanareader;
DROP USER IF EXISTS grafanareader;

CREATE USER grafanareader WITH PASSWORD 'password';
GRANT USAGE ON SCHEMA public TO grafanareader;
--GRANT SELECT ON public.receivers TO grafanareader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafanareader;