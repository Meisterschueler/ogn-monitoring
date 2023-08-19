TRUNCATE countries;
INSERT INTO countries (iso_a2_eh, geom)
SELECT
	iso_a2_eh,
	geom
FROM countries_import;
