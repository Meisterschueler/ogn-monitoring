CREATE TABLE IF NOT EXISTS testcases (
	testcase	TEXT,
	teststep	SMALLINT,
	testdesc	TEXT,
	testresult	BOOL
);
TRUNCATE testcases;

CREATE TABLE IF NOT EXISTS positions_testing (LIKE positions INCLUDING ALL);
CREATE TABLE IF NOT EXISTS events_takeoff_testing (LIKE events_takeoff INCLUDING ALL);

---

-- DROP TABLE events_takeoff_testing;
-- DROP TABLE positions_testing;
