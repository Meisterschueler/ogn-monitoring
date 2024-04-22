-- test with real data of a simple takeoff

TRUNCATE positions_testing;
TRUNCATE update_events_takeoff;

INSERT INTO positions_testing (ts, src_call, dst_call, receiver, receiver_time, symbol_table, symbol_code, course, speed, altitude, aircraft_type, climb_rate, turn_rate, receiver_ts, location)
SELECT *
FROM (
	VALUES
		(TIMESTAMPTZ'2024-03-29 10:07:37.867106+00','FLRD01CFE','OGFLR','Koenigsdf','100736h','/','''',90,0,1975,1,-19,0,TIMESTAMPTZ'2024-03-29 10:07:36+00',POINT(11.458166666666665,47.82956666666667)::GEOMETRY),
		(TIMESTAMPTZ'2024-03-29 10:07:57.802491+00','FLRD01CFE','OGFLR','Koenigsdf','100756h','/','''',26,0,1975,1,0,0,TIMESTAMPTZ'2024-03-29 10:07:56+00',POINT(11.458166666666665,47.82956666666667)::GEOMETRY),
		(TIMESTAMPTZ'2024-03-29 10:08:17.728155+00','FLRD01CFE','OGFLR','Koenigsdf','100816h','/','''',166,0,1975,1,0,0,TIMESTAMPTZ'2024-03-29 10:08:16+00',POINT(11.458166666666665,47.82956666666667)::GEOMETRY),
		(TIMESTAMPTZ'2024-03-29 10:08:37.825293+00','FLRD01CFE','OGFLR','Koenigsdf','100836h','/','''',102,13,1979,1,20,0.2,TIMESTAMPTZ'2024-03-29 10:08:36+00',POINT(11.458249999999998,47.829550000000005)::GEOMETRY),
		(TIMESTAMPTZ'2024-03-29 10:08:39.803715+00','FLRD01CFE','OGFLR','Koenigsdf','100838h','/','''',102,44,1982,1,59,-0.2,TIMESTAMPTZ'2024-03-29 10:08:38+00',POINT(11.458683333333333,47.829483333333336)::GEOMETRY),
		(TIMESTAMPTZ'2024-03-29 10:08:41.755274+00','FLRD01CFE','OGFLR','Koenigsdf','100840h','/','''',101,57,2002,1,1584,0,TIMESTAMPTZ'2024-03-29 10:08:40+00',POINT(11.4594,47.82940000000001)::GEOMETRY),
		(TIMESTAMPTZ'2024-03-29 10:08:42.818809+00','FLRD01CFE','OGFLR','Koenigsdf','100841h','/','''',99,57,2034,1,2416,-0.6,TIMESTAMPTZ'2024-03-29 10:08:41+00',POINT(11.459783333333332,47.829350000000005)::GEOMETRY),
		(TIMESTAMPTZ'2024-03-29 10:08:44.81998+00','FLRD01CFE','OGFLR','Koenigsdf','100843h','/','''',94,57,2130,1,3208,-0.8,TIMESTAMPTZ'2024-03-29 10:08:43+00',POINT(11.460566666666667,47.829283333333336)::GEOMETRY),
		(TIMESTAMPTZ'2024-03-29 10:08:45.726137+00','FLRD01CFE','OGFLR','Koenigsdf','100844h','/','''',92,55,2182,1,3445,-0.7,TIMESTAMPTZ'2024-03-29 10:08:44+00',POINT(11.46095,47.829266666666676)::GEOMETRY)
	) AS t(ts, src_call, dst_call, receiver, receiver_time, symbol_table, symbol_code, course, speed, altitude, aircraft_type, climb_rate, turn_rate, receiver_ts, location);

-- check standstill
SELECT update_events_takeoff('positions_testing', 'events_takeoff_testing', '2024-03-29 10:07:37', '2024-03-29 10:07:58');
INSERT INTO testcases (testcase, teststep, testdesc, testresult)
SELECT 'basic' AS testcase, 0 AS teststep, 'nothing should happen' AS testdesc, CASE WHEN (SELECT COUNT(*) FROM events_takeoff_testing) = 0 THEN TRUE ELSE FALSE END AS testresult;

SELECT update_events_takeoff('positions_testing', 'events_takeoff_testing', '2024-03-29 10:07:37', '2024-03-29 10:08:38');
INSERT INTO testcases (testcase, teststep, testdesc, testresult)
SELECT 'basic' AS testcase, 1 AS teststep, 'moving is not takeoff' AS testdesc, CASE WHEN (SELECT COUNT(*) FROM events_takeoff_testing) = 0 THEN TRUE ELSE FALSE END AS testresult;

-- plane is taking off (only two points)
SELECT update_events_takeoff('positions_testing', 'events_takeoff_testing', '2024-03-29 10:08:37', '2024-03-29 10:08:40');
INSERT INTO testcases (testcase, teststep, testdesc, testresult)
SELECT 'basic' AS testcase, 2 AS teststep, 'takeoff with event 0 (2 point detection)' AS testdesc, CASE WHEN (SELECT COUNT(*) FROM events_takeoff_testing WHERE event = 0) = 1 THEN TRUE ELSE FALSE END AS testresult;

-- take off with more previous points
SELECT update_events_takeoff('positions_testing', 'events_takeoff_testing', '2024-03-29 10:08:17', '2024-03-29 10:08:40');
INSERT INTO testcases (testcase, teststep, testdesc, testresult)
SELECT 'basic' AS testcase, 3 AS teststep, 'takeoff with event type 2 (3 point detection)' AS testdesc, CASE WHEN (SELECT COUNT(*) FROM events_takeoff_testing WHERE event = 2) = 1 THEN TRUE ELSE FALSE END AS testresult;

-- take off with more next points
SELECT update_events_takeoff('positions_testing', 'events_takeoff_testing', '2024-03-29 10:08:17', '2024-03-29 10:08:42');
INSERT INTO testcases (testcase, teststep, testdesc, testresult)
SELECT 'basic' AS testcase, 4 AS teststep, 'takeoff with event type 4 (4 point detection)' AS testdesc, CASE WHEN (SELECT COUNT(*) FROM events_takeoff_testing WHERE event = 4) = 1 THEN TRUE ELSE FALSE END AS testresult;

-- next airborne point
SELECT update_events_takeoff('positions_testing', 'events_takeoff_testing', '2024-03-29 10:08:17', '2024-03-29 10:08:43');
INSERT INTO testcases (testcase, teststep, testdesc, testresult)
SELECT 'basic' AS testcase, 5 AS teststep, 'takeoff with event type 6 (5 point detection)' AS testdesc, CASE WHEN (SELECT COUNT(*) FROM events_takeoff_testing WHERE event = 6) = 1 THEN TRUE ELSE FALSE END AS testresult;

-- complete range
SELECT update_events_takeoff('positions_testing', 'events_takeoff_testing', '2024-03-29 10:00:00', '2024-03-29 11:00:00');
INSERT INTO testcases (testcase, teststep, testdesc, testresult)
SELECT 'basic' AS testcase, 6 AS teststep, 'complete range: no change' AS testdesc, CASE WHEN (SELECT COUNT(*) FROM events_takeoff_testing WHERE event = 6) = 1 THEN TRUE ELSE FALSE END AS testresult;

-- just the end
SELECT update_events_takeoff('positions_testing', 'events_takeoff_testing', '2024-03-29 10:08:37', '2024-03-29 11:00:00');
INSERT INTO testcases (testcase, teststep, testdesc, testresult)
SELECT 'basic' AS testcase, 7 AS teststep, 'just the end: no change' AS testdesc, CASE WHEN (SELECT COUNT(*) FROM events_takeoff_testing WHERE event = 6) = 1 THEN TRUE ELSE FALSE END AS testresult;
