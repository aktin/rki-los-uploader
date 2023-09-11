Drop Table ed_data;
CREATE TABLE ed_data (
aufnahme_ts TIMESTAMP,
entlassung_ts TIMESTAMP,
triage_ts TIMESTAMP,
a_encounter_num	varchar(255),
a_encounter_ide	varchar(255),
a_billing_ide varchar(255)
);

-- define acess parameters for access on datafile
SHOW GLOBAL VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = 'ON';
GRANT FILE on *.* to 'root'@'localhost';

-- daten einlesen, ignoriert die Titelzeile
-- TODO, prüfe ob die zeitzone richtig gesetzt wird
SET SESSION time_zone = '+2:00';
LOAD DATA LOCAL INFILE 'F:/pandemieradar_sql/export_9999/1_result/test_data.txt' INTO TABLE ed_data IGNORE 1 LINES;
LOAD DATA LOCAL INFILE 'F:/pandemieradar_sql/export_9999/2_result/test_data.txt' INTO TABLE ed_data IGNORE 1 LINES;
 
 
 SELECT count(*)
 From ed_data;
 
  SELECT *
 From ed_data;