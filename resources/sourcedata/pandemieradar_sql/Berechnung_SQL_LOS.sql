-- !preview conn=DBI::dbConnect(RSQLite::SQLite())

-- Hier vor müsten alle Daten aus dem Export zu einer Datei zusammengefügt werden: case_data

-- 1.Anpassen von Zeitpunkten in der Datenbank: eventuell auf dem broker nicht nötig?
-- 2.Extrahieren des Jahres, der Kalenderwoche und des Jahres der Kalenderwoche
UPDATE case_data
SET aufnahme_ts = CONVERT_TIMEZONE('UTC', aufnahme_ts),
    triage_ts = CONVERT_TIMEZONE('UTC', triage_ts),
    entlassung_ts = CONVERT_TIMEZONE('UTC', entlassung_ts);

UPDATE case_data
SET jahr = EXTRACT(YEAR FROM aufnahme_ts),
    KW = TO_CHAR(aufnahme_ts, 'IW'),
    kalenderwoche_jahr = TO_CHAR(aufnahme_ts, 'IYYY');

-- Was ist der erste Zeitpunkt Aufnahme oder Triage
UPDATE case_data
SET Vergleich = CASE WHEN triage_ts IS NULL THEN 0
                    WHEN aufnahme_ts > triage_ts THEN 1
                    ELSE 0 END;
                    
UPDATE case_data
SET ersterZeitpunkt = CASE WHEN triage_ts IS NULL THEN TO_CHAR(aufnahme_ts, 'YYYY-MM-DD HH24:MI:SS')
                           WHEN aufnahme_ts < triage_ts THEN TO_CHAR(aufnahme_ts, 'YYYY-MM-DD HH24:MI:SS')
                           ELSE TO_CHAR(triage_ts, 'YYYY-MM-DD HH24:MI:SS') END;
                           
-- Berechnung der Length of Stay (LOS)
UPDATE case_data
SET los = EXTRACT(EPOCH FROM (entlassung_ts - ersterZeitpunkt)) / 60;

-- Filtern von fällen ohne Entlassungzeit= keine LOS/ über 24 h / unter 0 Minuten / unter 1 Minute
-- Erstellen einer neuen Tabelle "db" nach Filtern
CREATE TABLE db AS
SELECT *
FROM case_data
WHERE los IS NOT NULL;

-- Filtern von "db" nach los >= 1
DELETE FROM db
WHERE los < 1;

-- Filtern von "db" nach los < 1440
DELETE FROM db
WHERE los >= 1440;

-- Ändern des Datentyps von "los" in numerisch (falls notwendig)
-- Erstellen einer neuen Tabelle "db2"
CREATE TABLE db2 AS
SELECT *
FROM db;

-- Filtern von Kliniken die über 20% Fehlerquote (unplausibel) haben und über 300 Minuten LOS im Mittel
-- Erstellen der neuen Tabelle "los"
CREATE TABLE los AS
SELECT db2.klinik AS klinik, 
       AVG(db2.los) AS mean, 
       SUM(anzahl_fälle.Freq) AS Freq, 
       SUM(anzahl_fälle.n) AS n
FROM db2
LEFT JOIN anzahl_fälle ON db2.klinik = anzahl_fälle.klinik
GROUP BY db2.klinik;
-- Berechnen von "np" und "np_prozent"
UPDATE los
SET np = Freq - n,
    np_prozent = (np / Freq) * 100;
-- Filtern von "los" nach np_prozent < 20 und mean < 300
DELETE FROM los
WHERE np_prozent >= 20 OR mean >= 300;

-- Jetzt werden die Kliniken rausgefiltert die drin bleiben und die aus der großen DB gelöscht die aussortiert wurden
-- Erstellen einer neuen Tabelle "los" mit nur der "klinik"-Spalte
CREATE TABLE los AS
SELECT klinik
FROM los;
-- Erstellen der neuen Tabelle "gesamt_db_Pand" durch LEFT JOIN
CREATE TABLE gesamt_db_Pand AS
SELECT *
FROM los
LEFT JOIN db2 ON los.klinik = db2.klinik;

-- Anzahl teilnehmender Kliniken für spätere Verwendung
CREATE TABLE kliniken AS
SELECT kalenderwoche_jahr, KW, COUNT(DISTINCT klinik) AS n
FROM gesamt_db_Pand
GROUP BY kalenderwoche_jahr, KW;

-- Berechnung Mittel pro Kalenderwoche/ ob dies hier das gewichtete Mittel ist weiß ich nicht genau müssen wir probieren
CREATE TABLE zeitraum AS
SELECT kalenderwoche_jahr, KW, 
       SUM(los * klinik) / SUM(klinik) AS weighted_mean_los
FROM gesamt_db_Pand
GROUP BY kalenderwoche_jahr, KW;

-- Zusätzliche mittlere Fallzahl pro Kalenderwoche
CREATE TABLE fallzahl AS
SELECT kalenderwoche_jahr, KW, klinik, COUNT(*) AS Freq
FROM gesamt_db_Pand
GROUP BY kalenderwoche_jahr, KW, klinik;

CREATE TABLE df AS
SELECT Var1 AS kalenderwoche_jahr, 
       Var2 AS KW, 
       AVG(Freq) AS mean_Fallzahl
FROM fallzahl
GROUP BY Var1, Var2;


DELETE FROM df
WHERE mean_Fallzahl = 0;

ALTER TABLE df
RENAME COLUMN kalenderwoche_jahr TO Var1,
              KW TO Var2;

CREATE TABLE zeitraum AS
SELECT z.kalenderwoche_jahr, z.KW, z.weighted_mean_los, df.mean_Fallzahl
FROM zeitraum z
LEFT JOIN df ON z.kalenderwoche_jahr = df.Var1 AND z.KW = df.Var2;

-- Eintragung des Vor-Pandemiewertes und Vergleich für Zunahme und Abnahme
UPDATE zeitraum
SET LOS_vor_Pand = 193.5357;

ALTER TABLE zeitraum
ADD COLUMN Abweichung NUMERIC;

UPDATE zeitraum
SET Abweichung = `weighted.mean(los, klinik)` - LOS_vor_Pand;

ALTER TABLE zeitraum
ADD COLUMN Veränderung VARCHAR(10);

UPDATE zeitraum
SET Veränderung = CASE WHEN Abweichung > 0 THEN 'Zunahme' ELSE 'Abnahme' END;

CREATE TABLE zeitraum_updated AS
SELECT z.*, k.n
FROM zeitraum z
LEFT JOIN kliniken k ON z.kalenderwoche_jahr = k.kalenderwoche_jahr AND z.KW = k.KW;

-- bei der Abfrage ist immer die erste WOche und die letzte zu viel die müssen raus, dies ändert sich aber jede Woche,
-- hier müssen wir mal schauen wie das automatisch gehen würde
DELETE FROM zeitraum
WHERE (kalenderwoche_jahr, KW) IN (
    SELECT kalenderwoche_jahr, MIN(KW) FROM zeitraum GROUP BY kalenderwoche_jahr
    UNION ALL
    SELECT kalenderwoche_jahr, MAX(KW) FROM zeitraum GROUP BY kalenderwoche_jahr
);

-- alles zusammen fassen zu einer DB
ALTER TABLE zeitraum
ADD COLUMN date VARCHAR(10);

UPDATE zeitraum
SET date = CONCAT(kalenderwoche_jahr, '-W', KW);

ALTER TABLE zeitraum
DROP COLUMN kalenderwoche_jahr,
             KW;

-- Umbenennen der verbleibenden Spalten
ALTER TABLE zeitraum
RENAME COLUMN weighted_mean_los TO los_mean,
              mean_Fallzahl TO visit_mean,
              LOS_vor_Pand TO los_reference,
              Abweichung TO los_difference,
              Veränderung TO change,
              n TO ed_count;

ALTER TABLE zeitraum
ORDER BY date, ed_count, visit_mean, los_mean, los_reference, los_difference, change;

UPDATE zeitraum
SET los_mean = ROUND(los_mean, 2),
    visit_mean = ROUND(visit_mean, 2),
    los_reference = ROUND(los_reference, 2),
    los_difference = ROUND(los_difference, 2);

-- Speichern der Daten nach vorgelegten Standard des RKI, dies müsste ja auch immer geändert werden pro Woche??

CREATE TEMPORARY TABLE KW_XX_XX_20XX AS
SELECT *
FROM zeitraum
WHERE date >= '20XX-WXX' AND date <= '20XX-WXX';

-- Export der temporären Tabelle in eine CSV-Datei
COPY KW_XX_XX_20XX TO '/Pfad/zur/CSV/Datei/LOS_20XX-WXX_to_20XX-WXX_20230629-094752.csv' WITH CSV HEADER DELIMITER ',';

-- Löschen der temporären Tabelle
DROP TABLE KW_XX_XX_20XX;
