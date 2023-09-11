# REad data in mysql
1. create table:
CREATE TABLE ed_data (
aufnahme_ts varchar(255),
entlassung_ts varchar(255),
triage_ts varchar(255),
a_encounter_num	varchar(255),
a_encounter_ide	varchar(255),
a_billing_ide varchar(255)
);
2. SHOW GLOBAL VARIABLES LIKE 'local_infile';
3. SET GLOBAL local_infile = 'ON';
4. GRANT FILE on *.* to user@'localhost'
5. LOAD DATA LOCAL INFILE 'path/test_data.txt' INTO TABLE ed_data;

Loads the data from the own system


## Important
Calculator reads UTC Timezones and converts them into current timezone for calculation


## Verfahren
### Einlesen
1. Daten aus den Dateien einlesen
2. Daten prüfen ob nicht leer (nicht notwendig durch mysql?)
3. Daten zusammenführen zu einer Tabelle
### Daten aufbereiten
4. Daten und Uhrzeiten nach zeitzonen umformatieren zur aktuellen Zeitzone (wird mit timestamp beim einlesen automatisch gemacht?)
### Spalte für Jahr, Kalenderwoche und Kalenderwoche Jahr hinzufügen
5. Aus spalte "aufnahme_ts" das Jahr, die Kalenderwoche und jahr der kalenderwoche (???) extrahieren
### Spalte für ersterZ erstellen (wahrscheinlich redundant mit ersterZeiptunkt)
6. Neue Spalte "ersterZ", mit wert 1 wenn "aufnahme_ts" > "triage_ts" sonst 0
### Spalte vergleich erstellen
7. Spalte "vergleich" erstellen
8. wenn die Zelle in "triage_ts" "null" oder leer ist, dann 0 in spalte "vergleich" eintragen für diese Zeile
9. sonst: 1 wenn aufnahme_ts > triage_ts
### Spalte ersterZeitpunkt erstellen
10. Spalte "ersterZeitpunkt" erstellen (möglicherweise mit _ts markieren?)
11. Trage den timestamp ein aus "aufnahme_ts" und "triage_ts" der früher ist
12. wenn triage_ts null ist, aufnahme_ts direkt nehmen
### LOS spalte erstellen
13. 








