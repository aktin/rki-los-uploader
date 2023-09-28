#### BMG Pandemieradar ####
# conflicted pakete installieren
library(conflicted)
library(dplyr)

# Pakete in R
library(readr)
library(tidyverse)
library(lubridate)
library(mosaic)

#### Einlesen Anpassen ####
# Einlesen der Export-nummer Anpassen
export <- "2190"
# Verzeichnis der Exporte Anpassen
fileort <- "C:\\Users\\whoy\\PycharmProjects\\pythonProject5\\libraries\\test_data.txt"

# Enpacken der Daten
for (i in c(1:3, 8:56, 58, 60,67)) {
  zipF <- paste0(fileort, "export_", export, "/", i, "_result.zip")
  outDir <- paste0(fileort, "export_", export, "/", i, "_result")
  unzip(zipF, exdir = outDir)
}

# Liste der Dateinummern
file_numbers <- c(1:3, 8:44,48:58, 60, 67)

# Einlesen und Verarbeiten der Dateien
case_data <- lapply(file_numbers, function(i) {
  home_dir_windows <- Sys.getenv("USERPROFILE")
  file_path <- sprintf(paste0(home_dir_windows,"\\OneDrive - Uniklinik RWTH Aachen\\Desktop\\pandemieradar_sql\\export_9999\\%d_result\\test_data.txt"), i)
  if (file.exists(file_path)) {
    print(paste("nach if: ",file_path))
    read_delim(file_path, 
               delim = "\t", escape_double = FALSE, 
               col_types = cols(aufnahme_ts = col_datetime(), entlassung_ts = col_datetime(), triage_ts = col_datetime()), 
               trim_ws = TRUE) %>% mutate(klinik = i)
  } else {
    NULL
  }
})

# Filtern der gültigen Dataframes
valid_case_data <- case_data[sapply(case_data, function(df) !is.null(df) && nrow(df) > 0)]

# Zusammenführen zu einem großen Dataframe
case_data_all <- bind_rows(valid_case_data)

# Löschen der temporären Dataframes
rm(list = paste0("case_data_", file_numbers))
case_data<-case_data_all

####  Anpassen der Zeitpunkte zur lokalen zeitzone #### 
case_data$aufnahme_ts<-with_tz(case_data$aufnahme_ts)
case_data$triage_ts<-with_tz(case_data$triage_ts)
case_data$entlassung_ts<-with_tz(case_data$entlassung_ts)

# Neue Variablen Jahr, Kalenderwoche und Jahr der Kalenderwoche
case_data$jahr<-year(case_data$aufnahme_ts)
case_data$KW<-format(case_data$aufnahme_ts,"%V")
case_data$kalenderwoche_jahr<-format(case_data$aufnahme_ts,"%G")

####  Testen erster Zeitpunkt #### 
case_data$ersterZ<-ifelse(case_data$aufnahme_ts>case_data$triage_ts,1,0)

####  Neue Variablen erstellen für erster Zeitpunkt des Kontaktes in der NA
case_data$Vergleich <- ifelse(is.na(case_data$triage_ts), 0, ifelse(case_data$aufnahme_ts > case_data$triage_ts, 1, 0))
case_data$ersterZeitpunkt <- ifelse(is.na(case_data$triage_ts), format(as.POSIXct(case_data$aufnahme_ts, origin = "1970-01-01"), "%Y-%m-%d %H:%M:%S"), 
                                    format(as.POSIXct(ifelse(case_data$aufnahme_ts < case_data$triage_ts, case_data$aufnahme_ts, case_data$triage_ts), 
                                                      origin = "1970-01-01"), "%Y-%m-%d %H:%M:%S"))
#### Erstellen der Length of stay(LOS) #### 
case_data$los<-difftime(case_data$entlassung_ts,case_data$ersterZeitpunkt,units = c("mins"))

#### Klinken #### 
anzahl_fälle<-data.frame(table(case_data$klinik))
colnames(anzahl_fälle)[1]<-"klinik"

####  Filtern ohne Entlassungzeit= keine LOS/ über 24 h / unter 0 Minuten / unter 1 Minute #### 
db<-case_data%>%dplyr::filter(is.na(los)==FALSE)
db<-db%>%dplyr::filter(los >=1)
db<-db%>%dplyr::filter(los <1440)
db$los<-as.numeric(db$los)
db2<-db

#### Filtern der Kliniken mit über 20% Fehlerquote (unplausibel) und über 300 Minuten #### 
los<-data.frame(favstats(db2$los~db2$klinik))
colnames(los)[1]<-"klinik"
los<-left_join(los,anzahl_fälle)
los$np<-los$Freq-los$n
los$np_prozent<-(los$np/los$Freq)*100
los<-los%>%dplyr::filter(np_prozent<20)
los<-los%>%dplyr::filter(mean<300)
# Kliniken die übrig bleiben
los<-data.frame(los$klinik)
colnames(los)[1]<-"klinik"
los$klinik<-as.double(los$klinik)
####  Verknüpfung mit der Datenbank und nur behalten der gültigen Fälle #### 
gesamt_db_Pand<-left_join(los,db2)
# KLINIK 33 MUSS immer raus
gesamt_db_Pand<-gesamt_db_Pand%>%dplyr::filter(klinik !=33)

kliniken<-gesamt_db_Pand%>%group_by(kalenderwoche_jahr,KW)%>%summarise(n = length(unique(klinik)))

####  Berechnung Mittel pro Kalenderwoche #### 
zeitraum<-gesamt_db_Pand %>%                                           # Weighted mean by group
  group_by(kalenderwoche_jahr,KW) %>% 
  summarise(weighted.mean(los,klinik))

####  Zusätzliche mittlere Fallzahl pro Kalenderwoche #### 
fallzahl<-data.frame(table(gesamt_db_Pand$kalenderwoche_jahr,gesamt_db_Pand$KW,gesamt_db_Pand$klinik))
df<-fallzahl %>% 
  group_by(Var1, Var2) %>% 
  summarise(mean_Fallzahl = mosaic::mean(Freq, na.rm = TRUE))
df<-df%>%dplyr::filter(mean_Fallzahl !=0)
colnames(df)<-c("kalenderwoche_jahr","KW","mean_Fallzahl")
df$KW<-as.character(df$KW)
zeitraum$kalenderwoche_jahr<-as.factor(zeitraum$kalenderwoche_jahr)
zeitraum<-left_join(zeitraum,df)

#### ANPASsUNG LOS VOR PANDEMIE #### 
zeitraum$LOS_vor_Pand<-193.5357
zeitraum$Abweichung<-zeitraum$`weighted.mean(los, klinik)`-zeitraum$LOS_vor_Pand
zeitraum <- mutate(zeitraum, Veränderung = ifelse(Abweichung > 0, "Zunahme", "Abnahme"))
zeitraum<-left_join(zeitraum,kliniken)
## !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
## Hier wöchentliche Anpassung notwendig
zeitraum<-zeitraum%>%dplyr::filter(KW != "21" & KW!="26")

zeitraum$date<-paste(zeitraum$kalenderwoche_jahr,"-W",zeitraum$KW,sep = "")
zeitraum<-zeitraum[,-c(1,2)]
colnames(zeitraum)<-c("los_mean","visit_mean","los_reference","los_difference","change","ed_count","date")

col_order <- c("date", "ed_count", "visit_mean",
               "los_mean", "los_reference","los_difference","change")
zeitraum <- zeitraum[, col_order]

#### Anpassung und Speicherung der Daten#### 
## Hier wöchentliche Anpassung notwendig
zeitraum<-zeitraum %>% 
  mutate_if(is.numeric, round, digits = 2)
KW_22_25_2023<-zeitraum
write.table(KW_22_25_2023,file = paste0(home_dir_windows,"\\OneDrive - Uniklinik RWTH Aachen\\Desktop\\pandemieradar_sql\\LOS_2023-W22_to_2023-W25_20230629-094752.csv"),dec=".",sep = ",",row.names = FALSE,quote=FALSE)

