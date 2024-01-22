#### BMG Pandemieradar ####
# conflicted pakete installieren
library(conflicted)
library(dplyr)

# Pakete in R
library(readr)
library(tidyverse)
library(lubridate)
library(mosaic)

# Definition der S4-Klasse "DataProcessor"
fileort <- commandArgs(trailingOnly = TRUE)[1]
# fileort <- "C:\\Users\\whoy\\PycharmProjects\\pythonProject5\\libraries\\test_data.txt"
dataProcessor <- DataProcessor(fileort)
file_numbers <- c(1:3, 8:56, 58, 60, 67)
export <- getHighestExport(fileort)
unpackData(dataProcessor, file_numbers, export)
processFiles(dataProcessor)

setClass("DataProcessor",
         representation(fileort = "character"))

# Konstruktor-Methode für die Klasse
DataProcessor <- function(fileort) {
  obj <- new("DataProcessor", fileort = fileort)
  return(obj)
}

getHighestExport <- function(fileort) {
  # Listet alle Unterverzeichnisse in fileort auf
  subdirs <- list.dirs(fileort, full.names = FALSE, recursive = FALSE)

  # Extrahiert die Export-Werte als Zahlen
  export_values <- as.numeric(sub("^export_([0-9]+)$", "\\1", subdirs))

  # Wenn es keine Unterverzeichnisse gibt, gib 0 zurück
  if (length(export_values) == 0) {
    return(0)
  }

  # Gib den höchsten Export-Wert zurück
  return(max(export_values))
}

# Methode zum Entpacken der Daten
unpackData <- function(object, indices, export) {
  for (i in indices) {
    zipF <- file.path(object$fileort, sprintf("export_%s", getHighestExport(object$fileort)), sprintf("%d_result.zip", i))
    outDir <- file.path(object$fileort, sprintf("export_%s", getHighestExport(object$fileort)), sprintf("%d_result", i))

    # Überprüfen, ob das Ausgabeverzeichnis bereits existiert
    if (!dir.exists(outDir)) {
      dir.create(outDir)
    }

    tryCatch({
      unzip(zipF, exdir = outDir)
      print(paste("Dateien erfolgreich entpackt nach:", outDir))
    }, error = function(e) {
      warning(paste("Fehler beim Entpacken von Dateien:", e$message))
    })
  }
}

# Methode zum Einlesen und Verarbeiten der Dateien
processFiles <- function(object) {
  case_data <- bind_rows(
    lapply(object$fileNumbers, function(i) {
      file_path <- file.path(object$fileort, sprintf("%d_result\\test_data.txt", i))
      if (file.exists(file_path)) {
        cat("nach if: ", file_path, "\n")
        read_delim(
          file_path,
          delim = "\t", escape_double = FALSE,
          col_types = cols(aufnahme_ts = col_datetime(), entlassung_ts = col_datetime(), triage_ts = col_datetime()),
          trim_ws = TRUE
        ) %>% mutate(klinik = i)
      } else {
        NULL
      }
    })
  ) %>% filter(!is.null(aufnahme_ts) && nrow(.) > 0)

  # Löschen der temporären Dataframes
  rm(list = paste0("case_data_", object$fileNumbers))

  return(case_data)
}

fillCaseData <- function(case_data) {
  # Zeitstempel mit Zeitzone versehen
  case_data$aufnahme_ts <- with_tz(case_data$aufnahme_ts)
  case_data$triage_ts <- with_tz(case_data$triage_ts)
  case_data$entlassung_ts <- with_tz(case_data$entlassung_ts)

  # Neue Variablen Jahr, Kalenderwoche und Jahr der Kalenderwoche
  case_data$jahr <- year(case_data$aufnahme_ts)
  case_data$KW <- format(case_data$aufnahme_ts, "%V")
  case_data$kalenderwoche_jahr <- format(case_data$aufnahme_ts, "%G")

  # Testen erster Zeitpunkt
  case_data$ersterZ <- as.integer(case_data$aufnahme_ts > case_data$triage_ts)

  # Neue Variablen erstellen für erster Zeitpunkt des Kontaktes in der NA
  case_data$Vergleich <- ifelse(is.na(case_data$triage_ts), 0, as.integer(case_data$aufnahme_ts > case_data$triage_ts))
  case_data$ersterZeitpunkt <- ifelse(
    is.na(case_data$triage_ts),
    format(case_data$aufnahme_ts, "%Y-%m-%d %H:%M:%S"),
    format(pmax(case_data$aufnahme_ts, case_data$triage_ts), "%Y-%m-%d %H:%M:%S")
  )

  # Erstellen der Length of stay (LOS)
  case_data$los <- difftime(case_data$entlassung_ts, case_data$ersterZeitpunkt, units = "mins")

  return(case_data)
}

performAnalysis <- function(object) {
  anzahl_fälle <- countKliniken(object$case_data)
  db <- filterCases(object$case_data)
  los <- filterLos(db)
  los_valid <- filterLosValid(los, anzahl_fälle)
  gesamt_db_Pand <- joinKliniken(db, los_valid)
  zeitraum <- calculateZeitraum(gesamt_db_Pand)
  saveData(zeitraum)
  return(zeitraum)
}

# Weitere Hilfsfunktionen oder Methoden
countKliniken <- function(case_data) {
  anzahl_fälle <- data.frame(table(case_data$klinik))
  colnames(anzahl_fälle)[1] <- "klinik"
  return(anzahl_fälle)
}

filterCases <- function(case_data) {
  db <- case_data %>%
    dplyr::filter(is.na(los) == FALSE) %>%
    dplyr::filter(los >= 1) %>%
    dplyr::filter(los < 1440)
  db$los <- as.numeric(db$los)
  return(db)
}

filterLos <- function(db) {
  return(data.frame(favstats(db$los ~ db$klinik)))
}

filterLosValid <- function(los, anzahl_fälle) {
  los <- left_join(los, anzahl_fälle)
  los$np <- los$Freq - los$n
  los$np_prozent <- (los$np / los$Freq) * 100
  los <- los %>% dplyr::filter(np_prozent < 20)
  los <- los %>% dplyr::filter(mean < 300)
  los <- data.frame(los$klinik)
  colnames(los)[1] <- "klinik"
  los$klinik <- as.double(los$klinik)
  return(los)
}

joinKliniken <- function(db, los_valid) {
  gesamt_db_Pand <- left_join(los_valid, db)
  gesamt_db_Pand <- gesamt_db_Pand %>% dplyr::filter(klinik != 33)
  kliniken <- gesamt_db_Pand %>% group_by(kalenderwoche_jahr, KW) %>% summarise(n = length(unique(klinik)))
  return(gesamt_db_Pand, kliniken)
}

calculateZeitraum <- function(gesamt_db_Pand, kliniken, los) {
  zeitraum <- gesamt_db_Pand %>%
    group_by(kalenderwoche_jahr, KW) %>%
    summarise(weighted.mean(los, klinik))
  fallzahl <- calculateFallzahl(gesamt_db_Pand)
  zeitraum <- left_join(zeitraum, fallzahl)
  zeitraum$LOS_vor_Pand <- 193.5357
  zeitraum$Abweichung <- zeitraum$`weighted.mean(los, klinik)` - zeitraum$LOS_vor_Pand
  zeitraum <- mutate(zeitraum, Veränderung = ifelse(Abweichung > 0, "Zunahme", "Abnahme"))
  kliniken <- countKliniken(zeitraum)
  zeitraum <- left_join(zeitraum, kliniken)
  zeitraum <- zeitraum %>% dplyr::filter(KW != "21" & KW != "26")
  zeitraum$date <- paste(zeitraum$kalenderwoche_jahr, "-W", zeitraum$KW, sep = "")
  zeitraum <- zeitraum[, -c(1, 2)]
  colnames(zeitraum) <- c("los_mean", "visit_mean", "los_reference", "los_difference", "change", "ed_count", "date")
  col_order <- c("date", "ed_count", "visit_mean", "los_mean", "los_reference", "los_difference", "change")
  zeitraum <- zeitraum[, col_order]
  zeitraum <- zeitraum %>% mutate_if(is.numeric, round, digits = 2)
  return(zeitraum)
}

calculateFallzahl <- function(gesamt_db_Pand) {
  fallzahl <- data.frame(table(gesamt_db_Pand$kalenderwoche_jahr, gesamt_db_Pand$KW, gesamt_db_Pand$klinik))
  df <- fallzahl %>%
    group_by(Var1, Var2) %>%
    summarise(mean_Fallzahl = mosaic::mean(Freq, na.rm = TRUE))
  df <- df %>% dplyr::filter(mean_Fallzahl != 0)
  colnames(df) <- c("kalenderwoche_jahr", "KW", "mean_Fallzahl")
  df$KW <- as.character(df$KW)
  return(df)
}

saveData <- function(zeitraum) {
  KW_22_25_2023 <- zeitraum
  write.table(
    KW_22_25_2023,
    file = paste0(
      home_dir_windows,
      "\\OneDrive - Uniklinik RWTH Aachen\\Desktop\\pandemieradar_sql\\LOS_2023-W22_to_2023-W25_20230629-094752.csv"
    ),
    dec = "../../libraries",
    sep = ",",
    row.names = FALSE,
    quote = FALSE
  )
}







