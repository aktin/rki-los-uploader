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
fileort <- "C:\\Users\\whoy\\PycharmProjects\\pythonProject5\\libraries\\test_data.txt"
dataProcessor <- DataProcessor(fileort)
file_numbers <- c(1:3, 8:56, 58, 60, 67)
unpackData(dataProcessor, file_numbers)

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
setGeneric("unpackData", function(object, file_numbers) standardGeneric("unpackData"))

setMethod("unpackData", signature(object = "DataProcessor", indices = "numeric", export = "character"),
          function(object, indices, export) {
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
          })

# Methode zum Einlesen und Verarbeiten der Dateien
setGeneric("processFiles", function(object) standardGeneric("processFiles"))

setMethod("processFiles", signature(object = "DataProcessor"),
          function(object) {
            processFile <- function(i) {
              home_dir_windows <- Sys.getenv("USERPROFILE")
              file_path <- file.path(home_dir_windows, "OneDrive - Uniklinik RWTH Aachen", "Desktop", "pandemieradar_sql", "export_9999", sprintf("%d_result", i), "test_data.txt")

              if (file.exists(file_path)) {
                print(paste("nach if:", file_path))
                read_delim(file_path,
                           delim = "\t", escape_double = FALSE,
                           col_types = cols(aufnahme_ts = col_datetime(), entlassung_ts = col_datetime(), triage_ts = col_datetime()),
                           trim_ws = TRUE) %>% mutate(klinik = i)
              } else {
                NULL
              }
            }

            purrr::map(object$file_numbers, processFile)
          })



