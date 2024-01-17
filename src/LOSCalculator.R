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


dataProcessor <- DataProcessor(fileort)
file_numbers <- c(1:3, 8:56, 58, 60, 67)
unpackData(dataProcessor, file_numbers)

setClass("LOSCalculator", representation(filedir = "character"))

LOSCalculator <- function(filedir) {
  obj <- new("LOSCalculator", filedir = filedir)
  return(obj)
}

setGeneric("unpackData", function(object, indices) standardGeneric("unpackData"))

# Entpacken der Daten
setMethod("unpackData", signature(object = "DataProcessor", indices = "numeric"),
          function(object, file_numbers) {
            for (i in file_numbers) {
              zipF <- paste0(object$fileort, "export_", i, "_result.zip")
              outDir <- paste0(object$fileort, "export_", i, "_result")
              unzip(zipF, exdir = outDir)
            }
          })
