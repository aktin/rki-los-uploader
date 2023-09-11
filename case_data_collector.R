
BMG Pandemieradar ####
# Pakete in R
library(readr)
library(tidyverse)
library(lubridate)
library(mosaic)
#### Einlesen Anpassen ####
# Einlesen der Export-nummer Anpassen
export <- "9999"
# Verzeichnis der Exporte Anpassen
fileort <- "/"

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
  file_path <- sprintf("~/Users/Wiliam/IdeaProjects/LOC_Calculator/resources/sourcedata/pandemieradar_sql/export_9999/%d_result/case_data.txt", i)
  if (file.exists(file_path)) {
    read_delim(file_path, 
               delim = "\t", escape_double = FALSE, 
               col_types = cols(aufnahme_ts = col_datetime(), entlassung_ts = col_datetime(), triage_ts = col_datetime()), 
               trim_ws = TRUE) %>% mutate(klinik = i)
  } else {
    NULL
  }
})

file_path <- "~/Users/Wiliam/IdeaProjects/LOC_Calculator/resources/sourcedata/pandemieradar_sql/export_9999/data_all.txt"

# Export the dataframe to a tab-delimited text file
write.table(case_data, file = file_path, sep = "\t", row.names = FALSE)