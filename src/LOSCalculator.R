#### BMG Pandemieradar NEU ####
library(conflicted)
library(dplyr)
library(readr)
library(tidyverse)
library(lubridate)
library(mosaic)
library(ISOweek)

unpackData <- function(filepath, file_numbers) {
  for (i in file_numbers) {
    zipF <- file.path(filepath, sprintf("%d_result.zip", i))
    outDir <- file.path(filepath, sprintf("%d_result", i))
    if (!dir.exists(outDir)) {
      dir.create(outDir)
    }
    tryCatch({
      unzip(zipF, exdir = outDir)
      print(paste("Data successfully unpacked in:", outDir))
    }, error = function(e) {
      warning(paste("An Error occurred unpacking the data:", e$message))
    })
  }
}

processFiles <- function(filepath, filenumbers) {
  case_data <- bind_rows(
    lapply(filenumbers, function(i) {
      file_path <- file.path(filepath, sprintf("%d_result\\test_data.txt", i))
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
  ) %>% dplyr::filter(!is.null(aufnahme_ts) && nrow(.) > 0)
  rm(list = paste0("case_data_", dataProcessor$fileNumbers))
  return(case_data)
}

performAnalysis <- function(case_data) {
  filledCaseData <- fillCaseData(case_data)
  num_of_cases <- countClinics(filledCaseData)
  db <- filterCases(filledCaseData)
  los <- filterLos(db)
  los_valid <- filterLosValid(los, num_of_cases)
  complete_db_Pand <- joinClinics(db, los_valid)
  timeframe <- calculateTimeframe(complete_db_Pand)
  return(timeframe)
}

fillCaseData <- function(case_data) {
  case_data$aufnahme_ts <- with_tz(case_data$aufnahme_ts)
  case_data$triage_ts <- with_tz(case_data$triage_ts)
  case_data$entlassung_ts <- with_tz(case_data$entlassung_ts)
  case_data$jahr <- year(case_data$aufnahme_ts)
  case_data$KW <- format(case_data$aufnahme_ts, "%V")
  case_data$kalenderwoche_jahr <- format(case_data$aufnahme_ts, "%G")
  case_data$ersterZ <- as.integer(case_data$aufnahme_ts > case_data$triage_ts)
  case_data$Vergleich <- ifelse(is.na(case_data$triage_ts), 0, as.integer(case_data$aufnahme_ts > case_data$triage_ts))
  case_data$ersterZeitpunkt <- ifelse(
    is.na(case_data$triage_ts),
    format(case_data$aufnahme_ts, "%Y-%m-%d %H:%M:%S"),
    format(pmax(case_data$aufnahme_ts, case_data$triage_ts), "%Y-%m-%d %H:%M:%S")
  )
  case_data$los <- difftime(case_data$entlassung_ts, case_data$ersterZeitpunkt, units = "mins")
  return(case_data)
}

countClinics <- function(case_data) {
  num_of_cases <- data.frame(table(case_data$klinik))
  colnames(num_of_cases)[1] <- "klinik"
  return(num_of_cases)
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

filterLosValid <- function(los, num_of_cases) {
  los <- left_join(los, num_of_cases)
  los$np <- los$Freq - los$n
  los$np_prozent <- (los$np / los$Freq) * 100
  los <- los %>% dplyr::filter(np_prozent < 20)
  los <- los %>% dplyr::filter(mean < 300)
  los <- data.frame(los$klinik)
  colnames(los)[1] <- "klinik"
  los$klinik <- as.double(los$klinik)
  return(los)
}

joinClinics <- function(db, los_valid) {
  complete_db_Pand <- left_join(los_valid, db)
  complete_db_Pand <- complete_db_Pand %>% dplyr::filter(klinik != 33)
  clinics <- complete_db_Pand %>% group_by(kalenderwoche_jahr, KW) %>% summarise(n = length(unique(klinik)))
  return(complete_db_Pand, clinics)
}

calculateTimeframe <- function(complete_db_Pand, clinics, los) {
  timeframe <- complete_db_Pand %>%
    group_by(kalenderwoche_jahr, KW) %>%
    summarise(weighted.mean(los, klinik))
  case_num <- calculateCaseNumber(complete_db_Pand)
  timeframe <- left_join(timeframe, case_num)
  timeframe$LOS_vor_Pand <- 193.5357
  timeframe$Abweichung <- timeframe$`weighted.mean(los, klinik)` - timeframe$LOS_vor_Pand
  timeframe <- mutate(timeframe, Veraenderung = ifelse(Abweichung > 0, "Zunahme", "Abnahme"))
  clinics <- countClinics(timeframe)
  timeframe <- left_join(timeframe, clinics)
  calendarweek <- getCurrentCalendarWeek()
  timeframe <- timeframe %>% dplyr::filter(KW != as.character(calendarweek-1) & KW != as.character(calendarweek+4))
  timeframe$date <- paste(timeframe$kalenderwoche_jahr, "-W", timeframe$KW, sep = "")
  timeframe <- timeframe[, -c(1, 2)]
  colnames(timeframe) <- c("los_mean", "visit_mean", "los_reference", "los_difference", "change", "ed_count", "date")
  col_order <- c("date", "ed_count", "visit_mean", "los_mean", "los_reference", "los_difference", "change")
  timeframe <- timeframe[, col_order]
  timeframe <- timeframe %>% mutate_if(is.numeric, round, digits = 2)
  return(timeframe)
}

getCurrentCalendarWeek <- function() {
  current_date <- Sys.Date()
  first_day_in_month <- floor_date(current_date, "month")
  first_cw_in_month <- isoweek(first_day_in_month)
  return(first_cw_in_month)
}

calculateCaseNumber <- function(complete_db_Pand) {
  case_num <- data.frame(table(complete_db_Pand$kalenderwoche_jahr, complete_db_Pand$KW, complete_db_Pand$klinik))
  df <- case_num %>%
    group_by(Var1, Var2) %>%
    summarise(mean_Fallzahl = mosaic::mean(Freq, na.rm = TRUE))
  df <- df %>% dplyr::filter(mean_Fallzahl != 0)
  colnames(df) <- c("calendarweek_year", "KW", "mean_case_number")
  df$KW <- as.character(df$KW)
  return(df)
}

saveData <- function(filepath, timeframe) {
  write.table(
    timeframe,
    file = paste0(
      home_dir_windows,
      filepath
    ),
    dec = ".",
    sep = ",",
    row.names = FALSE,
    quote = FALSE
  )
}

# filepath <- commandArgs(trailingOnly = TRUE)[1]
filepath <- "C:\\Users\\mjavdoschin\\PycharmProjects\\LOC_Calculator\\libraries"
file_numbers <- c(1:3, 8:35,37:44,47:52,55,56, 60, 68,69,70)
unpackData(filepath, file_numbers)
case_data <- processFiles(filepath, filenumbers)
timeframe <- performAnalysis(case_data)
saveData(filepath, timeFrame)






