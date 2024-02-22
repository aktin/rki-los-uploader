#### BMG Pandemieradar NEU ####
library(conflicted)
library(dplyr)
library(readr)
library(tidyverse)
library(lubridate)
library(mosaic)
library(ISOweek)
library(r2r)

unpackFirstZip <- function(inDir, exDir) {
  tryCatch({
    unzip(inDir, exdir = exDir)
    print(paste("Data from", inDir, "successfully unpacked in:", exDir))
    return(exDir)
  }, error = function(e) {
    warning(paste("An error occurred unpacking the data:", e$message))
    return(NULL)  # Return a value in case of error
  })
}

#' This function unpacks the data from the given path to a zip file
#' @param inDir: path to a zip file fetched from the broker
#' @param filenumbers: IDs of the clinics, whose data is to be calculated
unpackRestData <- function(inDir, exDir, file_numbers) {
  for (i in file_numbers) {
    inDirZip <- file.path(inDir, sprintf("%d_result.zip", i))
    exDirZip <- file.path(exDir, sprintf("%d_result", i))
    if (!dir.exists(exDirZip)) {
      dir.create(exDirZip)
    }
    tryCatch({
      unzip(inDirZip, exdir = exDirZip)
      print(paste("Data from", inDirZip," successfully unpacked in:", exDirZip))
    }, error = function(e) {
      warning(paste("An error occurred unpacking the data:", e$message))
    })
  }
}

processFiles <- function(filepath, filenumbers) {
  case_data <- bind_rows(
    lapply(filenumbers, function(i) {
      filepath <- file.path(filepath, sprintf("%d_result\\case_data.txt", i))
      if (file.exists(filepath)) {
        read_delim(
          filepath,
          delim = "\t", escape_double = FALSE,
          col_types = cols(aufnahme_ts = col_datetime(), entlassung_ts = col_datetime(), triage_ts = col_datetime()),
          trim_ws = TRUE
        ) %>% mutate(clinic = i) #TODO Hashmap for german to english
      } else {
        NULL
      }
    })
  ) %>% dplyr::filter(!is.null(aufnahme_ts) && nrow(.) > 0)
  rm(list = paste0("case_data_", filenumbers))
  return(case_data)
}

# performs various methods on the extracted data
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

# fills the case_data dataframe with info
fillCaseData <- function(case_data) {
  case_data$aufnahme_ts <- with_tz(case_data$aufnahme_ts)
  case_data$triage_ts <- with_tz(case_data$triage_ts)
  case_data$discharge_ts <- with_tz(case_data$discharge_ts)
  case_data$jahr <- year(case_data$aufnahme_ts)
  case_data$cw <- format(case_data$aufnahme_ts, "%V")
  case_data$calendarweek_year <- format(case_data$aufnahme_ts, "%G")
  case_data$earliest_ts <- as.integer(case_data$aufnahme_ts > case_data$triage_ts)
  case_data$Vergleich <- ifelse(is.na(case_data$triage_ts), 0, as.integer(case_data$aufnahme_ts > case_data$triage_ts))
  case_data$earliest_ts <- ifelse(
    is.na(case_data$triage_ts),
    format(case_data$aufnahme_ts, "%Y-%m-%d %H:%M:%S"),
    format(pmax(case_data$aufnahme_ts, case_data$triage_ts), "%Y-%m-%d %H:%M:%S")
  )
  case_data$los <- difftime(case_data$discharge_ts, case_data$earliest_ts, units = "mins")
  return(case_data)
}

# a help-function to count the number of clinics
countClinics <- function(case_data) {
  num_of_cases <- data.frame(table(case_data$clinic))
  colnames(num_of_cases)[1] <- "clinic"
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
  return(data.frame(favstats(db$los ~ db$clinic)))
}

filterLosValid <- function(los, num_of_cases) {
  los <- left_join(los, num_of_cases)
  los$np <- los$freq - los$n
  los$np_prozent <- (los$np / los$freq) * 100
  los <- los %>% dplyr::filter(np_prozent < 20)
  los <- los %>% dplyr::filter(mean < 300)
  los <- data.frame(los$clinic)
  colnames(los)[1] <- "clinic"
  los$clinic <- as.double(los$clinic)
  return(los)
}

joinClinics <- function(db, los_valid) {
  complete_db_Pand <- left_join(los_valid, db)
  complete_db_Pand <- complete_db_Pand %>% dplyr::filter(clinic != 33)
  clinics <- complete_db_Pand %>% group_by(calendarweek_year, cw) %>% summarise(n = length(unique(clinic)))
  return(complete_db_Pand, clinics)
}

calculateTimeframe <- function(complete_db_Pand, clinics, los) {
  timeframe <- complete_db_Pand %>%
    group_by(calendarweek_year, cw) %>%
    summarise(weighted.mean(los, clinic))
  case_num <- calculateCaseNumber(complete_db_Pand)
  timeframe <- left_join(timeframe, case_num)
  timeframe$LOS_vor_Pand <- 193.5357
  timeframe$Abweichung <- timeframe$`weighted.mean(los, clinic)` - timeframe$LOS_vor_Pand
  timeframe <- mutate(timeframe, Veraenderung = ifelse(Abweichung > 0, "Zunahme", "Abnahme"))
  clinics <- countClinics(timeframe)
  timeframe <- left_join(timeframe, clinics)
  calendarweek <- getFirstCalendarWeekOfCurrentMonth()
  timeframe <- timeframe %>% dplyr::filter(cw != as.character(calendarweek-1) & cw != as.character(calendarweek+4))
  timeframe$date <- paste(timeframe$calendarweek_year, "-W", timeframe$cw, sep = "")
  timeframe <- timeframe[, -c(1, 2)]
  colnames(timeframe) <- c("los_mean", "visit_mean", "los_reference", "los_difference", "change", "ed_count", "date")
  col_order <- c("date", "ed_count", "visit_mean", "los_mean", "los_reference", "los_difference", "change")
  timeframe <- timeframe[, col_order]
  timeframe <- timeframe %>% mutate_if(is.numeric, round, digits = 2)
  return(timeframe)
}

# returns the first calendarweek from the current month
getFirstCalendarWeekOfCurrentMonth <- function() {
  current_date <- Sys.Date()
  first_day_in_month <- floor_date(current_date, "month")
  first_cw_in_month <- isoweek(first_day_in_month)
  return(first_cw_in_month)
}

calculateCaseNumber <- function(complete_db_Pand) {
  case_num <- data.frame(table(complete_db_Pand$calendarweek_year, complete_db_Pand$cw, complete_db_Pand$clinic))
  df <- case_num %>%
    group_by(Var1, Var2) %>%
    summarise(mean_case_number = mosaic::mean(freq, na.rm = TRUE))
  df <- df %>% dplyr::filter(mean_case_number != 0)
  colnames(df) <- c("calendarweek_year", "cw", "mean_case_number")
  df$cw <- as.character(df$cw)
  return(df)
}

saveData <- function(filepath, timeframe) {
  write.table(
    "Table for: " + timeframe,
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

removeTrailingFileFromPath <- function(filepath) {
  index <- mosaic::max(gregexpr("/", filepath)[[1]])
  if (index != -1) {
    exDir <- substr(filepath, 1, index - 1)
    print(exDir)
    return(exDir)
  } else {
    print("Kein '/' gefunden.")
  }
}

tablenameToEng <- function(var) {
  m <- hashmap()
  m[c("aufnahme_ts", "entlassung_ts", 3)] <- c("admission_ts", "discharge_ts", "c")
  return(m[var])
}

# filepath <- commandArgs(trailingOnly = TRUE)[1]
filepath <- "C:/Users/User/PycharmProjects/LOC_Calculator/libraries/broker_test_results.zip"
exDir <- removeTrailingFileFromPath(filepath)
# file_numbers <- c(1:3, 8:35,37:44,47:52,55,56, 60, 68,69,70)
file_numbers <- c(1, 2)
newInDir <- unpackFirstZip(filepath, exDir)
unpackRestData(newInDir, exDir, file_numbers)
case_data <- processFiles(newInDir, file_numbers)
timeframe <- performAnalysis(case_data)
saveData(exDir, timeframe)






