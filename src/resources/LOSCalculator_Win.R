#### BMG Pandemieradar NEU ####
required_packages <- c("conflicted", "dplyr", "readr", "tidyverse", "lubridate", "mosaic", "ISOweek", "r2r")

# install all required packages
for (package_name in required_packages) {
  if (!require(package_name, character.only = TRUE)){
    install.packages(package_name)
    library(package_name, character.only = TRUE)
  }
}

conflicts_prefer(mosaic::max)
conflicts_prefer(mosaic::mean)
conflicts_prefer(mosaic::sum)
conflicts_prefer(dplyr::filter)

# Change Variables before using the script !!!!!!!!!!!!
#filepath <- '/home/wiliam/Downloads/export_9999.zip'
filepath <- 'C:\\Users\\wiliam\\Downloads\\LOS_resources\\export_9999.zip'
last_cw_last_month <- 29
first_cw_next_month <- 34

max_accepted_error <- 25 # in %, used to exclude data sources with an error rate of i% or higher
max_accepted_los <- 415 # in min, used to exclude data sources with an mean length of stay of i mins and higher

discharge_col_name <- 'entlassung_ts' # name of the discharge column from broker export data
admittance_col_name <- 'aufnahme_ts' # name of the admittance column from broker export data


#' Unpacks a zip file from the specified input directory to the specified extraction directory.
#' @param inDir: Character string specifying the path to the input zip file.
#' @param exDir: Character string specifying the path to the directory where the zip file will be extracted.
#' @return If successful, returns the path to the extraction directory. If an error occurs, returns NULL.
unpackZip <- function(inDir, exDir) {
  tryCatch({
    unzip(inDir, exdir = exDir)
    print(paste("Data from", inDir, "successfully unpacked in:", exDir))
    return(exDir)
  }, error = function(e) {
    warning(paste("An error occurred unpacking the data:", e$message))
    return(NULL)  # Return a value in case of error
  })
}

#' This function receives the directory of the unzipped broker result. Each
#' result zip in this directory will also be unzipped. Each result zip contains
#' a number of the originating clinic.
#' @param exDir: the filepath to the unpacked broker result zip that contains the zip archives of all hospitals
#' @param file_numbers: result IDs in exDir
unpackClinicResult <- function(exDir, file_numbers) {
  for (i in file_numbers) {
    path_zipped <- file.path(exDir, sprintf("%d_result.zip", i))
    path_unzipped <- file.path(exDir, sprintf("%d_result", i))
    if (!dir.exists(path_unzipped)) {
      dir.create(path_unzipped)
    }
    tryCatch({
      unzip(path_zipped, exdir = path_unzipped)
      print(paste("Data from", path_zipped," successfully unpacked in:", path_unzipped))
    }, error = function(e) {
      warning(paste("An error occurred unpacking the result sets:", e$message))
    })
  }
}

#' This function uses the unpacked zip archives from the broker results that
#' contain result sets for each hospital. It adds the hospital number as a new column.
#' At the end a dataframe with all hospital results is created, identified by the hospital number
#' @param exDir the filepath to the unpacked broker result zip that contains the zip archives of all hospitals
#' @param file_numbers hospital numbers in directory name to identify corresponding hospital
processFiles <- function(exDir, file_numbers) {
  all_data_df <- data.frame()
  error_rates <- data.frame(clinic=NA, errors=NA) # counts the number of invalid entries for each clinic

  for (i in file_numbers) {
    filepath_i <- file.path(exDir, sprintf("%d_result/case_data.txt", i), fsep = "/")
    print(paste("This is used in processFiles: ", filepath_i))

    if (file.exists(filepath_i)) {

      df <- read_delim(
        filepath_i,
        delim = "\t", escape_double = FALSE,
        trim_ws = TRUE
      ) %>% mutate(clinic = i)

      if(!discharge_col_name %in% colnames(df)) {
        next
      }

      if(!admittance_col_name %in% colnames(df)) {
        df$aufnahme_ts <- NA
      }

      all_data_df <- rbind(all_data_df, df)

    } else {
      print(paste("No file found: ", filepath_i))
      NULL
    }
  }

  if(nrow(all_data_df) > 0) {
    return(all_data_df)
  } else {
    return(NULL)
  }

}

#' This method calculates length of stay, analyses them and calculates a summary
#' @param case_data: A data frame containing case data of all clinics
#' @return A data frame representing the results of the length of stay analysis.
performAnalysis <- function(case_data) {
  filledCaseData <- fillCaseData(case_data)
  num_of_cases <- countClinics(filledCaseData)
  db <- filterCases(filledCaseData)
  los <- filterLos(db)
  los_valid <- filterLosValid(los, num_of_cases)
  complete_db_Pand <- joinClinics(db, los_valid)
  timeframe <- calculateTimeframe(complete_db_Pand, los)
  return(timeframe)
}

#' This method receives a data frame and put all datetime entries to the same
#' default timezone. It generates additional data from them like calendar week
#' and length of stay.
#' @param case_data: a data frame containing emergency department data
fillCaseData <- function(case_data) {
  # set dateteimes to same timezone
  case_data$aufnahme_ts <- with_tz(case_data$aufnahme_ts, tzone = "Europe/Berlin")
  case_data$entlassung_ts <- with_tz(case_data$entlassung_ts, tzone = "Europe/Berlin")
  case_data$triage_ts <- with_tz(case_data$triage_ts, tzone = "Europe/Berlin")

  # extract additional information from datetimes
  case_data$jahr <- year(case_data$aufnahme_ts)
  case_data$cw <- format(case_data$aufnahme_ts, "%V")
  case_data$calendarweek_year <- format(case_data$aufnahme_ts, "%G")

  # Use aufnahme_ts as default and reference value
  case_data$first_ts <- as.POSIXct(ifelse(is.na(case_data$aufnahme_ts), case_data$triage_ts, case_data$aufnahme_ts), tz = "Europe/Berlin")
  # if triage is beore aufnahme, use triage_ts in first_ts
  case_data$first_ts <- as.POSIXct(ifelse(!is.na(case_data$first_ts) & !is.na(case_data$triage_ts) & case_data$first_ts > case_data$triage_ts, case_data$triage_ts, case_data$aufnahme_ts), tz = "UTC")# todo else part is unnecessary, only keep triagets part
  case_data$los <- difftime(case_data$entlassung_ts, case_data$first_ts, units = "mins")
  return(case_data)
}

#' This method counts the number of clinics represented in case_data data frame
#' @param case_data: a data frame containing emergency department data
countClinics <- function(case_data) {
  num_of_cases <- data.frame(table(case_data$clinic))
  colnames(num_of_cases)[1] <- "clinic"
  return(num_of_cases)
}

#' This method receives a data frame and filters entries after the los column
#' @param case_data: a table of case data with length of stay already calculated
filterCases <- function(case_data) {
  db<-case_data%>%filter(is.na(los)==FALSE)
  db<-db%>%filter(los >=1)
  db<-db%>%filter(los <1440)
  db$los<-as.numeric(db$los)
  db2<-db
  return(db)
}

#' Summarise the length of stay data by clinic.
#' @param db: A data frame containing case data.
#' @return A data frame representing the summary statistics of length of stay (LOS) data grouped by clinic.
summariseLos <- function(db) {
  result <- data.frame(favstats(db$los ~ db$clinic))
  names(result)[1] <- "clinic"
  return(result)
}

#' Filters the length of stay data by clinic.
#' @param db: A data frame containing case data.
#' @return A data frame representing the summary statistics of length of stay (LOS) data grouped by clinic.
filterLos <- function(db) {
  result <- data.frame(favstats(db$los ~ db$clinic))
  names(result)[1] <- "clinic"
  return(result)
}

#' This Method removes clinic with high error rates and unrealistic high mean waiting times from table
#' @param los: table grouped by clinic and summarised los data
#' @param num_of_cases: clinic numbers
filterLosValid <- function(los, num_of_cases) {
  los <- left_join(los, num_of_cases, by="clinic")
  for(i in 1:nrow(los)) {
    los$Freq[los$clinic==i] <- los$Freq[los$clinic==i]
  }
  los$np <- los$Freq - los$n
  los$np_prozent <- (los$np / los$Freq) * 100
  los <- los %>% dplyr::filter(np_prozent < max_accepted_error)
  los <- los %>% dplyr::filter(mean < max_accepted_los)
  los <- data.frame(los$clinic)
  colnames(los)[1] <- "clinic"
  los$clinic <- as.double(los$clinic)
  return(los)
}

joinClinics <- function(db, los_valid) {
  complete_db_Pand <- left_join(los_valid, db)
  complete_db_Pand <- complete_db_Pand %>% dplyr::filter(clinic != 33)
  return(complete_db_Pand)
}

calculateTimeframe <- function(complete_db_Pand, los) {
  clinics <- complete_db_Pand %>% group_by(calendarweek_year, cw) %>% summarise(n = length(unique(clinic)))
  timeframe <- complete_db_Pand %>%
    group_by(calendarweek_year, cw) %>%
    summarise(weighted_los = mean(los))
  case_num <- calculateCaseNumber(complete_db_Pand)
  timeframe  <- left_join(timeframe, case_num)
  timeframe$LOS_vor_Pand <- 193.5357
  timeframe$Abweichung <- timeframe$weighted_los - timeframe$LOS_vor_Pand
  timeframe <- mutate(timeframe, Veraenderung = ifelse(Abweichung > 0, "Zunahme", "Abnahme"))
  timeframe <- left_join(timeframe, clinics)
  calendarweek <- getFirstCalendarWeekOfCurrentMonth()
  # timeframe <- timeframe %>% dplyr::filter(cw != as.character(calendarweek-1) & cw != as.character(calendarweek+4))
  timeframe <- timeframe %>% dplyr::filter(cw != last_cw_last_month & cw != first_cw_next_month)# todo rework, why only last and not second last, why manually. maybe a whitelist approach?
  timeframe$date <- paste(timeframe$calendarweek_year, "-W", timeframe$cw, sep = "")
  timeframe <- timeframe[, -c(1, 2)]
  colnames(timeframe) <- c("los_mean", "visit_mean", "los_reference", "los_difference", "change", "ed_count", "date")
  col_order <- c("date", "ed_count", "visit_mean", "los_mean", "los_reference", "los_difference", "change")
  timeframe <- timeframe[, col_order]
  timeframe <- timeframe %>% mutate_if(is.numeric, round, digits = 2)
  return(timeframe)
}

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
    summarise(mean_case_number = mosaic::mean(Freq, na.rm = TRUE))
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

removeTrailingFileFromPath <- function(filepath, regex) {
  index <- max(gregexpr(regex, filepath)[[1]])
  if (index > 1) {
    exDir <- substr(filepath, 1, index - 1)
    print(exDir)
    return(exDir)
  } else {
    print(paste("Kein'", regex, "'gefunden."))
  }
}

tablenameToEng <- function(var) {
  m <- hashmap()
  m[c("admission_ts", "discharge_ts", 3)] <- c("aufnahme_ts", "entlassung_ts", "c")
  return(m[var])
}

getHospitalNumbers <- function(path) {
  file_list <- list.files(path = path)
  zip_files <- file_list[grep("\\.zip", file_list)]
  hospital_numbers <- list()
  for(filename in zip_files) {
     hospital_numbers <- c(hospital_numbers, as.numeric(strsplit(filename, "_")[[1]][1]))
  }
  return(hospital_numbers)
}


main <- function(filepath){
  options(digits=2)
    exDir <- gsub(".zip", "", filepath)
    unpackZip(filepath, exDir)

    file_numbers <- getHospitalNumbers(exDir)
    unpackClinicResult(exDir, file_numbers)
    case_data <- processFiles(exDir, file_numbers)
    if(!is.null(case_data)) {
      timeframe <- performAnalysis(case_data)
    } else {
      timeframe <- data.frame("no_data" = {"no_data"})
      print("case_data is NULL, check the given table for missing columns.")
    }
    timeframe_stringified <- timeframe %>% mutate_all(as.character)
    print(timeframe_stringified)

    unlink(exDir, recursive = TRUE)
}

main(filepath)

