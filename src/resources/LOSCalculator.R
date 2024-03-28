#### BMG Pandemieradar NEU ####
required_packages <- c("conflicted", "dplyr", "readr", "tidyverse", "lubridate", "mosaic", "ISOweek", "r2r")
for (package_name in required_packages) {
  if (!require(package_name, quietly = TRUE)) {
    #install.packages(package_name, INSTALL_opts = '--no-lock')  # Install the package if it's not installed
  }
  library(package_name, character.only = TRUE)
}

conflicts_prefer(mosaic::max)
conflicts_prefer(mosaic::mean)


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

#' This function unpacks the data from the given path to a zip file
#' @param inDir: path to a zip file fetched from the broker
#' @param file_numbers: IDs of the clinics, whose data is to be calculated
unpackClinicResult <- function(exDir, file_numbers) {
  for (i in file_numbers) {
    inDirZip <- file.path(exDir, sprintf("%d_result.zip", i))
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

#' This function uses the unpacked zip archives from the broker results that 
#' contain result sets for each hospital. It adds the hospital number as a new column.
#' At the end a dataframe with all hospital results is created, identified by the hospital number
#' @param exDir the filepath to the unpacked broker result zip that contains the zip archives of all hospitals
#' @param file_numbers hospital numbers in directory name to identify corresponding hospital
processFiles <- function(exDir, file_numbers) {
  all_data_df <- data.frame()
  for (i in file_numbers) {
    filepath_i <- file.path(exDir, sprintf("%d_result/case_data.txt", i), fsep = "/")
    print(paste("This is used in processFiles: ", filepath_i))
    
    if (file.exists(filepath_i)) {
      df <- read_delim(
        filepath_i,
        delim = "\t", escape_double = FALSE,
        trim_ws = TRUE
      ) %>% mutate(clinic = i)
     
      if(!"entlassung_ts" %in% colnames(df)) {
        next
      } 
      
      if(!"aufnahme_ts" %in% colnames(df)) {
        df$aufnahme_ts <- NA
      }
      
      df <- df[complete.cases(df$entlassung_ts), ]  # remove rows with empty entlassung_ts
      df <- df %>% mutate(aufnahme_ts = coalesce(aufnahme_ts, triage_ts)) # empty aufnahme_ts are filled with corresponding triage_ts
      df <- df[complete.cases(df$aufnahme_ts), ]  # remove rows where aufnahme_ts is still empty
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

# performs various methods on the extracted data
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

# fills the case_data dataframe with info
fillCaseData <- function(case_data) {
  case_data$aufnahme_ts <- with_tz(case_data$aufnahme_ts)
  case_data$entlassung_ts <- with_tz(case_data$entlassung_ts)
  case_data$triage_ts <- with_tz(case_data$triage_ts)
  case_data$jahr <- year(case_data$aufnahme_ts)
  case_data$cw <- format(case_data$aufnahme_ts, "%V")
  case_data$calendarweek_year <- format(case_data$aufnahme_ts, "%G")
  case_data$los <- difftime(case_data$entlassung_ts, case_data$aufnahme_ts, units = "mins")
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
  result <- data.frame(favstats(db$los ~ db$clinic))
  names(result)[1] <- "clinic"  
  return(result)
}

filterLosValid <- function(los, num_of_cases) {
  los <- left_join(los, num_of_cases, by="clinic")
  los$np <- los$Freq - los$n
  los$np_prozent <- (los$np / los$Freq) * 100
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
  timeframe <- timeframe %>% dplyr::filter(cw != as.character(calendarweek-1) & cw != as.character(calendarweek+4))
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


args <- commandArgs(trailingOnly=TRUE)
# Access the path variable passed from Python
filepath <- args[1]
filepath <- '/home/wiliam/PycharmProjects/LOC_Calculator/test/resources/unittest_result.zip'

# Path to extraction location, regex on win: '\\\\' and linux '/'
exDir <- paste0(removeTrailingFileFromPath(filepath, '/'),"/broker_result")

if(!dir.exists(exDir)) {
  dir.create(exDir)
  print(paste("Directory", exDir, "created."))
} else {
  print(paste("Directory", exDir, "already exists."))
}

# file_numbers <- c(1:3, 8:35,37:44,47:52,55,56,60, 68,69,70)
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
timeframe_path <- paste0(exDir, "/timeframe.csv")
write.csv(timeframe, timeframe_path, row.names = FALSE)
print(paste0("timeframe_path:",timeframe_path))  


