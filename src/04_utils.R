################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 04_add_data.R
################################################################################

# Documentation: get_directories
# Usage: get_directories (path)
# Description: Gets list of sub-directories for given path
# Args/Options: path
# Returns: list
# Output: ...
get_directories <- function(path) {
	dirs <- list.dirs(path, full.names = TRUE)
	dirs <- dirs[-1]
	return(dirs)
}


# Documentation: unzip_gz
# Usage: unzip_gz (file)
# Description: Checks if the unzipped version already exists, if not -> gunzip
# Args/Options: file
# Returns: ...
# Output: ...
unzip_gz <- function(file_js, file_gz) {
	if (!file.exists(file_js)) {
		gunzip(file_gz, remove = FALSE)
	}
}
# Documentation: select_scooter_trips
# Usage: select_scooter_trips (dt)
# Description: Chooses the wanted columns and rows in a complex datatable
# Args/Options: dt
# Returns: datatable
# Output: ...
select_scooter_trips <- function(dt) {
	dt <- dt$data$categories$vehicles %>% bind_rows()
	dt <- dt %>%
		filter(type == "scooter")
	
	return(dt)
}

# Documentation: add_scooter_time_id
# Usage: add_scooter_time_id (dt, timestamp)
# Description: Extracts timestamp from js-file & combines it with scooter id
# Args/Options: dt, timestamp
# Returns: datatable
# Output: ...
add_scooter_time_id <- function(dt, timestamp) {
	dt <- dt %>%
		mutate(scooter_id = id,
					 id = paste0(id,"_",timestamp))
	
	return(dt)
	
}

# Documentation: add_timestamp
# Usage: add_timestamp (dt)
# Description: Creates timestamp-column based on "start_time" (y_m_d_h_m_s)
# Args/Options: dt
# Returns: datatable
# Output: ...
add_timestamp <- function(dt){
	dt <- dt %>%
		mutate(year = year(start_time),
					 month = month(start_time),
					 day = day(start_time),
					 hour = hour(start_time),
					 min = minute(start_time),
					 sec = second(start_time)) %>%
		mutate(timestamp = paste0(
			year,
			"_",
			month,
			"_",
			day,
			"_",
			hour,
			"_",
			min,
			"_",
			sec)) %>%
		filter(hour != 0) %>% # Rohdaten zur Stunde 0
		select(-all_of(
			c("year","month","day","hour","min","sec")
		))
	
	return(dt)
}



# Documentation: create_feather_files
# Usage: create_feather_files (input_path, output_path)
# Description: Reads .json.gz-files and creates feather-files
# Args/Options: input_path: Path to ".json.gz"-files
# Returns: ...
# Output: ...
# Action: Reads and writes files
create_feather_files <- function(input_path, output_path){
	
	# List all directories of the raw data containing battery information
	dirs <- get_directories(path = input_path)
	
	lapply(dirs, function(x){
		
		# List all files owithin these directories
		files <- list.files(x, pattern = "\\.gz$", full.names = TRUE)
		
		
		lapply(files, function(y){
			
			con <- gzcon(file(y, "rb"))
			stream <- stream_in(con)
			data <- stream$data$categories[[1]]
			data <- data$vehicles %>% bind_rows()
			data <- data %>%
				filter(type == "scooter")
			
			# Create timestamp from filename...
			timestamp <- strsplit(y,split="\\.")[[1]][1]
			timestamp <- strsplit(timestamp,split="/")[[1]]
			length_of_split <- length(timestamp)
			timestamp <- timestamp[length_of_split]
			
			# ...and add it to the data
			data <- data %>%
				mutate(timestamp = timestamp) %>%
				select(id, charge, timestamp)
			
			filename <- here(output_path, timestamp)
			fileending <- ".feather"
			path <- paste0(filename, fileending)
			write_feather(x = data, path = path)
			close(con)
		})
	})
}



# Documentation: sort_files_datetime_asc
# Usage: sort_files_datetime_asc(list)
# Description: Sorts raw data by ascending request-datetime
# Args/Options: list
# Returns: list
# Output: ...
sort_files_datetime_asc <- function(list) {
	res <- lapply(list, function(x){
		file <- strsplit(x, split="/")
		file <- file[[1]]
		file <- file[length(file)]
	})
	
	timestamps <- res %>% unlist
	formatted_timestamps <- as.POSIXct(timestamps, format = "%Y_%m_%d_%H_%M_%S")
	ordered_list <- list[order(formatted_timestamps)]
	
	return(ordered_list)
}


# Documentation: add_charge_col
# Usage: add_charge_col(dt)
# Description: Sorts raw data by ascending request-datetime
# Args/Options: list
# Returns: list
# Output: ...
add_charge_col <- function(dt){
	
	pb <- txtProgressBar(min = 0, max = 100, style = 3)
	
	for(i in 1:nrow(dt)){
		setTxtProgressBar(pb, i)
		
		id_i <- dt[i, "id"] %>% as.integer
		timestamp <- dt[i, "timestamp"]
		filename <- here(path_bolt_berlin_processed, timestamp)
		fileending <- ".feather"
		path <- paste0(filename, fileending)
		feather_idx <- match(path, list_raw_feather_files) - 1
		if(is.na(feather_idx)){
			next
		}
		data <- feather_data[[feather_idx]] 
		charge <- data %>%
			filter(id == id_i) %>%
			select(charge) %>%
			as.integer()
		dt[i, "charge"] <- charge
	}
}