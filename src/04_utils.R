################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 04_create_feather_files.R
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




# Documentation: create_feather_files_bolt
# Usage: create_feather_files_bolt (input_path, output_path)
# Description: Reads BOLT-.json.gz-files and creates feather-files
# Args/Options: input_path: Path to ".json.gz"-files
# Returns: ...
# Output: ...
# Action: Reads and writes files
create_feather_files_bolt <- function(input_path, output_path){
	
	# List all directories of the raw data containing battery information
	dirs <- get_directories(path = input_path)

	# pb <- txtProgressBar(min = 1, max = length(dirs), style = 3)
	
	lapply(dirs, function(x){
		
		# List all files owithin these directories
		files <- list.files(x, pattern = "\\.gz$", full.names = TRUE)
		# setTxtProgressBar(pb, x)
		
		lapply(files, function(y){
			
			con <- gzcon(file(y, "rb"))
			stream <- stream_in(con)
			print(stream)
			data <- stream$data$categories[[1]]
			data <- data$vehicles %>% bind_rows()
			print(data)
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



# Documentation: create_feather_files_tier
# Usage: create_feather_files_tier (input_path, output_path)
# Description: Reads TIER-.json.gz-files and creates feather-files
# Args/Options: input_path: Path to ".json.gz"-files
# Returns: ...
# Output: ...
# Action: Reads and writes files
create_feather_files_tier <- function(input_path, output_path){
	
	# List all directories of the raw data containing battery information
	dirs <- get_directories(path = input_path)
	
	# pb <- txtProgressBar(min = 1, max = length(dirs), style = 3)
	
	lapply(dirs, function(x){
		
		# List all files owithin these directories
		files <- list.files(x, pattern = "\\.gz$", full.names = TRUE)
		# setTxtProgressBar(pb, x)
		
		lapply(files, function(y){
			
			con <- gzcon(file(y, "rb"))
			stream <- stream_in(con)

			data <- stream$data[[1]]

			data <- data %>%
				filter(attributes$vehicleType == "escooter")
			
			# Create timestamp from filename...
			timestamp <- strsplit(y,split="\\.")[[1]][1]
			timestamp <- strsplit(timestamp,split="/")[[1]]
			length_of_split <- length(timestamp)
			timestamp <- timestamp[length_of_split]
			
			# ...and add it to the data
			data <- data %>%
				mutate(timestamp = timestamp,
							 charge = attributes$batteryLevel,
							 charge_distance = attributes$currentRangeMeters) %>%
				select(id, 
							 charge, 
							 charge_distance, 
							 timestamp)
			
			filename <- here(output_path, timestamp)
			fileending <- ".feather"
			path <- paste0(filename, fileending)
			write_feather(x = data, path = path)
			close(con)
		})
	})
}




create_feather_files_voi <- function(input_path, output_path){
	
	# List all directories of the raw data containing battery information
	dirs <- get_directories(path = input_path)
	
	# pb <- txtProgressBar(min = 1, max = length(dirs), style = 3)
	
	lapply(dirs, function(x){
		
		# List all files owithin these directories
		files <- list.files(x, pattern = "\\.gz$", full.names = TRUE)
		# setTxtProgressBar(pb, x)
		
		lapply(files, function(y){
			
			con <- gzcon(file(y, "rb"))
			stream <- stream_in(con)
			vehicle_groups <- stream$data$vehicle_groups[[1]]
			vehicles  <- vehicle_groups$vehicles[[1]]

			data <- vehicles %>%
				filter(category == "scooter")
			
			# Create timestamp from filename...
			timestamp <- strsplit(y,split="\\.")[[1]][1]
			timestamp <- strsplit(timestamp,split="/")[[1]]
			length_of_split <- length(timestamp)
			timestamp <- timestamp[length_of_split]
			
			# ...and add it to the data
			data <- data %>%
				mutate(timestamp = timestamp,
							 charge = battery) %>%
				select(id, 
							 charge,
							 timestamp)
			print(data %>% head)
			filename <- here(output_path, timestamp)
			fileending <- ".feather"
			path <- paste0(filename, fileending)
			write_feather(x = data, path = path)
			close(con)
		})
	})
}






