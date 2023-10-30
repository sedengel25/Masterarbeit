################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 05_add_charge_data.R
#############################################################################
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
		mutate(timestamp_start = paste0(
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
		mutate(year = year(dest_time),
					 month = month(dest_time),
					 day = day(dest_time),
					 hour = hour(dest_time),
					 min = minute(dest_time),
					 sec = second(dest_time)) %>%
		mutate(timestamp_dest = paste0(
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
		# filter(hour(start_time) != 0) %>% # Rohdaten zur Stunde 0
		select(-all_of(
			c("year","month","day","hour","min","sec")
		))
	
	return(dt)
}


# Documentation: add_charge_col_bolt
# Usage: add_charge_col_bolt(dt, input_path, output_path)
# Description: Adds charge-col of BOLT raw data
# Args/Options: dt, input_path, output_path
# Returns: dt
# Output: ...
add_charge_col_bolt <- function(dt, input_path, output_path){
	
	pb <- txtProgressBar(min = 0, max = nrow(dt), style = 3)
	
	for(i in 1:nrow(dt)){
		setTxtProgressBar(pb, i)
		
		id_i <- dt[i, "id"] %>% as.integer
		# print(id_i)
		
		### Start charge -------------------------------------------------
		timestamp_start <- dt[i, "timestamp_start"]
		filename_start <- here(input_path, timestamp_start)
		fileending_start <- ".feather"
		path_feather_start <- paste0(filename_start, fileending_start)
		feather_idx_start <- match(path_feather_start, list_feather_files) - 1
		if(is.na(feather_idx_start)){
			next
		}
		
		
		data_start <- feather_data[[feather_idx_start]] 
		# print("start")
		charge_start <- data_start %>%
			filter(id == id_i) %>%
			select(charge) %>%
			pull() %>%
			as.integer()
		# print(charge_start)
		if(length(charge_start)==0){
			next
		}
		dt[i, "charge_start"] <- charge_start
		
		### Dest charge -------------------------------------------------
		dest_hour <- hour(dt[i, "dest_time"] %>% pull)
		ride_i <- dt[i, "ride"] %>% as.integer
		
		if(length(dest_hour)==0){
			next
		}

		if(dest_hour==0){
			while(dest_hour==0){
				
				ride_i <- (ride_i + 1) %>% as.integer 

				timestamp_dest <- dt %>%
					filter(id == id_i,
								 ride == ride_i) %>%
					select(timestamp_dest) %>%
					pull
				
				dest_hour <- dt %>%
					filter(id == id_i,
								 ride == ride_i) %>%
					select(dest_time) %>%
					pull %>%
					hour
				
				if(length(dest_hour)==0){
					break
				}
				}
		} else {
			timestamp_dest <- dt[i, "timestamp_dest"]
		}


		filename_dest <- here(input_path, timestamp_dest)
		fileending_dest <- ".feather"
		path_feather_dest <- paste0(filename_dest, fileending_dest)

		feather_idx_dest <- match(path_feather_dest, list_feather_files)
		if(is.na(feather_idx_dest)){
			next
		}
		
		data_dest <- feather_data[[feather_idx_dest]] 
		

		charge_dest <- data_dest %>%
			filter(id == id_i) %>%
			select(charge) %>%
			pull() %>%
			as.integer()
		# print(charge_dest)
		if(length(charge_dest)==0){
			next
		}
		dt[i, "charge_dest"] <- charge_dest


	}
	write_rds(dt, file = output_path)
}




# Documentation: add_charge_col_tier
# Usage: add_charge_col_tier(dt, input_path, output_path)
# Description: Adds charge-col of TIER raw data
# Args/Options: dt, input_path, output_path
# Returns: dt
# Output: ...
add_charge_col_tier <- function(dt, input_path, output_path){
	
	pb <- txtProgressBar(min = 0, max = nrow(dt), style = 3)
	
	for(i in 1:nrow(dt)){
		setTxtProgressBar(pb, i)
		
		id_i <- dt[i, "id"] %>% as.character
		timestamp <- dt[i, "timestamp"]
		filename <- here(input_path, timestamp)
		fileending <- ".feather"
		path_feather <- paste0(filename, fileending)
		feather_idx <- match(path_feather, list_feather_files) - 1
		if(is.na(feather_idx)){
			next
		}
		data <- feather_data[[feather_idx]] 
		data <- data %>% 
			distinct(id, .keep_all = TRUE) 
		print(id_i)
		print(feather_idx)
		# print(data)
		charge <- data %>%
			filter(id == id_i) %>%
			select(batteryLevel) %>%
			# pull() %>%
			as.integer()
		dist_on_charge <- data %>%
			filter(id == id_i) %>%
			select(currentRangeMeters) %>%
			pull() %>%
			as.integer()
		
		if(length(charge)==0){
			next
		}
		
		if(length(dist_on_charge)==0){
			next
		}
		
		dt[i, "charge"] <- charge
		dt[i, "dist_on_charge"] <- dist_on_charge

	}
	
	write_rds(dt, file = output_path)
}
