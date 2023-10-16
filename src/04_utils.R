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