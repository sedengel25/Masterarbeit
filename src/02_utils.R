################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 02_get_overview_data.R
################################################################################

# Documentation: create_overview
# Usage: create_overview(list, dt_table, dt_plot, input_path)
# Description: Reads all files and creates an overview 
# Args/Options: list, dt_table, dt_plot, input_path
# Returns: datatable
# Output: ...
create_overview <- function(list, dt_table, dt_plot, input_path) {

  for(file in list){
  	data <- paste0(input_path, file) %>% read_rds()
  	
  	min_date <- min(data$start_time) %>% as.Date
  	max_date <- max(data$start_time) %>% as.Date
  	
  	min_starttime <- min(data$start_time) %>% as.POSIXct
  	max_starttime <- max(data$start_time) %>% as.POSIXct
  	
  	if(is.infinite(min_date)){
  		next
  	}
  	
  	timespan <- seq(from = min_date, to = max_date, by = "1 day") 
  	
  	provider <- strsplit(file, split = "_")[[1]][1]
  	city <- strsplit(file, split = "_")[[1]][2]
  	id_temp <- strsplit(file, split = "_")[[1]][3]
  	id <- strsplit(id_temp, split = "\\.")[[1]][1] %>% as.integer()


  	dt_plot <- bind_rows(dt_plot, data.table(
  		date = timespan,
  		provider = provider
  	))
  	
  	days <- difftime(max_starttime, min_starttime, units = "days") %>% as.numeric %>% round %>% as.integer
  	
  	
  	dt_table <- bind_rows(dt_table, data.table(
  		provider = provider,
  		city = city,
  		id = id,
  		min_starttime = min_starttime,
  		max_starttime = max_starttime,
  		days = days
  	))

  }
	

	list_of_dts <- list(dt_plot = dt_plot, dt_table = dt_table)
	return(list_of_dts)
}


