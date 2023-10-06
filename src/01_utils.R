################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 01_get_overview_data.R
################################################################################

# Documentation: create_overview
# Usage: create_overview(list, dt_table, dt_plot)
# Description: Reads all files and creates an overview 
# Args/Options: list, dt_table, dt_plot
# Returns: datatable
# Output: ...
create_overview <- function(list, dt_table, dt_plot) {

  for(file in list){
  	data <- paste0(raw_data_path,file) %>% fread()
  	
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
  	
  	dt_plot <- bind_rows(dt_plot, data.table(
  		date = timespan,
  		provider = provider
  	))
  	
  	days <- difftime(max_starttime, min_starttime, units = "days") %>% as.numeric %>% round
  	
  	
  	dt_table <- bind_rows(dt_table, data.table(
  		provider = provider,
  		city = city,
  		min_starttime = min_starttime,
  		max_starttime = max_starttime,
  		days = days
  	))

  }
	
	id <- seq(from = 1, to = nrow(dt_table))
	dt_table <- dt_table %>%
								mutate(id = id)
	list_of_dts <- list(dt_plot = dt_plot, dt_table = dt_table)
	return(list_of_dts)
}


