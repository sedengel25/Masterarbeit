################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 03_read_data.R
################################################################################

# Documentation: combine_files_to_dt
# Usage: combine_files_to_dt(list)
# Description: comines list of filenames into 1 datatable
# Args/Options: list
# Returns: datatable
# Output: ...
combine_files_to_dt <- function(list, path){
	
	list_of_dt <- lapply(list, function(x){
		
		provider <- strsplit(x, split = "_")[[1]][1]
		city <- strsplit(x, split = "_")[[1]][2]
		dt <- paste0(path,x) %>% read_rds()
		dt %>% 
			mutate(id = as.character(id), 
						 provider = provider,
						 city = city)
	})
	
	list_of_non_empty_dt <- list_of_dt[sapply(list_of_dt, nrow) > 0]
	combined_dts <- dplyr::bind_rows(list_of_non_empty_dt) 
	return(combined_dts)
	
}





choose_datasets <- function(dt, days, provider, city){
	
	valid_days <- c(0:14)
	valid_provider <- c("VOI", "BOLT", "TIER")
	valid_city <- c("BERLIN", "HAMBURG", "MUNICH", "COLOGNE")
	
	if (!(days %in% valid_days)) {
		stop("Invalid choice. Please choose a number betwenn 0 and 14.")
	}
	
	if (!(provider %in% valid_provider)) {
		stop("Invalid choice. Please choose between TIER, BOLT and VOI (char) ")
	}
	
	if (!(city %in% valid_city)) {
		stop("Invalid choice. Please choose between 
				 BERLIN, HAMBURG, MUNICH, COLOGNE (char)")
	}
	
	ids <- which(dt$days==days &
							 	dt$provider==provider &
							 	dt$city==city)
	
	
	list_of_ids <- dt[ids,"id"] %>% as.list()
	list_of_ids <- list_of_ids$id
	
	return(list_of_ids)
}