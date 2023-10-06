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
		dt <- paste0(path,x) %>% fread()
		dt %>% 
			mutate(id = as.character(id), 
						 provider = provider,
						 city = city)
	})
	
	list_of_non_empty_dt <- list_of_dt[sapply(list_of_dt, nrow) > 0]
	combined_dts <- dplyr::bind_rows(list_of_non_empty_dt) 
	return(combined_dts)
	
}

