################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 01_read_data.R
################################################################################

# Documentation: combine_files_to_dt
# Usage: combine_files_to_dt(list)
# Description: comines list of filenames into 1 datatable
# Args/Options: list
# Returns: datatable
# Output: ...
combine_files_to_dt <- function(list, path){
	list_of_dt <- lapply(list, function(x){
		dt <- paste0(path,x) %>% fread()
		dt %>% 
			mutate(id = as.character(id))
	})
	
	list_of_non_empty_dt <- list_of_dt[sapply(list_of_dt, nrow) > 0]
	combined_dts <- dplyr::bind_rows(list_of_non_empty_dt) 
	return(combined_dts)
}

