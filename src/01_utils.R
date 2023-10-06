################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 01_rename_transform.R
################################################################################
# Documentation: transform_to_rds
# Usage: transform_to_rds(list, input_path, output_path)
# Description: Reads all files, transforms them to rds and give them an id
# Args/Options: list, input_path, output_path
# Returns: ...
# Output: writes rds files

transform_to_rds <- function(list, input_path, output_path) {
	
  list_of_dt <- lapply(seq_along(list), function(idx){
  	
  	x <- list[[idx]]
  	provider <- strsplit(x, split = "_")[[1]][1]
  	city <- strsplit(x, split = "_")[[1]][2]
  	dt <- paste0(input_path,x) %>% fread()
  	dt <- dt %>% 
  		mutate(scooter_id = as.character(id), 
  					 provider = provider,
  					 city = city)
  	file <- paste0(output_path, provider, "_", city, "_",idx, ".rds")
  	write_rds(dt, file)
  })
}
