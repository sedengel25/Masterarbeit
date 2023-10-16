################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file reads in the csv-files, transforms them to rds and adds an id
################################################################################
source("./src/00_config.R")
source("./src/01_utils.R")

list_of_files <- list.files(path_raw_data)


transform_to_rds(list = list_of_files, 
								 input_path = path_raw_data,
								 output_path = path_processed_data_1)



