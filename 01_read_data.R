################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file reads in the data and saves the combined data
################################################################################
source("./src/00_config.R")
source("./src/01_utils.R")


################################################################################
# Read all csv-files of e-scooter mobility data and combine it into a datatable
################################################################################
list_of_files <- list.files(raw_data_path)


dt <- combine_files_to_dt(list = list_of_files, path = raw_data_path) 

################################################################################
# Save data
################################################################################
write_rds(x = dt, file = dt_path)
