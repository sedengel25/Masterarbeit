################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file creates processed and edited rds-files based on the raw data
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/04_utils.R")




create_feather_files_voi(input_path = path_raw_voi_cologne_06_05,
										 output_path = "./")

