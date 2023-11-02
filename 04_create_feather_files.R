################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file creates processed and edited rds-files based on the raw data
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/04_utils.R")




create_feather_files_tier(input_path = path_raw_tier_munich_09_13,
										 output_path = "./")

