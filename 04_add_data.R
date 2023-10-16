################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file creates processed and edited rds-files based on the raw data
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/04_utils.R")


# Reads processed data
dt <- read_rds(dt_path)


# Lists all files of the raw data containing battery information
dirs <- get_directories(path = bolt_berlin_data_path)[-1]


dt_battery <- data.table(
	id = character(0),
	lat = numeric(0),
	lng = numeric(0),
	charge = integer(0),
	distance_on_charge = integer(0),
	scooter_id = integer(0)
)

lapply(dirs[-1], function(x){
	
	files <- list.files(x, full.names = TRUE)
	print(x)
		
		lapply(files, function(y){
			
		file_js <- sub(".gz$", "", y)
		file_gz <- y
		unzip_gz(file_js = file_js, file_gz = file_gz)
		dt_js <- fromJSON(file_js)
		dt_js <- select_scooter_trips(dt = dt_js)
		timestamp <- strsplit(y,split="\\.")[[1]][1]
		timestamp <- strsplit(timestamp,split="/")[[1]][9]
		dt_js <- add_scooter_time_id(dt = dt_js, timestamp = timestamp)
		dt_js <- dt_js %>%
			select(id, lat, lng, charge, distance_on_charge, scooter_id)
		filename <- paste0(processed_data_4_path, timestamp)
		write_rds(dt_js, filename)
		
	})
})
