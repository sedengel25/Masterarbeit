################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file creates an overview of the collected data
################################################################################
source("./src/00_config.R")
source("./src/02_utils.R")

list_of_files <- list.files(path_processed_data_1)

# Create empty data.table
dt_plot <- data.table(
	date = as.Date(character(0)),
	provider = character(0)
)

dt_table <- data.table(
	provider = character(0),
	city = character(0),
	min_starttime = as.POSIXct(character(0)),
	max_starttime = as.POSIXct(character(0)),
	days = numeric(0)
)


list_of_dt <- create_overview(list = list_of_files, 
													   dt_plot = dt_plot, 
													   dt_table = dt_table,
														 input_path = path_processed_data_1)

dt_plot <- list_of_dt$dt_plot

dt_table <- list_of_dt$dt_table

# Plot data --------------------------------------


# overview_data <- ggplot(dt_plot, aes(x = date, y = provider)) +
# 	geom_point() +
# 	scale_x_date(limits = c(min(dt_plot$date), max(dt_plot$date)),
# 							 date_labels = "%d-%m-%Y") +
# 	scale_y_discrete(limits = c("VOI", "BOLT", "TIER")) +
# 	labs(x = "date", y = "provider") 
# 
# ggsave(file_rds_dt_plot, plot = overview_data, 
# 			 width = 30, height = 20, units = "cm")


# Save data --------------------------------------
write_rds(x = dt_table, file = file_rds_dt_table)






