options(scipen = 999)
library(dplyr)



# Load data --------------------------------------
files <- list.files("./data/")

library(data.table)

#create empty data.table
dt_plot <- data.table(
	date = as.Date(character(0)),
	provider = character(0)
)

dt_table <- data.table(
	provider = character(0),
	min_starttime = as.POSIXct(character(0)),
	max_starttime = as.POSIXct(character(0)),
	days = numeric(0)
)


#fill data.table with the dates, where the crawler ran successfully
for(file in files){
	
	data <- paste0("./data/",file) %>% fread()
	
	min_date <- min(data$start_time) %>% as.Date
	max_date <- max(data$start_time) %>% as.Date
	
	min_starttime <- min(data$start_time) %>% as.POSIXct
	max_starttime <- max(data$start_time) %>% as.POSIXct
	
	if(is.infinite(min_date)){
		next
	}
	
	timespan <- seq(from = min_date, to = max_date, by = "1 day") 
	provider <- strsplit(file, split = "_")[[1]][1]
	
	
	dt_plot <- rbind (dt_plot, data.table(
		date = timespan,
		provider = provider
	))
	
	days <- difftime(max_starttime, min_starttime, units = "days") %>% as.numeric %>% round

	
	dt_table <- rbind (dt_table, data.table(
		provider = provider,
		min_starttime = min_starttime,
		max_starttime = max_starttime,
		days = days
	))
	
}



# Plot data --------------------------------------
library(ggplot2)

overview_data <- ggplot(dt_plot, aes(x = date, y = provider)) +
	geom_point() +
	scale_x_date(limits = c(min(dt_plot$date), max(dt_plot$date)),
							 date_labels = "%d-%m-%Y") +
	scale_y_discrete(limits = c("VOI", "BOLT", "TIER")) +
	labs(x = "date", y = "provider") 

ggsave("./figs/overview_data.png", plot = overview_data, 
			 width = 30, height = 20, units = "cm")


# Create xlsx --------------------------------------
library(writexl)
write_xlsx(x = dt_table, path = "./output/overview_data.xlsx")


