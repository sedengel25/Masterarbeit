################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file explores the data for the first time to get an idea of the quality
################################################################################
source("./src/00_config.R")
dt <- read_rds(dt_path)

# Inspect number of trips over time
posixct_min_start_time <- min(dt$start_time)
posixct_max_start_time <- max(dt$start_time)

dt <- dt %>%
	mutate(date = as.Date(start_time))


dt_trips_per_day <- dt %>%
											group_by(date) %>%
											summarise(n = n()) %>%
											as.data.table()


ggplot(data = dt_trips_per_day)+
	geom_line(aes(x = date, y = n))


dt_dates <- seq(from = as.Date(posixct_min_start_time), 
		to = as.Date(posixct_max_start_time), by = "1 day") %>%
	as.data.table() %>%
	rename("date" = ".")
	

left_join(dt_trips_per_day, dt_dates, by = "date")
head(dt)

dt_temp = dt%>%
	filter(duration < 30)

ggplot(dt_temp)+
	geom_density(aes(x=duration))
