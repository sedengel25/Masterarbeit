################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file explores the data for the first time to get an idea of the quality
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/05_utils.R")

dt <- read_rds(path_dt)
summary(dt)


################################################################################
# Testing
################################################################################
dt <- dt %>%
	mutate(year = year(start_time),
				 month = month(start_time),
				 day = day(start_time),
				 hour = hour(start_time),
				 min = minute(start_time),
				 sec = second(start_time)) %>%
	mutate(scooter_id = id,
				 id = paste0(id,
				 						"_",
				 						year,
				 						"_",
				 						month,
				 						"_",
				 						day,
				 						"_",
				 						hour,
				 						"_",
				 						min,
				 						"_",
				 						sec)) %>%
	select(-all_of(
		c("year","month","day","hour","min","sec")
	))



list.dirs(path_processed_data_4)

################################################################################
# Trying to detect maintenance- /charging- / relocation-trips
################################################################################

dt <- dt %>%
	filter(duration <= 1440, 
				 distance <= 5000,
				 duration >100)



dt <- dt %>%
	mutate(weekday = wday(start_time, week_start = 1),
				 start_hour = hour(start_time),
				 dest_hour = hour(dest_time),
				 date = as.Date(start_time)) %>%
	select(c("start_hour", "duration", "dest_hour"))

dt_norm <- normalize_dt(dt = dt)


# GMM --------------------------------------
gmm <- Mclust(data = dt_norm[,c(1,2)], G = 4)
dt_c <- dt %>%
	mutate(cluster = gmm$classification)



plot <- create_3d_cluster_plot(dt = dt_c)
plot

dt_test <- dt_c %>%
	filter(cluster %in% c(2,3,4)) 

dt_test$dest_hour %>% hist()



################################################################################
# Descriptive Analysis
################################################################################
dt_descr_anal <- dt %>%
	mutate(weekday = wday(start_time, week_start = 1),
				 start_hour = hour(start_time),
				 date = as.Date(start_time)) %>%
	select(c("date", "weekday", "start_hour", "duration", "distance"))



dt_trips_per_h <- trips_per_h(dt = dt_descr_anal)

dt_trips_per_wd <- trips_per_wd(dt = dt_descr_anal)

dt_trips_per_date <- trips_per_date(dt = dt_descr_anal)

dt_trips_per_date_wd <- trips_per_date_wd(dt = dt_descr_anal)

ggplot_line_starthour <- create_start_hour_plot(dt = dt_trips_per_h)

ggplot_line_weekday <- create_weekday_plot(dt = dt_trips_per_wd)

ggplot_line_date <- create_date_plot(dt = dt_trips_per_date)

ggplot_line_date_wd <- create_date_and_wd_plot(dt = dt_trips_per_date_wd)




#############################################################################
# Ablage
################################################################################
# dt_dbscan_pca <- prcomp(dt_dbscan)
# num_lambda <- dt_dbscan_pca$sdev^2
# cumsum(num_lambda)/sum(num_lambda)
# 
# 
# dt_dbscan_pca <- dt_dbscan_pca$x %>%
# 	as.data.table %>%
# 	select("PC1","PC2")
# int_min_pts <- 4


# dt_dates <- seq(from = as.Date(posixct_min_start_time), 
# 		to = as.Date(posixct_max_start_time), by = "1 day") %>%
# 	as.data.table() %>%
# 	rename("date" = ".")
# 	
# 
# left_join(dt_trips_per_day, dt_dates, by = "date")
# head(dt)
# 
# dt_temp = dt%>%
# 	filter(duration < 30)
# 
# ggplot(dt_temp)+
# 	geom_density(aes(x=duration))

 
# sf_points <- st_as_sf(dt_most_trips,
# 											coords = c("start_loc_lon", "start_loc_lat"),
# 											crs = 4326)
# 
# shp_berlin <- st_read(path_shp_file_berlin)
# 
# sf_points_to_district <- st_join(sf_points, shp_berlin)
# 
# dt_trips_assigned_to_district <- as.data.table(sf_points_to_district) %>%
# 	select(Gemeinde_s, distance, duration) %>%
# 	mutate(Gemeinde_s = int_vec <- as.integer(sub("^0+", "", Gemeinde_s)))

