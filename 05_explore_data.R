################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file explores the data for the first time to get an idea of the quality
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/05_utils.R")

dt <- read_rds(path_dt_charge)

min_date <- min(dt$start_time)

###############################################################################
# Trying to detect maintenance- /charging- / relocation-trips
################################################################################
dt <- dt %>%
	mutate(weekday = wday(start_time, week_start = 1),
				 start_hour = hour(start_time),
				 dest_hour = hour(dest_time),
				 date = as.Date(start_time),
				 day = as.integer(difftime(start_time, min_date, units = "days")) + 1) %>%
	select(id, ride, day, start_hour, dest_hour, duration, distance, charge) %>%
	filter(charge>-1
				 # , duration <= 1440
				 # , distance <= 10000
				 )

# Add 'dist_on_charge'
dt <- dt %>% 
	mutate(dist_on_charge = (charge/100)*34000) 


# Flag trips where scooter has higher charge than same scooter trip before
dt <- dt %>%
	arrange(id, ride) %>%
	mutate(charge_increase = ifelse(id != lag(id),
																	NA,
																	charge > lag(charge)
	)) 


# Move that column by 1
dt <- dt %>%
	mutate(charge_increase = lead(charge_increase))

# Flag NAs as non-charging trips (FALSE)
dt <- dt %>%
	mutate(charge_increase = ifelse(is.na(charge_increase),
																	FALSE,
																	charge_increase))

# Flag trips where the trip-distance is higher than the distance on charge
dt <- dt %>%
	mutate(charge_increase = if_else(distance > dist_on_charge, 
																	 TRUE,
																	 charge_increase
																	 	))



# GMM --------------------------------------
dt_gmm <- dt %>%
	filter(charge_increase!=TRUE) %>%
	select(day, start_hour, dest_hour, duration, distance, charge) 


dt_gmm_norm <- normalize_dt(dt = dt_gmm)


gmm <- Mclust(data = dt_gmm_norm)

dt_c <- dt_gmm %>%
	mutate(cluster = gmm$classification)


plot <- create_3d_cluster_plot(dt = dt_c)
plot
create_summary_for_clusters(dt = dt_c)
create_hist_grid(dt = dt_c)

test_gmm <- dt_c %>%
	filter(cluster==2) %>%
	select(-all_of("cluster"))



create_summary_for_clusters(dt = test_c)
create_hist_grid(dt = test_c)

test_c <- test_c %>%
	mutate(cluster = as.integer(cluster))

test_gmm_2 <- test_c %>%
	filter(cluster!=4)

test_2_c <- create_gmm_clustered_dt(dt = test_gmm_2)
create_hist_grid(dt = test_2_c)
create_summary_for_clusters(dt = test_2_c)

test_gmm_3 <-  test_2_c %>%
	filter(cluster!=5)
test_3_c <- create_gmm_clustered_dt(dt = test_gmm_3)
create_hist_grid(dt = test_3_c)
create_summary_for_clusters(dt = test_3_c)

test_3_c %>%
	filter(cluster==6)

dt %>%
	filter(distance==15525.5000)

dt %>%
	filter(id==577869) %>%
	arrange(ride)


# Long distances?
dt %>%
	filter(distance==15525.5000)

dt %>%
	arrange(desc(distance)) %>%
	head(100)


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

