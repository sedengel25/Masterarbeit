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
				 day = as.integer(difftime(start_time, min_date, units = "days")) + 1)%>%
	filter(charge>-1)

# Add 'last_trip'
dt <- dt %>%
	arrange(id, ride) %>%
	mutate(last_trip = if_else(id != lag(id),
														 TRUE,
														 FALSE))



# Move that column by 1
dt <- dt %>%
	mutate(last_trip = lead(last_trip)) %>%
	mutate(last_trip = if_else(is.na(last_trip),
														 TRUE,
														 last_trip))

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
	mutate(charge_increase = lead(charge_increase)) %>%
	mutate(charge_increase = ifelse(is.na(charge_increase),
																	FALSE,
																	charge_increase))

###############################################################################
# Venn diagram
################################################################################
dt <- dt %>%
	mutate(dist_greater_charge = if_else(distance > dist_on_charge,
																			 TRUE,
																			 FALSE)) 


dt_venn <- dt %>%
	select(dist_greater_charge, charge_increase, last_trip)

ggvenn(dt_venn, colnames(dt_venn))



dt_categories <- dt %>%
	mutate(
		class = case_when(
			charge_increase == FALSE &
				dist_greater_charge == FALSE &
				last_trip == FALSE ~ "normal_trip",
			charge_increase == FALSE &
				dist_greater_charge == FALSE & last_trip == TRUE ~ "last_trip",
			charge_increase == FALSE &
				dist_greater_charge == TRUE &
				last_trip == FALSE ~ "distgreatercharge",
			charge_increase == FALSE &
				dist_greater_charge == TRUE &
				last_trip == TRUE ~ "distgreatercharge_lasttrip",
			charge_increase == TRUE &
				dist_greater_charge == FALSE &
				last_trip == FALSE ~ "chargeincrease",
			charge_increase == TRUE &
				dist_greater_charge == FALSE &
				last_trip == TRUE ~ "chargeincrease_lasttrip",
			charge_increase == TRUE &
				dist_greater_charge == TRUE &
				last_trip == FALSE ~ "chargeincrease_distgreatercharge",
			charge_increase == TRUE &
				dist_greater_charge == TRUE &
				last_trip == TRUE ~ "chargeincrease_lasttrip_distgreatercharge",
			TRUE ~ "Unknown"
			
		)
	)



dt_normal_trips <- dt_categories %>%
	filter(class=="normal_trip") %>%
	select(start_hour, distance, duration, day, charge)

set.seed(123)
dt_norm <- normalize_dt(dt = dt_normal_trips)
gmm <- Mclust(data = dt_norm)
dt_normal_trips_c <- dt_normal_trips %>%
	mutate(cluster = gmm$classification)

create_hist_grid_cluster(dt = dt_normal_trips_c)
create_summary_for_clusters(dt = dt_normal_trips_c)

dt_normal_trips_c_9 <- dt_normal_trips_c %>%
	filter(cluster==9)

dt_normal_trips_c_9
# Create Binary-variable for high charge loss and no charge loss
# Add do venn diagram


dt_reloc <- dt %>%
	filter(charge_increase==FALSE) %>%
	filter(last_trip==FALSE) %>%
	filter(distance > dist_on_charge) 

dt_reloc <- get_subsequent_trips(dt_subset = dt_reloc,
															 dt_compare = dt)



dt_reloc <- dt_reloc %>%
	mutate(charge_lead = lead(charge)) %>%
	mutate(charge_loss = charge - charge_lead) %>%
	filter(is.na(lead_ride)) %>%
	filter(last_trip==FALSE) %>%
	select(id, ride, start_loc_lat, start_loc_lon, dest_loc_lat, dest_loc_lon, 
				 start_time, distance, dist_on_charge, duration, charge, charge_lead, charge_loss)

dt_reloc 

###############################################################################
# Spatial
################################################################################
sf_lines <- create_sf_start_dest_lines(dt = dt_reloc)

leaflet_map <- create_berlin_leaflet_map()

leaflet_trips <- leaflet_map %>% addPolylines(data = sf_lines)
leaflet_trips


# GMM --------------------------------------
dt_gmm <- dt %>%
	filter(charge_increase!=TRUE) %>%
	select(day, start_hour, dest_hour, duration, distance, charge, last_trip) 

dt_gmm_c <- dt %>%
	filter(charge_increase!=TRUE) %>%
	select(day, start_hour, dest_hour, duration, distance, charge) 




set.seed(123)
dt_c <- create_gmm_clustered_dt(dt_cluster = dt_gmm_c, dt = dt_gmm)
 

plot <- create_3d_cluster_plot(dt = dt_c)
plot
create_summary_for_clusters(dt = dt_c)
create_hist_grid(dt = dt_c)

# Cluster 1: 173550 trips with classic scooter trip duration and distance
# Cluster 2: 10781 trips with long duration and a bit longer distance

dt_gmm_2_c <- dt_c %>%
	filter(cluster==2) %>%
	select(-all_of(c("cluster", "last_trip")))

dt_gmm_2 <- dt_c %>%
	filter(cluster==2) %>%
	select(-all_of(c("cluster")))

set.seed(123)
dt_c_2 <- create_gmm_clustered_dt(dt_cluster = dt_gmm_2_c, dt = dt_gmm_2)
table(dt_c_2$last_trip)
create_summary_for_clusters(dt = dt_c_2)
create_hist_grid(dt = dt_c_2)
# Cluster 1: 1822 trips, very long dur, dest_hour 00:00, partly low charge
# Cluster 2: 1067 trips, high charge
# Cluster 3: 748 trips with long duration and partly low charge
# Cluster 4: 
# Cluster 5 & 6: 3431 + 1306 trips, actual 5km-trips



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

