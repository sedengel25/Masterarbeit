################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file explores the data for the first time to get an idea of the quality
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/06_utils.R")

dt <- read_rds(path_dt_charge_voi_berlin_06_05)


min_date <- min(dt$start_time)



###############################################################################
# Trying to detect maintenance- /charging- / relocation-trips
################################################################################
dt <- dt %>%
	mutate(
				 weekday = wday(start_time, week_start = 1),
				 start_hour = hour(start_time),
				 dest_hour = hour(dest_time),
				 day = as.integer(difftime(start_time, min_date, units = "days")) + 1)%>%
	filter(charge_start > -1,
				 charge_dest > -1) 
# Trips started between 00:00 and 00:01 don't have charge-information

# Add 'last_trip'
# dt <- dt %>%
# 	arrange(id, ride) %>%
# 	mutate(last_trip = if_else(id != lag(id),
# 														 TRUE,
# 														 FALSE))



# Move that column by 1
# dt <- dt %>%
# 	mutate(last_trip = lead(last_trip)) %>%
# 	mutate(last_trip = if_else(is.na(last_trip),
# 														 TRUE,
# 														 last_trip))



# Add 'dist_on_charge'
# dt <- dt %>% 
# 	mutate(dist_on_charge = (charge/100)*34000) 



# Flag trips where scooter has higher charge than same scooter trip before
dt <- dt %>%
	mutate(charge_increase = ifelse(charge_dest > charge_start,
																	TRUE,
																	FALSE)
	) 

dt %>%
	filter(charge_increase==TRUE) %>%
	nrow
# Move that column by 1
# dt <- dt %>%
# 	mutate(charge_increase = lead(charge_increase)) %>%
# 	mutate(charge_increase = ifelse(is.na(charge_increase),
# 																	FALSE,
# 																	charge_increase))

# Add 'dist_greater_charge'
# dt <- dt %>%
# 	mutate(dist_greater_charge = if_else(distance > dist_on_charge,
# 																			 TRUE,
# 																			 FALSE)) 

# Add 'charge_loss'
dt <- dt %>%
	# mutate(charge_after = if_else(id != lead(id),
	# 															NA,
	# 															lead(charge)
	# 															)
	# ) %>%
	mutate(charge_loss = charge_start - charge_dest)


# Venn diagram
# dt_venn <- dt %>%
# 	select(dist_greater_charge
# 				 , charge_increase
# 				 , last_trip
# 				 # , critical_charge
# 				 # , long_duration
# 				 )
# 
# ggvenn(dt_venn, colnames(dt_venn))


# ggplot_charge_vs_noncharge <-
# 	create_plot_compare_charge_noncharge(dt_1 = dt %>%
# 																			 	filter(charge_increase == TRUE),
# 																			 dt_2 = dt %>%
# 																			 	filter(charge_increase == FALSE))

# ggplot_charge_vs_noncharge %>% grid.draw()


###############################################################################
# GMM Start
################################################################################
dt <- dt %>%
	filter(charge_increase == FALSE)


# Turns all categorical vars into integer
list_objects_gmm <- prepare_gmm(dt = dt)
dt <- list_objects_gmm$dt
factor_levels <- list_objects_gmm$factor_levels

# Normalize only numerical vars 
int_idx_continous_vars <- which(sapply(dt, is.integer)==FALSE) %>% as.numeric()
dt_norm <- normalize_dt(dt = dt[,..int_idx_continous_vars])


# Select columns for Clustering
char_cols <- c("duration", "charge_loss")
dt_gmm <- create_gmm_dt(char_cols = char_cols, dt_norm = dt_norm, dt = dt)

# dt_vars_class <- dt_gmm %>% summary.default %>% as.data.frame %>% 
# 	dplyr::group_by(Var1) %>%  tidyr::spread(key = Var2, value = Freq) %>%
# 	as.data.table
# set.seed(123)
gmm <- Mclust(data = dt_gmm)

dt_c <- dt[,..char_cols] %>%
	mutate(cluster = gmm$classification)

# dt_c <- dt_c %>%
# 	mutate(distance = distance + 0.01)

# Here, we're adding another condition to differentiate between binary and non-binary categorical variables.
tibble_dt_c <- dt_c %>% 
	pivot_longer(cols = -cluster, names_to = "variable", values_to = "value") %>%
	as.data.table






list_max_val <- lapply(dt[,..char_cols], max)
list_min_val <- lapply(dt[,..char_cols], min)
# list_min_val$distance <- list_min_val$distance + 0.01
list_max_val$charge_loss <- quantile(dt_c$charge_loss, 0.99) 


create_mixed_grid_vars_over_cluster(dt_pivot_longer = tibble_dt_c)





dt_c %>%
	filter(cluster == 1)%>% 
	summary







# dt_c_4 <- dt_c %>%
# 	filter(cluster == 4) %>%
# 	select(-all_of("cluster"))
# 
# dt_c_4_norm <- normalize_dt(dt = dt_c_4)
# char_cols <- c("duration", "charge_loss", "time_of_day", "distance")
# dt_gmm_c_4 <- create_gmm_dt(char_cols = char_cols,
# 															dt_norm = dt_c_4_norm,
# 															dt = dt_c_4)
# 
# gmm_c_4 <-  Mclust(data = dt_gmm_c_4)
# dt_c_4_c <- dt_c_4 %>%
# 	mutate(cluster = gmm_c_4$classification)
# # dt_c_4_c <- dt_c_4_c %>%
# # 	mutate(distance = distance + 0.01)
# 
# tibble_dt_c_4 <- dt_c_4_c %>%
# 	pivot_longer(cols = -cluster, names_to = "variable", values_to = "value") %>%
# 	as.data.table
# 
# list_max_val <- lapply(dt[,..char_cols], max)
# list_min_val <- lapply(dt[,..char_cols], min)
# list_min_val$distance <- list_min_val$distance + 0.01
# list_max_val$charge_loss <- quantile(dt_c_4_c$charge_loss, 0.99)
# create_mixed_grid_vars_over_cluster(dt_pivot_longer = tibble_dt_c_4)
# 
# 
# dt_c_4_c <- dt_c_4_c %>% 
# 	mutate(charge_per_hour = (charge_loss/duration)*60)
# 
# create_summary_for_clusters(dt_c_4_c)
# 
# dt_c <- dt_c %>% 
# 	mutate(charge_per_hour = (charge_loss/duration)*60)
# create_summary_for_clusters(dt_c)
###############################################################################
# Spatial
################################################################################
# sf_lines <- create_sf_start_dest_lines(dt = dt_reloc)
# 
# leaflet_map <- create_berlin_leaflet_map()
# 
# leaflet_trips <- leaflet_map %>% addPolylines(data = sf_lines)
# leaflet_trips



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






