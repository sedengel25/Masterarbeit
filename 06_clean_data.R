################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file explores the data for the first time to get an idea of the quality
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/06_utils.R")

dt <- read_rds(path_dt_charge_bolt_berlin_06_05)
dt
min_date <- min(dt$start_time)


###############################################################################
# Trying to detect maintenance- /charging- / relocation-trips
################################################################################
dt <- dt %>%
	mutate(
				 weekday = wday(start_time, week_start = 1),
				 start_hour = hour(start_time),
				 dest_hour = hour(dest_time),
				 # date = as.Date(start_time),
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

dt

# Add 'dist_on_charge'
# dt <- dt %>% 
# 	mutate(dist_on_charge = (charge/100)*34000) 



# Flag trips where scooter has higher charge than same scooter trip before
dt <- dt %>%
	arrange(id, ride) %>%
	mutate(charge_increase = ifelse(id != lag(id),
																	NA,
																	charge > lag(charge))
	) 


# Move that column by 1
dt <- dt %>%
	mutate(charge_increase = lead(charge_increase)) %>%
	mutate(charge_increase = ifelse(is.na(charge_increase),
																	FALSE,
																	charge_increase))

# Add 'dist_greater_charge'
# dt <- dt %>%
# 	mutate(dist_greater_charge = if_else(distance > dist_on_charge,
# 																			 TRUE,
# 																			 FALSE)) 

# Add 'charge_loss'
dt <- dt %>%
	mutate(charge_after = if_else(id != lead(id),
																NA,
																lead(charge)
																)
	) %>%
	mutate(charge_loss = charge - charge_after)


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


ggplot_charge_vs_noncharge <-
	create_plot_compare_charge_noncharge(dt_1 = dt %>%
																			 	filter(charge_increase == TRUE),
																			 dt_2 = dt %>%
																			 	filter(charge_increase == FALSE))

ggplot_charge_vs_noncharge %>% grid.draw()


# LAST TRIPS

###############################################################################
# GMM Start
################################################################################
dt <- dt %>%
	filter(last_trip == FALSE) %>%
	filter(charge_increase == FALSE)


# Turns all categorical vars into integer
list_objects_gmm <- prepare_gmm(dt = dt)
dt <- list_objects_gmm$dt
factor_levels <- list_objects_gmm$factor_levels

# Normalize only numerical vars 
int_idx_continous_vars <- which(sapply(dt, is.integer)==FALSE) %>% as.numeric()
dt_norm <- normalize_dt(dt = dt[,..int_idx_continous_vars])


# Select columns for Clustering
char_cols <- c("distance", "duration", "charge_loss")
dt_gmm <- create_gmm_dt(char_cols = char_cols, dt_norm = dt_norm, dt = dt)

dt_vars_class <- dt_gmm %>% summary.default %>% as.data.frame %>% 
	dplyr::group_by(Var1) %>%  tidyr::spread(key = Var2, value = Freq) %>%
	as.data.table
# set.seed(123)
gmm <- Mclust(data = dt_gmm)

dt_c <- dt[,..char_cols] %>%
	mutate(cluster = gmm$classification)


# Here, we're adding another condition to differentiate between binary and non-binary categorical variables.
tibble_dt_c <- dt_c %>% 
	pivot_longer(cols = -cluster, names_to = "variable", values_to = "value") %>%
	as.data.table




# tibble_dt_c <- sample_subset(dt = tibble_dt_c, size = 100000) 

list_max_val <- lapply(dt[,..char_cols], max)
list_min_val <- lapply(dt[,..char_cols], min)
create_mixed_grid_vars_over_cluster(dt_pivot_longer = tibble_dt_c)



###############################################################################
# GMM Ende
################################################################################






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


dt_last_trips <- dt_categories %>%
	filter(class == "last_trip") %>%
	select(
		id,
		ride,
		start_loc_lat,
		start_loc_lon,
		dest_loc_lat,
		dest_loc_lon,
		start_time,
		start_hour,
		distance,
		duration,
		day,
		charge
	)

dt_last_trips_norm <- normalize_dt(dt_last_trips[,-c(1:7)])
gmm <- Mclust(data = dt_last_trips_norm)
dt_last_trips_c <- dt_last_trips[,-c(1:7)] %>%
	mutate(cluster = gmm$classification)
create_hist_grid_cluster(dt_last_trips_c)
create_summary_for_clusters(dt_last_trips_c)

dt_normal_trips <- dt_categories %>%
	filter(class == "normal_trip") %>%
	select(
		id,
		ride,
		start_loc_lat,
		start_loc_lon,
		dest_loc_lat,
		dest_loc_lon,
		start_time,
		start_hour,
		distance,
		duration,
		day,
		charge
	)

# Cluster 'dt_normal_trips"
set.seed(123)

# Remove unexplainable Cluster from data

# Write remaining data to rds-file


###############################################################################
# Spatial
################################################################################
# sf_lines <- create_sf_start_dest_lines(dt = dt_reloc)
# 
# leaflet_map <- create_berlin_leaflet_map()
# 
# leaflet_trips <- leaflet_map %>% addPolylines(data = sf_lines)
# leaflet_trips








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


# # Filter data to only include cases where cluster_1 is TRUE
# dt_test_filtered <- dt_test %>% 
# 	filter(cluster_1 == TRUE)
# 
# # Create a bar plot for 'time_of_day_num'
# bar_plot <- ggplot(data = dt_test_filtered) +
# 	geom_bar(aes(x = as.factor(time_of_day_num)), 
# 					 stat = "count", 
# 					 fill = "blue") +
# 	labs(x = "Time of Day", y = "Count") +
# 	theme_light()
# 
# # Create a density plot for 'charge'
# density_plot <- ggplot(data = dt_test_filtered) +
# 	geom_density(aes(x = charge), 
# 							 fill = "red", 
# 							 alpha = 0.5) +
# 	labs(x = "Charge", y = "Density") +
# 	theme_light()
# 
# # Now we will use 'facet_grid' to specify the layout. The 'facet_grid' function 
# # is used because it allows more complex layouts compared to 'facet_wrap'.
# # We want 'cluster_1' to be on the rows (so it's in the formula's left side), 
# # and the plots themselves to be laid out in columns (side by side).
# 
# # Also, since we only have one level in 'cluster_1', we will use a space ('.') 
# # on the right side of the formula to indicate there's no faceting variable there.
# 
# # Add faceting to the bar plot. This will put the name of the variable at the top.
# bar_plot <- bar_plot + 
# 	facet_grid(cluster_1 ~ ., 
# 						 labeller = as_labeller(c(cluster_1 = "Cluster 1")), 
# 						 switch = "both") # 'switch' moves strip labels to the top
# 
# # Add faceting to the density plot. This will also put the name of the variable at the top.
# density_plot <- density_plot + 
# 	facet_grid(cluster_1 ~ ., 
# 						 labeller = as_labeller(c(cluster_1 = "Cluster 1")), 
# 						 switch = "both") # 'switch' moves strip labels to the top
# 
# # Combine the plots side-by-side using 'patchwork'
# combined_plot <- bar_plot + density_plot
# 
# # Print the final plot
# print(combined_plot)



# # Start building the plot
# ggplot(data = dt_test) +
# 	# Add the bar geometry
# 	geom_bar(
# 		aes(x = time_of_day_num, y = ..count..),  # ensuring we count the occurrences of each 'time_of_day_num'
# 		stat = "count",  # use the count statistics
# 		position = position_dodge()  # dodge positions for bars side-by-side
# 	) +
# 	# Add the density plot
# 	geom_density(
# 		aes(x = charge, y = ..scaled..),  # 'scaled' ensures the density plot fits well in the facet matrix
# 		fill = NA,  # this can be any color or remain transparent
# 		color = "blue"  # for demonstration, set to any preferred color
# 	) +
# 	# Now, we set up the faceting
# 	facet_matrix(
# 		cols = vars(charge, time_of_day_num),  # set columns for facets
# 		rows = vars(cluster_1)  # set rows for facets
# 	)
# 

# Step 2: Adjusting the ggplot calls based on these distinctions

# Since you have different types of categorical variables (binary and others), you might need to customize the aesthetics for each.
# Here, you'll start with the ggplot function, specifying data and aesthetics common to all geoms.

plot_bar <- ggplot(tibble_dt_c) +
	# For binary categorical variables, we use simpler bars.
	geom_bar(
		data = . %>% filter(data_type == "binary"), 
		aes(x = as.factor(value), fill = as.factor(value))
		# position = position_dodge(), 
		# stat = "count"
	) +
	# For non-binary categorical variables, you might want a different approach or aesthetic.
	geom_bar(
		data = . %>% filter(data_type == "categorical"), 
		aes(x = as.factor(value), fill = as.factor(value))
		# position = position_dodge(), 
		# stat = "count"
	) +
	geom_density(
		data = . %>% filter(variable == "charge"),
		aes(x = value)
	) +
	facet_grid(cluster ~ variable, scales = "free") 

# test_tibble <- tibble_dt_c %>%
# 	mutate(cluster_1 = if_else(cluster == 1,
# 														 TRUE,
# 														 FALSE)
# 	)

geom_auto

dt_test <- sample_subset(dt = dt_c, size = 10000)

unique_clusters <- unique(dt_test$cluster)

dt_test_modified <- dt_test

for (cluster in unique_clusters) {
	column_name <- paste0("cluster_", cluster) # Create the new column name
	dt_test_modified <- dt_test_modified %>%
		mutate(!!column_name := if_else(cluster == !!cluster, TRUE, FALSE))
}
print(head(dt_test_modified))


ggplot(dt_test_modified) +
	geom_autodensity() +
	# geom_autohistogram() + 
	facet_matrix(cols = vars(distance, duration, charge),
							 rows = vars(cluster_4, 
							 						cluster_5, 
							 						cluster_6, 
							 						cluster_8, 
							 						cluster_3, 
							 						cluster_1,
							 						cluster_7))
