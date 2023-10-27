################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 06_clean_data.R
################################################################################

# Documentation: normalize_dt
# Usage: normalize_dt (dt)
# Description: Normalizes dt 
# Args/Options: dt
# Returns: datatable
# Output: ...
normalize_dt <- function(dt) {
  dt_norm <- as.data.table(lapply(dt, rescale))
  
  return(dt_norm)
}


# Documentation: denorm_dt_
# Usage: denorm_dt (dt, dt_norm)
# Description: Denormalizes dt 
# Args/Options: dt, dt_norm
# Returns: datatable
# Output: ...
denorm_dt <- function(dt, dt_norm) {
  dt_rev <- as.data.frame(mapply(function(norm_col, orig_col) {
  	rescale(norm_col, to = range(orig_col), from = c(0, 1))
  }, norm_col = dt_norm, orig_col = dt, SIMPLIFY = FALSE))
  
  return(dt_rev)
}


# Documentation: add_cluster
# Usage: add_cluster(dt, dt_norm, cluster_model)
# Description: Adds the cluster found by the clustering algorithm to dt
# Args/Options: dt, cluster_model
# Returns: datatable
# Output: ...
add_cluster <- function(dt, cluster_model) {
  dt_reversed <- dt %>% 
  	mutate(cluster = cluster_model$cluster)
  
  return(dt_reversed)
}

# Documentation: remove_noise
# Usage: remove_noise(dt)
# Description: Removes the noise-cluster found by algorithms like DBSCAN
# Args/Options: dt
# Returns: datatable
# Output: ...
remove_noise <- function(dt) {
	dt <- dt %>%
  	filter(cluster != 0)
	return(dt)
}


# Documentation: trips_per_cluster_and_h
# Usage: trips_per_cluster_and_h(dt)
# Description: Groups dt by cluster & start_hour and counts the trips
# Args/Options: dt
# Returns: datatable
# Output: ...
trips_per_cluster_and_h <- function(dt) {
  dt_dbcsan_trips_per_cl_and_h <- dt %>%
  	group_by(cluster, start_hour) %>%
  	summarise(trips_count = n()) %>%
  	arrange(cluster, start_hour) %>%
  	as.data.table()
  
  return(dt_dbcsan_trips_per_cl_and_h)
}


# Documentation: trips_per_h
# Usage: trips_per_h(dt)
# Description: Groups dt by start_hour and counts the trips
# Args/Options: dt
# Returns: datatable
# Output: ...
trips_per_h <- function(dt) {
	dt_dbcsan_trips_per_h <- dt %>%
		group_by(start_hour) %>%
		summarise(trips_count = n()) %>%
		arrange(start_hour) %>%
		as.data.table()
	
	return(dt_dbcsan_trips_per_h)
}

# Documentation: trips_per_wd
# Usage: trips_per_wd(dt)
# Description: Groups dt by weekday and counts the trips
# Args/Options: dt
# Returns: datatable
# Output: ...
trips_per_wd <- function(dt) {
	dt_dbcsan_trips_per_wd <- dt %>%
		group_by(weekday) %>%
		summarise(trips_count = n()) %>%
		arrange(weekday) %>%
		as.data.table()
	
	return(dt_dbcsan_trips_per_wd)
}


# Documentation: trips_per_date
# Usage: trips_per_date(dt)
# Description: Groups dt by date and counts the trips
# Args/Options: dt
# Returns: datatable
# Output: ...
trips_per_date <- function(dt) {
	dt_dbcsan_trips_per_date <- dt %>%
		group_by(date) %>%
		summarise(trips_count = n()) %>%
		arrange(date) %>%
		as.data.table()
	
	return(dt_dbcsan_trips_per_date)
}


# Documentation: trips_per_date_wd
# Usage: trips_per_date_wd(dt)
# Description: Groups dt by date and counts the trips
# Args/Options: dt
# Returns: datatable
# Output: ...
trips_per_date_wd <- function(dt) {
	dt_dbcsan_trips_per_date_wd <- dt %>%
		group_by(date) %>%
		summarise(trips_count = n()) %>%
		arrange(date) %>%
		as.data.table()
	
	dt_dbcsan_trips_per_date_wd <- dt_dbcsan_trips_per_date_wd %>%
		mutate(weekday = wday(date, week_start = 1))
	
	dt_dbcsan_trips_per_date_wd <- dt_dbcsan_trips_per_date_wd %>%
		mutate(date_wd = paste0(date, "_", weekday))
	
	return(dt_dbcsan_trips_per_date_wd)
}




# Documentation: create_summary_for_clusters
# Usage: create_summary_for_clusters(dt)
# Description: Creates a summary (min, max, mean...) for all clusters & vars
# Args/Options: dt
# Returns: datatable
# Output: ...
create_summary_for_clusters <- function(dt) {
  dt_summary_cluster <- dt %>%
  	group_by(cluster) %>%
  	summarise(
  		trips = n(),
  		across(
  			everything(), 
  			list(
  				min = ~min(.),
  				q1 = ~quantile(., 0.25),
  				mean = ~mean(.),
  				q3 = ~quantile(., 0.75),
  				max = ~max(.)
  			)
  		)
  	) %>%
  	as.data.table()
  
  return(dt_summary_cluster)
}


# Documentation: create_start_hour_plot_by_cluster
# Usage: create_start_hour_plot_by_cluster(dt)
# Description: Creates a plot for the start_hours for all clusters
# Args/Options: dt
# Returns: ggplot
# Output: ...
create_start_hour_plot_by_cluster <- function(dt) {
  plot <- ggplot(dt, aes(
  	x = start_hour, 
  	y = trips_count, 
  	color = as.factor(cluster), 
  	group = cluster)
  ) +
  	geom_line() +
  	labs(title = "Trips Count by Start Hour and Cluster",
  			 x = "Start Hour",
  			 y = "Trips Count",
  			 color = "Cluster") +
  	theme_minimal()
  
  return(plot)
}

# Documentation: create_start_hour_plot
# Usage: create_start_hour_plot(dt)
# Description: Creates a plot for the start_hours
# Args/Options: dt
# Returns: ggplot
# Output: ...
create_start_hour_plot <- function(dt) {
	plot <- ggplot(dt, aes(
		x = start_hour, 
		y = trips_count)
	) +
		geom_line() +
		geom_point() + 
		geom_text(aes(label = trips_count), vjust = -0.5)
		labs(title = "Trips Count by Start Hour",
				 x = "Start Hour",
				 y = "Trips Count") +
		scale_x_continuous(breaks = unique(dt$start_hour)) +
		theme_minimal()
	
	return(plot)
}


# Documentation: create_weekday_plot
# Usage: create_weekday_plot(dt)
# Description: Creates a plot for the weekdays
# Args/Options: dt
# Returns: ggplot
# Output: ...
create_weekday_plot <- function(dt) {
	plot <- ggplot(dt, aes(
		x = weekday, 
		y = trips_count)
	) +
		geom_line() +
		geom_point() + 
		geom_text(aes(label = trips_count), vjust = -0.5)
		labs(title = "Trips Count by Weekday",
				 x = "Weekday",
				 y = "Trips Count") +
		scale_x_continuous(breaks = unique(dt$weekday)) + 
		theme_minimal()
	
	return(plot)
}


# Documentation: create_date_plot
# Usage: create_date_plot(dt)
# Description: Creates a plot for the dates
# Args/Options: dt
# Returns: ggplot
# Output: ...
create_date_plot <- function(dt) {
	plot <- ggplot(dt, aes(
		x = date, 
		y = trips_count)
	) +
		geom_line() +
		geom_point() + 
		geom_text(aes(label = trips_count), vjust = -0.5)
		labs(title = "Trips Count by Date",
				 x = "Date",
				 y = "Trips Count") +
		scale_x_continuous(breaks = unique(dt$date)) + 
		theme_minimal()
	
	return(plot)
}


# Documentation: create_date_and_wd_plot
# Usage: create_date_and_wd_plot(dt)
# Description: Creates a plot for the dates and the weekdates
# Args/Options: dt
# Returns: ggplot
# Output: ...
create_date_and_wd_plot <- function(dt) {
	plot <- ggplot(dt, aes(
		x = date_wd, 
		y = trips_count)
	) +
		geom_line(aes(group = 1)) +
		geom_point() + 
		geom_text(aes(label = trips_count), vjust = -0.5)
	labs(title = "Trips Count by Date_Weekday",
			 x = "Date",
			 y = "Trips Count") +
		scale_x_continuous(breaks = unique(dt$date_wd)) + 
		theme_minimal()
	
	return(plot)
}


# Documentation: create_opt_k_kmeans_plot
# Usage: create_opt_k_kmeans_plot(dt, ks)
# Description: Creates a plot to look for the optimal number of clusters
# Args/Options: dt, ks
# Returns: ggplot
# Output: ...
create_opt_k_kmeans_plot <- function(dt, ks) {
  tot.withinss <- sapply(ks, function(k) {
  	kmeans_result <- kmeans(dt, k, nstart = 10L)
  	kmeans_result$tot.withinss
  })
  
  withinss_df <- cbind.data.frame(ks, tot.withinss)
  
  plot <- ggplot(withinss_df, aes(ks, tot.withinss)) +
  	geom_point() +
  	geom_line() +
  	scale_y_continuous(name = "Total WSS", trans = "log10") +
  	scale_x_continuous(name = "Number of clusters", breaks = ks)
  
  return(plot)
}


# Documentation: calc_silhoutte_score
# Usage: calc_silhoutte_score(dt)
# Description: Calculates the silhouette score of a clustering result
# Args/Options: dt
# Returns: ggplot
# Output: ...
calc_silhoutte_score <- function(dt) {
  sil <- intCriteria(dt %>% as.matrix(), 
  															dt$cluster, 
  															c("Silhouette"))
  sil <- sil$silhouette
  return(sil)
}

# Documentation: create_kmeans_clustered_dt
# Usage: create_kmeans_clustered_dt(dt, dt_norm, k)
# Description: Runs kmeans on norm. data, denorm. after + adds cluster
# Args/Options: dt, dt_norm, k
# Returns: datatable
# Output: ...
create_kmeans_clustered_dt <- function(dt, dt_norm, k) {
  model <- kmeans(dt_norm, k, nstart = 1L)
  dt_norm <- add_cluster(dt = dt_norm,
  											 cluster_model = model)
  dt_rev <- denorm_dt(dt = dt,
  										dt_norm = dt_norm)
  dt_rev <- add_cluster(dt = dt_rev,
  											cluster_model = model)
  
  return(dt_rev)
}

# Documentation: create_dbscan_clustered_dt
# Usage: create_dbscan_clustered_dt(dt, dt_norm, eps, minpts)
# Description: Runs dbcsan on norm. data, denorm. after + adds cluster
# Args/Options: dt, dt_norm, eps, minpts
# Returns: datatable
# Output: ...
create_dbscan_clustered_dt <- function(dt, dt_norm, eps, minpts) {
	model <- dbscan(dt_norm, eps, minpts)
	dt_norm <- add_cluster(dt = dt_norm,
												 cluster_model = model)
	dt_rev <- denorm_dt(dt = dt,
											dt_norm = dt_norm)
	dt_rev <- add_cluster(dt = dt_rev,
												cluster_model = model)
	
	return(dt_rev)
}

# Documentation: create_gmm_clustered_dt
# Usage: create_gmm_clustered_dt(dt)
# Description: Runs dbcsan on norm. data, adds cluster to org. data
# Args/Options: dt
# Returns: datatable
# Output: ...
create_gmm_clustered_dt <- function(dt) {
	dt_norm <- normalize_dt(dt = dt)
	gmm <- Mclust(data = dt_norm)
	dt_c <- dt %>%
		mutate(cluster = gmm$classification)
	
	return(dt_c)
}


# Documentation: create_3d_cluster_plot
# Usage: create_3d_cluster_plot(dt)
# Description: Creates a 3d plotly plot with the axes dist, dur, start_h
# Args/Options: dt
# Returns: plotly
# Output: ...
create_3d_cluster_plot <- function(dt) {
  plot <- plot_ly(data = dt) %>%
  	add_markers(
  		x = ~duration,
  		y = ~dest_hour,
  		z = ~charge,
  		color = ~cluster,
  		#colors = colorRampPalette(c("red", "green", "blue"))(3), # example color palette
  		marker = list(size = 10, opacity = 0.8)
  	) %>%
  	layout(scene = list(xaxis = list(title = "Duration"),
  											yaxis = list(title = "Dest Hour"),
  											zaxis = list(title = "Charge")))
  
  return(plot)
}





# Documentation: select_most_frequent_scooters
# Usage: select_most_frequent_scooters(dt, n)
# Description: Creates subset of dt with most (n) used scooters 
# Args/Options: dt, n
# Returns: datatable
# Output: ...
select_most_frequent_scooters <- function(dt, n) {
  char_scooters_most_rides <- dt %>%
  	group_by(id) %>%
  	summarise(trips_count = n()) %>%
  	arrange(desc(trips_count)) %>%
  	head(n) %>%
  	pull(id)
  
  
  dt <- dt %>%
  	filter(id %in% char_scooters_most_rides)
  
  return(dt)
}


# Documentation: create_duration_line_plot
# Usage: create_duration_line_plot(dt)
# Description: Creates linplot; x = ride, y = duration
# Args/Options: dt
# Returns: ggplot
# Output: ...
create_duration_line_plot <- function(dt) {
  ggplot(dt, aes(x = ride)) +
  	geom_line(aes(y = duration, group = 1), color = "blue") +
  	geom_point(aes(y = duration), color = "blue") +
  	labs(y = "Duration (in blue)", title = "Duration over Time") +
  	theme_minimal()
}


# Documentation: create_hist_grid_cluster
# Usage: create_hist_grid_cluster(dt)
# Description: Creates a grid of histograms for each variable & cluster
# Args/Options: dt
# Returns: ggplot
# Output: ...
create_hist_grid_cluster <- function(dt) {
	dt <- dt %>%
		mutate(cluster = as.factor(cluster))
	
	
	tibble_dt <- dt %>% 
		pivot_longer(cols = -cluster, names_to = "variable", values_to = "value")
	
	# Now, we create the plot using facet_grid
	plot <- ggplot(tibble_dt, aes(x = value)) +
		geom_histogram(bins = 30, fill = "blue", color = "black") + # customize as needed
		facet_grid(cluster ~ variable, scales = "free") +
		theme_light() +
		labs(title = "Distribution of variables across clusters", 
				 x = "Value", 
				 y = "Count")
	

	
	return(plot)
}

# Documentation: create_hist_grid_classes
# Usage: create_hist_grid_classes(dt)
# Description: Creates a grid of histograms for each variable & class
# Args/Options: dt
# Returns: ggplot
# Output: ...
create_hist_grid_classes <- function(dt) {
	dt <- dt %>%
		mutate(class = as.factor(class))
	
	
	tibble_dt <- dt %>% 
		pivot_longer(cols = -class, names_to = "variable", values_to = "value")
	
	# Now, we create the plot using facet_grid
	plot <- ggplot(tibble_dt, aes(x = value)) +
		geom_histogram(bins = 30, fill = "blue", color = "black") + # customize as needed
		facet_grid(class ~ variable, scales = "free") +
		theme_light() +
		labs(title = "Distribution of variables across class", 
				 x = "Value", 
				 y = "Count")
	
	
	
	return(plot)
}


# Documentation: get_subsequent_trips
# Usage: get_subsequent_trips(dt_subset, dt_compare)
# Description: Takes id & ride of sub dt & looks for subsequent trips in big dt
# Args/Options: dt_subset, dt_compare
# Returns: datatable
# Output: ...
get_subsequent_trips <- function(dt_subset, dt_compare) {
	list_of_rides <- list()
	for(i in 1:nrow(dt_subset)){
		id_i <- dt_subset[i, "id"] %>% as.character()
		ride_i <- dt_subset[i, "ride"] %>% as.integer()
		
		dt_temp <- dt_compare %>%
			filter(id==id_i) %>%
			arrange(ride) %>%
			mutate(lead_ride = lead(ride))

		next_ride <- dt_temp %>%
			filter(ride==ride_i) %>%
			select(lead_ride) %>%
			as.integer()
		
		row <- dt_temp %>%
			filter(ride==next_ride)
		
		list_of_rides[[i]] <- row
	}
	dt_subset <- bind_rows(dt_subset, list_of_rides) %>%
		arrange(id, ride)
	return(dt_subset)
}


# Documentation: create_sf_start_dest_lines
# Usage: create_sf_start_dest_lines(dt)
# Description: Creates sf-lines for start-dest-coordiantes of given dt
# Args/Options: dt
# Returns: sf-lines
# Output: ...
create_sf_start_dest_lines <- function(dt) {
	# Function to create a line from starting and destination coordinates
	create_line <- function(lon1, lat1, lon2, lat2) {
		st_linestring(rbind(c(lon1, lat1), c(lon2, lat2)))
	}
	
	# Apply the function to each ride
	lines <- mapply(create_line, 
									dt$start_loc_lon, dt$start_loc_lat, 
									dt$dest_loc_lon, dt$dest_loc_lat, 
									SIMPLIFY = FALSE)
	
	# Create an 'sf' object with the lines
	sf_lines <- st_sf(id = dt$id, geometry = st_sfc(lines, crs = 4326))
	
	return(sf_lines)
}



# Documentation: create_berlin_leaflet_map
# Usage: create_berlin_leaflet_map(dt)
# Description: Creates OSM-leaflet-map for Berlin
# Args/Options: ...
# Returns: leaflet
# Output: ...
create_berlin_leaflet_map <- function() {
	m <- leaflet() %>%
		setView(lng = 13.4050, lat = 52.5200, zoom = 10) %>%  # coordinates for Berlin
		addTiles()  # Add default OpenStreetMap map tiles
	return(m)
}


# Documentation: create_plot_compare_charge_noncharge
# Usage: create_plot_compare_charge_noncharge(dt_1, dt_2)
# Description: Compares duration of trip and new charge status after trip
# Args/Options: dt_1, dt_2
# Returns: ggplot
# Output: ...
create_plot_compare_charge_noncharge <- function(dt_1, dt_2) {
	p1.1 <- ggplot(dt_1) + 
		geom_point(aes(x = charge_dest, y = duration), color ="#E69F00")
	
	p1.2 <- ggplot(dt_2) + 
		geom_point(aes(x = charge_dest, y = duration), color ="#56B4E9")
	
	
	p2.1 <- ggplot(dt_1) + 
		geom_boxplot(aes(x = charge_dest)) 
	
	p2.2 <- ggplot(dt_2) + 
		geom_boxplot(aes(x = charge_dest)) 
	
	p3.1 <- ggplot(dt_1) + 
		geom_boxplot(aes(x = log(duration))) +
		geom_text(aes(x = mean(log(duration)), 
									y = 0.4,
									label =round(mean(duration),2)),
							color = "#E69F00")
	
	p3.2 <- ggplot(dt_2) + 
		geom_boxplot(aes(x = log(duration))) +
		geom_text(aes(x = mean(log(duration)), 
									y = 0.4,
									label =round(mean(duration),2)),
							color = "#56B4E9")
	
	p4.1 <- ggplot(dt_1) + 
		geom_bar(aes(x = weekday)) 
	
	p4.2 <- ggplot(dt_2) + 
		geom_bar(aes(x = weekday)) 
	
	p5.1 <- ggplot(dt_1) + 
		geom_bar(aes(x = dest_hour)) 
	
	p5.2 <- ggplot(dt_2) + 
		geom_bar(aes(x = dest_hour)) 
	
	# Create an arrangement of plots (not yet drawing them)
	# grid_arrangement <- arrangeGrob(p1.1, p1.2,
	# 																p2.1, p2.2, 
	# 																p3.1, p3.2, 
	# 																p4.1, p4.2, 
	# 																nrow = 2)
	# 
	# # Create titles as grobs (graphic objects)
	# title_grob_1 <-
	# 	textGrob("Charging trips", gp = gpar(fontsize = 14, fontface = "bold"))
	# title_grob_2 <-
	# 	textGrob("Non charging trips", gp = gpar(fontsize = 14, fontface = "bold"))
	# 
	# # Use the 'gtable' package functions to add the titles to the top of the layout
	# grid_arrangement <-
	# 	gtable::gtable_add_rows(grid_arrangement,
	# 													heights = unit(1, "lines"),
	# 													pos = 0)
	# grid_arrangement <-
	# 	gtable::gtable_add_grob(grid_arrangement,
	# 													title_grob_1,
	# 													t = 1,
	# 													l = 1,
	# 													r = 1) # Adjust 'l' and 'r' to position the title
	# grid_arrangement <-
	# 	gtable::gtable_add_grob(grid_arrangement,
	# 													title_grob_2,
	# 													t = 1,
	# 													l = 2,
	# 													r = 2) # Adjust 'l' and 'r' to position the title
	
	plot <- grid.arrange(p1.1, 
							 p2.1, 
							 p3.1, 
							 p4.1,
							 p5.1,
							 textGrob("Charging trips", 
							 				 gp = gpar(fontsize = 10, font = 3)),
							 p1.2,
							 p2.2,
							 p3.2,
							 p4.2,
							 p5.2,
							 textGrob("Non charging trips", 
							 				 gp = gpar(fontsize = 10, font = 3)),
							 nrow = 2
							 )
	
	return(plot)
}



# Documentation: prepare_gmm
# Usage: prepare_gmm(dt)
# Description: Creates and selects columns for the gmm-clustering
# Args/Options: dt
# Returns: datatable
# Output: ...
prepare_gmm <- function(dt) {

	dt <- dt %>%
		mutate(
			time_of_day = case_when(
				dest_hour >= 0 & dest_hour < 7  ~ "night",
				dest_hour >= 7 & dest_hour < 13 ~ "morning",
				dest_hour >= 13 & dest_hour < 20 ~ "afternoon",
				dest_hour >= 20 & dest_hour <= 23 ~ "evening",
			)
		) %>%
		mutate(
			part_of_week = case_when(
				weekday >= 1 & weekday <= 5 ~ "working day",
				weekday > 5 ~ "weekend"
			)
		) 
		
	dt <- dt %>%
		mutate(
			charge_increase = factor(charge_increase),
			time_of_day = factor(time_of_day),
			part_of_week = factor(part_of_week)
		)
	

	factor_levels <- list(
		charge_increase = levels(dt$charge_increase),
		time_of_day = levels(dt$time_of_day),
		part_of_week = levels(dt$part_of_week)
	)


	

	dt <- dt %>%
		mutate(
			charge_increase = as.integer(charge_increase),
			time_of_day = as.integer(time_of_day),
			part_of_week = as.integer(part_of_week)
		) %>%
		select(distance, 
					 duration, 
					 charge_start,
					 charge_dest,
					 charge_loss,
					 charge_increase, 
					 time_of_day,
					 part_of_week)
	
	return(list("dt" = dt, "factor_levels" = factor_levels))
	
}



# Documentation: convert_to_original_category
# Usage: convert_to_original_category(integers, original_levels)
# Description: Returns the original factor levels
# Args/Options: integers, original_levels
# Returns: vector of characters
# Output: ...
convert_to_original_category <- function(integers, original_levels) {
	# Check if all integers are valid indices
	if (all(integers %in% seq_along(original_levels))) {
		return(original_levels[integers])
	} else {
		warning("Some integers do not correspond to factor levels.")
		return(NA) # or handle this case as you need
	}
}


# Documentation: create_plots
# Usage: create_plots(dt_sub)
# Description: Creates list of mixed ggplots 
# Args/Options: dt_sub
# Returns: list
# Output: ...
create_plots <- function(dt_sub) {
	
	ticks_time_of_day <- lapply(1:4,function(x){
		convert_to_original_category(x, factor_levels$time_of_day)
	}) %>% unlist
	
	i = 1
	plots <- list()
	variables <- unique(dt_sub$variable)
	print(variables)
		if("last_trip" %in% variables){
			p_last_trip <- ggplot(dt_sub) +
				geom_bar(
					data = . %>% filter(variable == "last_trip"),
					aes(x = factor(value, levels = c(1,2)), fill = as.factor(value))
				) +
				theme(legend.position = "none") +
				scale_x_discrete(drop = FALSE) +
				xlab("last_trip")
			
			plots[[i]] <- p_last_trip
		i = i +1
	}

	if("time_of_day" %in% variables){
		p_time_of_day <- ggplot(dt_sub) +
			geom_bar(
				data = . %>% filter(variable == "time_of_day"),
				aes(x = factor(value, levels = c(1,2,3,4)), fill = as.factor(value))
			) +
			theme(legend.position = "none") +
			scale_x_discrete(drop = FALSE,
											 labels = ticks_time_of_day) +
			xlab("time_of_day")
		
		plots[[i]] <- p_time_of_day
	i = i +1
	}

	if("part_of_week" %in% variables){
		p_part_of_week <- ggplot(dt_sub) +
			geom_bar(
				data = . %>% filter(variable == "part_of_week"),
				aes(x = factor(value, levels = c(1,2)), fill = as.factor(value))
			) +
			theme(legend.position = "none") +
			scale_x_discrete(drop = FALSE) +
			xlab("part_of_week")
		
		plots[[i]] <- p_part_of_week
		i = i +1
	}
	
	if("charge_increase" %in% variables){
		p_charge_increase <- ggplot(dt_sub) + 
			geom_bar(
				data = . %>% filter(variable == "charge_increase"), 
				aes(x = factor(value, levels = c(1,2)), fill = as.factor(value))
			) +
			theme(legend.position = "none") + 
			scale_x_discrete(drop = FALSE) +
			xlab("charge_increase")
		
		plots[[i]] <- p_charge_increase
		i = i +1
	}
	
	if("distance" %in% variables){
		p_distance <- ggplot(dt_sub %>% filter(variable == "distance")) + 
			geom_boxplot(
				aes(x = log(value))
			) +
			geom_text(aes(x = mean(log(value)), 
										y = 0.5,
										label =round(mean(value),2)),
								color = "#D55E00") + 
			scale_x_continuous(name = "log-distance", 
												 limits = c(list_min_val$distance %>% log,
												 					 list_max_val$distance %>% log))
		
		plots[[i]] <- p_distance
		i = i +1
	}

	if("duration" %in% variables){
		p_duration <- ggplot(dt_sub %>% filter(variable == "duration")) + 
			geom_boxplot(
				aes(x = log(value))
			) +
			geom_text(aes(x = mean(log(value)), 
										y = 0.5,
										label =round(mean(value),2)),
								color = "#D55E00") + 
			scale_x_continuous(name = "log-duration", 
												 limits = c(list_min_val$duration %>% log,
												 					 list_max_val$duration %>% log))
		
		plots[[i]] <- p_duration
		i = i +1
	}
	
	if("charge" %in% variables){
		p_charge <- ggplot(dt_sub) + 
			geom_density(
				data = . %>% filter(variable == "charge"), 
				aes(x = value)
			) +
			scale_x_continuous(name = "charge", 
												 limits = c(list_min_val$charge %>% log,
												 					 list_max_val$charge %>% log))
		
		plots[[i]] <- p_charge
		i = i +1
	}

	if("charge_loss" %in% variables){
		p_charge_loss <- ggplot(dt_sub) + 
			geom_bar(
				data = . %>% filter(variable == "charge_loss"), 
				aes(x = value)
			) +
			scale_x_continuous(name = "charge_loss", 
												 limits = c(-1,
												 					  list_max_val$charge_loss))
		
		plots[[i]] <- p_charge_loss
		i = i +1
	}
	return(plots)
}




# Documentation: create_mixed_grid_vars_over_cluster
# Usage: create_mixed_grid_vars_over_cluster(dt_pivot_longer)
# Description: Creates grid of mixed plots over different vars & clusters 
# Args/Options: dt_pivot_longer
# Returns: grid (gridExtra)
# Output: ...
create_mixed_grid_vars_over_cluster <- function(dt_pivot_longer) {
	

	int_n_col <- unique(dt_pivot_longer$variable) %>% length + 1
  vector_numb_clusters <- unique(dt_pivot_longer$cluster)
  numb_trips_cluster <- dt_pivot_longer %>%
  	filter(variable==dt_pivot_longer[1,"variable"] %>% as.character) %>%
  	select(cluster) %>%
  	pull %>%
  	table %>%
  	as.numeric
  
  list_of_subsets <- lapply(vector_numb_clusters, function(x){
  	dt_pivot_longer %>%
  		filter(cluster == x)
  })
  
  list_of_plots <- lapply(list_of_subsets, create_plots)
  list_1 <- do.call(c, list_of_plots)
  list_2 <- lapply(vector_numb_clusters, function(i) {
  	textGrob(paste("Cluster", i, "\n n =", numb_trips_cluster[i]), 
  					 gp = gpar(fontsize = 10, font = 3))
  })
  list_combined <- list()
  
  int_index_1 <- 1
  int_index_2 <- 1
  int_total_length <- length(list_1) + length(list_2) 
  
  for (i in 1:int_total_length) {
  	print(i)
  	if ((i %% int_n_col)!=0) {
  		list_combined[[i]] <- list_1[[int_index_1]]
  		int_index_1 <- int_index_1 + 1	
  	} else {
  		list_combined[[i]] <- list_2[[int_index_2]]
  		int_index_2 <- int_index_2 + 1	
  	}
  }

  
  grid_layout <- do.call("grid.arrange", c(list_combined, ncol = int_n_col))
  
  return(grid_layout)
}



# Documentation: create_gmm_dt
# Usage: create_gmm_dt(char_cols, dt, dt_norm)
# Description: Takes num vars from dt_norm and int vars from dt
# Args/Options: char_cols, dt, dt_norm
# Returns: datatable
# Output: ...
create_gmm_dt <- function(char_cols, dt, dt_norm) {
	list_of_cols <- list()
	i <- 1
	for(name in char_cols){
		class <- dt[,..name] %>% pull %>% class 
		if(class=="numeric"){
			list_of_cols[[i]] = dt_norm[,..name]
			i = i + 1
		} else {
			list_of_cols[[i]] = dt[,..name]
			i = i + 1
		}
	}
	
	dt_gmm <- list.cbind(list_of_cols)
	
	return(dt_gmm)
}
