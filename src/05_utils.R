################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 05_explore_data.R
################################################################################

# Documentation: normalize_dt
# Usage: normalize_dt (dt)
# Description: Normalizes dt 
# Args/Options: dt
# Returns: datatable
# Output: ...
normalize_dt <- function(dt) {
  dt_norm <- as.data.frame(lapply(dt, rescale))
  
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


# Documentation: create_3d_cluster_plot
# Usage: create_3d_cluster_plot(dt)
# Description: Creates a 3d plotly plot with the axes dist, dur, start_h
# Args/Options: dt
# Returns: plotly
# Output: ...
create_3d_cluster_plot <- function(dt) {
  plot <- plot_ly(data = dt) %>%
  	add_markers(
  		x = ~start_hour,
  		y = ~dest_hour,
  		#z = ~start_hour,
  		color = ~cluster,
  		#colors = colorRampPalette(c("red", "green", "blue"))(3), # example color palette
  		marker = list(size = 10, opacity = 0.8)
  	) %>%
  	layout(scene = list(xaxis = list(title = "Distance"),
  											yaxis = list(title = "Duration"),
  											zaxis = list(title = "Start Hour")))
  
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