################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 04_explore_data.R
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
  dt_dbcsan_trips_per_hour <- dt %>%
  	group_by(cluster, start_hour) %>%
  	summarise(trips_count = n()) %>%
  	arrange(cluster, start_hour) %>%
  	as.data.table()
  
  return(dt_dbcsan_trips_per_hour)
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


# Documentation: create_start_hour_plot
# Usage: create_start_hour_plot(dt)
# Description: Creates a plot for the start_hours for all clusters
# Args/Options: dt
# Returns: ggplot
# Output: ...
create_start_hour_plot <- function(dt) {
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


# Documentation: calc_silhoutte_score
# Usage: calc_silhoutte_score(dt)
# Description: Clalculates the silhouette score of a clustering result
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