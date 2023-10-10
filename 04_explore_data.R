################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file explores the data for the first time to get an idea of the quality
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/04_utils.R")

dt <- read_rds(dt_path)

# Inspect number of trips over time
# posixct_min_start_time <- min(dt$start_time)
# posixct_max_start_time <- max(dt$start_time)

dt <- dt %>%
	mutate(date = as.Date(start_time))


# dt_trips_per_day <- dt %>%
# 											group_by(date) %>%
# 											summarise(n = n()) %>%
# 											as.data.table()



dt <- dt %>%
	mutate(avg_kmh = (distance/duration)*0.06) %>%
	# Where cut off value? I don't want to cut off relocating
	filter(avg_kmh <= 50, 
				 duration <= 1440,
				 distance <= 100000)

################################################################################
# Clustering Analysis
################################################################################

# K-means --------------------------------------

dt_kmeans <- dt %>%
	mutate(weekday = wday(start_time),
				 start_hour = hour(start_time)) %>%
	select(c("duration", "weekday", "start_hour"))

dt_kmeans_normalized <- normalize_dt(dt = dt_kmeans)

# Find optimal parameters

int_k <- 8

model_kmeans <- kmeans(dt_kmeans_normalized, int_k, nstart = 1L)

dt_kmeans_normalized <- add_cluster(dt = dt_kmeans_normalized,
																		cluster_model = model_kmeans)
dt_kmeans_reversed <- denorm_dt(dt = dt_kmeans, 
														    dt_norm = dt_kmeans_normalized)
dt_kmeans_reversed <- add_cluster(dt = dt_kmeans_reversed,
																		cluster_model = model_kmeans)
dt_summary_cluster <- create_summary_for_clusters(dt = dt_kmeans_reversed)

dt_kmeans_trips_per_hour <- trips_per_cluster_and_h(dt = dt_kmeans_reversed)

plot_kmeans <- create_start_hour_plot(dt = dt_kmeans_trips_per_hour)

dt_kmeans_normalized_reduced <- sample_subset(dt = dt_kmeans_normalized,
																							size = 10000)
num_kmeans_sil <- calc_silhoutte_score(dt = dt_kmeans_normalized_reduced)

# DBSCAN --------------------------------------

dt_dbcsan <- dt %>%
	mutate(weekday = wday(start_time),
				 start_hour = hour(start_time)) %>%
	select(c("duration", "weekday", "start_hour"))

dt_dbcsan_normalized <- normalize_dt(dt = dt_dbcsan)

# Find optimal parameters

int_min_pts <- 100

int_eps <- 0.1

model_dbscan <- dbscan(dt_dbcsan_normalized, eps = int_eps, minPts = int_min_pts)

dt_dbcsan_normalized <- add_cluster(dt = dt_dbcsan_normalized, 
																		cluster_model = model_dbscan)
dt_dbcsan_reversed <- denorm_dt(dt = dt_dbcsan, 
														    dt_norm = dt_dbcsan_normalized)
dt_dbcsan_reversed <- add_cluster(dt = dt_dbcsan_reversed,
																	cluster_model = model_dbscan)
dt_dbcsan_trips_per_hour <- trips_per_cluster_and_h(dt = dt_dbcsan_reversed)

dt_dbcsan_normalized_reduced <- sample_subset(dt = dt_dbcsan_normalized,
																							size = 10000)
num_dbscan_sil <- calc_silhoutte_score(dt = dt_dbcsan_normalized_reduced)
################################################################################
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
# 
# kNNdistplot(dt_dbscan_pca, k = int_min_pts)
# int_eps <- 0.00009
# model_dbscan_pca <- dbscan(dt_dbscan_pca, eps = int_eps, minPts = int_min_pts)


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
# ks <- 1:10
# tot.withinss <- sapply(ks, function(k) {
# 	kmeans_result <- kmeans(dt_kmeans_normalized, k, nstart = 10L)
# 	kmeans_result$tot.withinss
# })
# 
# withinss_df <- cbind.data.frame(ks, tot.withinss)
# 
# ggplot(withinss_df, aes(ks, tot.withinss)) +
# 	geom_point() +
# 	geom_line() +
# 	scale_y_continuous(name = "Total WSS", trans = "log10") +
# 	scale_x_continuous(name = "Number of clusters", breaks = ks)