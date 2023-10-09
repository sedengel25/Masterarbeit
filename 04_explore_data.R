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



dt <- dt %>%
	mutate(avg_kmh = (distance/duration)*0.06) %>%
	# Where cut off value? I don't want to cut off relocating
	filter(avg_kmh <= 50, 
				 duration <= 1440,
				 distance <= 100000)

################################################################################
# Temporal Analysis
################################################################################

# K-Means
dt_kmeans <- dt %>%
	mutate(weekday = wday(start_time),
				 start_hour = hour(start_time)) %>%
	select(c("duration", "weekday", "start_hour"))

dt_kmeans_normalized <- as.data.frame(lapply(dt_kmeans, rescale))

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


k <- 8
model_kmeans <- kmeans(dt_kmeans_normalized, k, nstart = 1L)
model_kmeans

dt_kmeans_reversed <- as.data.frame(mapply(function(norm_col, orig_col) {
	rescale(norm_col, to = range(orig_col), from = c(0, 1))
}, norm_col = dt_kmeans_normalized, orig_col = dt_kmeans, SIMPLIFY = FALSE))

dt_kmeans_reversed <- dt_kmeans_reversed %>% 
	mutate(cluster = model_kmeans$cluster)


dt_summary_cluster <- dt_kmeans_reversed %>%
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
dt_summary_cluster


# DBSCAN
dt_dbcsan <- dt %>%
	mutate(weekday = wday(start_time),
				 start_hour = hour(start_time)) %>%
	select(c("duration", "weekday", "start_hour"))


# ggplot(data = dt_trips_per_day)+
# 	geom_line(aes(x = date, y = n))
# 
# 
# dt_dbscan <- dt[,c("start_loc_lat", "start_loc_lon", "dest_loc_lat", 
# 									 "dest_loc_lon", "start_time", "dest_time", "distance",
# 									 "duration")]
# 
# 
# 
# # dt_dbscan <- dt_dbscan %>%
# # 	mutate(
# # 		start_month = month(start_time),
# # 		start_day = day(start_time),
# # 		start_hour = hour(start_time),
# # 		start_minute = minute(start_time),
# # 		dest_month = month(start_time),
# # 		dest_day = day(dest_time),
# # 		dest_hour = hour(dest_time),
# # 		dest_minute = minute(dest_time)
# # 	) %>%
# # 	select(where(is.numeric))
# # 
# # 
# 
# # 
# # int_cols_only_na <- apply(dt_dbscan, 2, function(x) all(is.na(x))) %>% which %>% as.numeric
# # 
# # dt_dbscan <- dt_dbscan %>%
# # 	select(-all_of(int_cols_only_na))
# 
# 
# 
# #start_hour, weekday, distance, duration
# dt_dbscan <- dt_dbscan %>%
# 	mutate(weekday = wday(start_time),
# 				 start_hour = hour(start_time)) %>%
# 	select(c("start_loc_lat", "start_loc_lon", "dest_loc_lat", 
# 						 "dest_loc_lon", "distance", "duration", "weekday", "start_hour"))
# 
# 
# dt_dbscan <- dt_dbscan %>%
# 	mutate(across(.fns = ~ (.x - min(.x)) / (max(.x) - min(.x))))
# 
# int_min_pts <- 2*ncol(dt_dbscan)
# # dt_dbscan <- dt_dbscan[which(dt_dbscan$duration<100),]
# # dt_dbscan <- dt_dbscan[which(dt_dbscan$distance<5000),]
# kNNdistplot(dt_dbscan, k = int_min_pts)
# 
# 
# int_eps <- 0.27
# 
# model_dbscan_2 <- dbscan(dt_dbscan, eps = int_eps, minPts = int_min_pts)
# 
# model_dbscan_2
# 
# 
# 
# 
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
# 
# 
# fviz_cluster(model_dbscan_pca, dt_dbscan_pca)













vector_rand_inx <- sample(1:nrow(dt_dbscan_pca), size = 50000)

dt_hdbscan_pca <- dt_dbscan_pca %>%
	slice(vector_rand_inx)
int_min_pts <- 4
model_hdbscan_pca <- hdbscan(dt_hdbscan_pca, minPts = int_min_pts)


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
