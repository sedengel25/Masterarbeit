################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file creates clusters based on the p-values of the flows
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/00_config_psql.R")
source("./src/12_utils.R")

################################################################################
# Configuration
################################################################################
char_city_prefix <- "col"
int_crs <- 32632
int_k <- read_rds(file_rds_int_k)
double_alpha <- 0.1
char_alpha <- gsub("\\.", "", as.character(double_alpha))
char_flows_pvalues <- paste0(char_city_prefix, "_flows_pvalues")
char_common_flows <- paste0(char_city_prefix, "_", int_k, "_common_flows")
char_common_flows_dr_only <- paste0(char_city_prefix, 
																		"_", 
																		int_k, 
																		"_common_flows_dr_only")
char_viz <- paste0(char_city_prefix, "_vis_flows")
char_cluster <- paste0(char_city_prefix, "_cluster")
# Create a table for the common flows for the directy reachable flows only
# psql_create_table_directly_reachable_only(con = con,
# 																					table_common_flows = char_common_flows,
# 																					table_common_flows_dr_only = char_common_flows_dr_only)

dt_flows <- read.csv(file_csv_p_values)
for(i in 1:nrow(dt_flows)){
	if(dt_flows[i, "density"] <= double_alpha){
		dt_flows[i, "core_flow"] <- "yes"
	}
}
# Add a column for cluster IDs and a flag for processed flows
dt_flows$cluster_id <- NA
dt_flows$processed <- FALSE

cluster_id <- 1

################################################################################
# Assign Cluster (Density Conenctivity Mechanism)
################################################################################
while (any(dt_flows$core_flow == 'yes' &
					 !dt_flows$processed)) {
	# Select a random unprocessed core flow
	unprocessed_core_flows <- get_unprocessed_core_flows(dt_flows)
	

	if (length(unprocessed_core_flows) == 0){
		break
	}

	selected_flow <- sample(unprocessed_core_flows, 1)

	# Assign a cluster
	dt_flows$cluster_id[dt_flows$flow1 == selected_flow] <-
		cluster_id
	# Flag flow as processed
	dt_flows$processed[dt_flows$flow1 == selected_flow] <-
		TRUE
	 

	repeat {
		cat("selected_flows: ", selected_flow, "\n")

		directly_reachable_flows <-
			psql_get_directly_reachable_flows(
				con = con,
				table_common_flows_dr_only = char_common_flows_dr_only,
				flow_considered = selected_flow
			)
		
		cat("Flows that are directly reachable from randomly selected core flow ", 
				selected_flow, ": \n",
				paste0(directly_reachable_flows, "\n")
		)

		# directly_reachable_flows_idx <- which(dt_flows$flow1%in%directly_reachable_flows)
		
		# Get all directly reachable core flows from selected core flow
		directly_reachable_cores <-
			directly_reachable_flows[directly_reachable_flows %in% 
															 	unprocessed_core_flows]
		
		cat("Core flows that are directly reachable from randomly selected core flow ", 
				selected_flow, ": \n",
				paste0(directly_reachable_cores, "\n")
		)
		
		# Stop once there is no directly reachable core flow
		if (length(directly_reachable_cores) == 0){
			break
		}

		

		# Assign the found directly-reachable core flows the same ID as the selected flow..
		dt_flows$cluster_id[dt_flows$flow1 %in% directly_reachable_cores] <- cluster_id
		# ...and flag them as processed
		dt_flows$processed[dt_flows$flow1 %in% directly_reachable_cores] <- TRUE
		
		# The newly added core flows again maybe have further dr-core-flows, therefore:
		# Update the selected flow for the next iteration 
		selected_flow <- directly_reachable_cores
		unprocessed_core_flows <- get_unprocessed_core_flows(dt_flows)
		cat("length unprocessed core flows: ", 
				length(unprocessed_core_flows), "\n")
	}

	cluster_core_flows <- dt_flows$flow1[which(dt_flows$cluster_id == cluster_id)]

	if (length(cluster_core_flows) == 0){
		break
	}
	
	# Add non-core flows that are directly reachable from core flows in this cluster
	directly_reachable_non_cores <-
		psql_get_directly_reachable_flows(con,
																			table_common_flows_dr_only = char_common_flows_dr_only,
																			flow_considered = cluster_core_flows)
	
	non_core_flows <-
		directly_reachable_non_cores[!directly_reachable_non_cores %in% unprocessed_core_flows]
	
	dt_flows$cluster_id[dt_flows$flow1 %in% non_core_flows] <-
		cluster_id
	dt_flows$processed[dt_flows$flow1 %in% non_core_flows] <-
		TRUE

	# Increment cluster ID for next iteration
	cluster_id <- cluster_id + 1
	print(cluster_id)
}

dt_flows_sel <- select(dt_flows, flow1, cluster_id)
dt_flows_sel$cluster_id %>% table
dt_flows_sel %>% summary
dbWriteTable(conn = con, name = paste0(char_cluster, "_", char_alpha),
						 value = dt_flows_sel, overwrite = TRUE)

psql_add_cluster_id(con = con,
										table_viz = char_viz,
										table_cluster = char_cluster)


################################################################################
# Create shapefiles for each cluster
################################################################################
# Read the data from the database
dt_viz <- st_read(con, char_viz)

# Find unique cluster_ids
int_unique_clusters <- unique(dt_viz$cluster_id_01)

dir.create(here::here(path_output_12, double_alpha), recursive = TRUE, showWarnings = FALSE)
# Loop over each unique cluster_id and write to a shapefile
for(cluster in int_unique_clusters) {
	if(is.na(cluster)){
		next
	}
	print(cluster)
	dt_viz_sub <- dt_viz %>%
		filter(cluster_id == cluster)

	# Create a name for the shapefile
	char_shp_filename <- paste0("cluster_", cluster, ".shp")

	# Write the subset to a shapefile
	st_write(dt_viz_sub, here::here(path_output_12, double_alpha, char_shp_filename))
}

