################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file creates clusters based on the p-values of the flows
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/00_config_psql.R")
# source("./src/12_utils.R")

char_city_prefix <- "col"
char_flows_pvalues <- paste0(char_city_prefix, "_flows_pvalues")
char_random_common_flows <- paste0(char_city_prefix, "_random_common_flows")
dt_flows <- RPostgres::dbReadTable(con, char_flows_pvalues)

# Add a column for cluster IDs and a flag for processed flows
dt_flows$cluster_id <- NA
dt_flows$processed <- FALSE

cluster_id <- 1

while (any(dt_flows$core_flow == 'yes' &
					 !dt_flows$processed)) {
	# Select a random unprocessed core flow
	unprocessed_core_flows <-
		dt_flows$flow1[dt_flows$core_flow == 'yes' &
															!dt_flows$processed]
	
	if (length(unprocessed_core_flows) == 0){
		break
	}

	selected_flow <- sample(unprocessed_core_flows, 1)
	dt_flows$cluster_id[dt_flows$flow1 == selected_flow] <-
		cluster_id
	dt_flows$processed[dt_flows$flow1 == selected_flow] <-
		TRUE
	
	# Find directly reachable core flows
	repeat {
		cat("selected_flows: ", selected_flow, "\n")
		query <- paste0("SELECT flow1 AS flow FROM ",
										char_random_common_flows,
										" WHERE directly_reachable = 'yes' AND flow2 IN (",
										paste(selected_flow, collapse = ", "),
										") UNION
										SELECT flow2 AS flow
										FROM col_common_flows
										WHERE directly_reachable = 'yes' AND flow1 IN (",
										paste(selected_flow, collapse = ", "),
										");")

		res <- dbSendQuery(con, query)
		directly_reachable_flows <- dbFetch(res) %>% pull %>% as.numeric
		
		directly_reachable_cores <-
			directly_reachable_flows[dt_flows$flow1[directly_reachable_flows] %in% 
															 	unprocessed_core_flows]
		
		if (length(directly_reachable_cores) == 0)
			break
		
		dt_flows$cluster_id[dt_flows$flow1 %in% directly_reachable_cores] <-
			cluster_id
		dt_flows$processed[dt_flows$flow1 %in% directly_reachable_cores] <-
			TRUE
		
		# Update the selected flow for the next iteration
		selected_flow <- directly_reachable_cores
		unprocessed_core_flows <-
			dt_flows$flow1[dt_flows$core_flow == 'yes' &
																!dt_flows$processed]
		cat("length unprocessed core flows: ", 
				length(unprocessed_core_flows), "\n")
	}

	cluster_core_flows <- dt_flows$flow1[which(dt_flows$cluster_id == cluster_id)]
	# Add non-core flows that are directly reachable from core flows in this cluster
	query <- paste0("SELECT flow1 AS flow FROM ",
									char_random_common_flows,
									" WHERE directly_reachable = 'yes' AND flow2 IN (",
									paste(cluster_core_flows, collapse = ", "),
									") UNION
									SELECT flow2 AS flow
									FROM col_common_flows
									WHERE directly_reachable = 'yes' AND flow1 IN (",
									paste(cluster_core_flows, collapse = ", "),
									");")
	
	res <- dbSendQuery(con, query)
	directly_reachable_non_cores <- dbFetch(res) %>% pull %>% as.numeric
	
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


