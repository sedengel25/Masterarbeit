################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 12_snn_create_clusters.R
#############################################################################
# Documentation: psql_create_table_directly_reachable_only
# Usage: psql_create_table_directly_reachable_only(table_common_flows, table_common_flows_dr_only)
# Description: Creates a new table with only the directly-reachable-flows
# Args/Options: table_common_flows, table_common_flows_dr_only
# Returns: ...
# Output: ...
# Action: Executing a psql-query
psql_create_table_directly_reachable_only <- function(con,
																											table_common_flows, 
																											table_common_flows_dr_only) {
	psql_drop_table_if_exists(con, table = char_common_flows_dr_only)
	query <- paste0("create table ",
									table_common_flows_dr_only,
									" as select * from ",
									table_common_flows,
									" where directly_reachable = 'yes'")
	
	dbExecute(con, query)
}


# Documentation: psql_get_directly_reachable_flows
# Usage: psql_get_directly_reachable_flows(con, table_common_flows, flow_considered)
# Description: Gets all dr-flows from considered flow(s)
# Args/Options: con, table_common_flows, flow_considered
# Returns: vector of integers
# Output: ...
# Action: Executing a psql-query
psql_get_directly_reachable_flows <- function(con,
																							table_common_flows_dr_only, 
																							flow_considered) {
	query <- paste0("SELECT flow1 AS flow FROM ",
									table_common_flows_dr_only,
									" WHERE flow2 IN (",
									paste(flow_considered, collapse = ", "),
									") UNION
  										SELECT flow2 AS flow
  										FROM ",
									table_common_flows_dr_only,
									" WHERE flow1 IN (",
									paste(flow_considered, collapse = ", "),
									");")
	
	res <- dbSendQuery(con, query)
	directly_reachable_flows <- dbFetch(res) %>% pull %>% as.numeric
	dbClearResult(res)
	return(directly_reachable_flows)
}

# Documentation: psql_add_cluster_id
# Usage: psql_add_cluster_id(con, table_viz, table_cluster)
# Description: Adds cluster_ids to to viz-table
# Args/Options: con, table_viz, table_cluster
# Returns: ...
# Output: ...
# Action: Executes psql-query
psql_add_cluster_id <- function(con, table_viz, table_cluster) {
	
	
	query <- paste0("ALTER TABLE ",
									table_viz,
									" ADD COLUMN cluster_id INTEGER;")
	dbExecute(con, query)
	
	query <- paste0("UPDATE ", table_viz,
									" SET cluster_id = ",
									table_cluster,
									".cluster_id FROM ",
									table_cluster,
									" WHERE ",
									table_viz,
									".id = ",
									table_cluster,
									".flow1;")
	cat(query)
	dbExecute(con, query)
}

# Documentation: get_unprocessed_core_flows
# Usage: get_unprocessed_core_flows(dt_flows)
# Description: Gets all unprocessed core flows
# Args/Options: dt_flows
# Returns: vector of integers
# Output: ...
# Action: ...
get_unprocessed_core_flows <- function(dt_flows) {
	unprocessed_core_flows <- dt_flows %>%
		filter(core_flow == "yes") %>%
		filter(processed == FALSE) %>%
		select(flow1) %>%
		pull
}