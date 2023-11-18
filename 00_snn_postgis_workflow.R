################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file calculates the network distances between all OD-pairs
################################################################################













################################################################################
# Python
################################################################################



dt <- RPostgres::dbReadTable(con, "col_2po_4pgr_subset") %>%
	mutate(m = km*1000) %>%
	select(id, source, target, m) 

head(dt)


# install_miniconda()
conda_list(conda = "C:/Users/Seppi/AppData/Local/r-miniconda/_conda.exe")

source_python("src/import_py_dijkstra_functions.py")
g <- from_pandas_edgelist(df = dt, 
										 source = "source", 
										 target = "target",
										 edge_attr = "m",
										 edge_key = "id")

all_to_all_shortest_paths_to_sqldb <- function(con, dt, g, buffer) {
	
	dt_dist_mat <- data.table(
		source = integer(),
		target = integer(),
		m = numeric()
	)
	
	# Create empty table for OD-matrix of local nodes
	if (dbExistsTable(con, "distance_matrix")) {
		dbRemoveTable(con, "distance_matrix")
		dbWriteTable(con, 'distance_matrix', dt_dist_mat)
	} else {
		dbWriteTable(con, 'distance_matrix', dt_dist_mat)
	}
	
	for (source in dt$source){
		# Compute the shortest path and length from the source to every other node
		res = single_source_dijkstra(g, source, weight = "m", cutoff = buffer)
		res = res[[1]] %>% unlist
		
		targets = names(res) %>% as.integer
		sources = rep(source, length(targets))
		distances = res %>% as.numeric
		dt_append <- data.table(
			source = sources,
			target = targets,
			m = distances
		)
		print(dt_append)
		RPostgres::dbAppendTable(conn = con,
														 name = "distance_matrix",
														 value = dt_append
		)
	}
}


