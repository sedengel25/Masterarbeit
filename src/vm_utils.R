################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 10_snn_calc_pvalues.R
#############################################################################
psql_get_name_of_geom_col <- function(con, table) {
  query <- paste0("SELECT column_name FROM information_schema.columns
  WHERE table_schema = 'public' AND table_name = '",
                  table,
                  "' AND udt_name = 'geometry';")
  cat(query)
  col_name <- dbGetQuery(con, query) %>% as.character()
  return(col_name)
}

# Documentation: psql_drop_table_if_exists
# Usage: psql_drop_table_if_exists(con, table)
# Description: Drops table if it already exists
# Args/Options: con, table
# Returns: ...
# Output: ...
# Action: Executing a psql-query
psql_drop_table_if_exists <- function(con, table) {
  query <- paste0("DROP TABLE IF EXISTS ", table)
  dbExecute(con, query)
}

# Documentation: psql_create_random_od_points
# Usage: psql_create_random_od_points(con, table_network, table_all_points)
# Description: Creates a table for points every x meter on the network
# Args/Options: con, table_network, table_all_points
# Returns: ...
# Output: ...
# Action: Executing a psql-query
psql_create_random_od_points <- function(con, m, table_network, table_all_points) {
	psql_drop_table_if_exists(con, table = table_all_points)
	# Create points on the network by setting a point each x meter
	query <- paste0("CREATE TABLE ", table_all_points,
									" (
  	id SERIAL PRIMARY KEY,
  	line_id INTEGER,
  	geom GEOMETRY(Point,32632),
  	distance_to_start DOUBLE PRECISION,
  	distance_to_end DOUBLE PRECISION
  );")
	
	dbExecute(con, query)
	
	
	name_geom_col <- psql_get_name_of_geom_col(con, table_network)
	
	query <- paste0("
  INSERT INTO ", table_all_points,
									" (line_id, geom, distance_to_start, distance_to_end)
  SELECT
  l.id as line_id,
  ST_LineInterpolatePoint(l.",
									name_geom_col,
									", series.fraction / l.length) AS geom,
  (series.fraction) AS distance_to_start,
  (l.length - series.fraction) AS distance_to_end
  FROM
  (SELECT id, ",
									name_geom_col,
									", ST_Length(",
									name_geom_col,
									") as length
  	FROM ",
									table_network,
									" WHERE ST_GeometryType(",
									name_geom_col,
									") = 'ST_LineString') l
  CROSS JOIN LATERAL
  generate_series(0, cast(l.length as integer), ", m, ") AS series(fraction)
  WHERE
  series.fraction / l.length <= 1;")
	dbExecute(con, query)
}


# Documentation: psql_create_table_random_od_points
# Usage: psql_create_table_random_od_points(con, 
# table_random_od_points, table_all_points,random_indices)
# Description: Creates a table for the random OD-points
# Args/Options: con, table_random_od_points, table_all_points,random_indices
# Returns: ...
# Output: ...
# Action: Executing a psql-query
psql_create_table_random_od_points <- function(con, 
																							 table_random_od_points, 
																							 table_all_points,
																							 random_indices) {
	query <- paste0("DROP TABLE IF EXISTS ", table_random_od_points)
	dbExecute(con, query)
	
	query <- paste0("CREATE TABLE ", table_random_od_points,
									" (
  	id SERIAL PRIMARY KEY,
  	line_id integer,
  	geom GEOMETRY(Point,32632),
  	distance_to_start DOUBLE PRECISION,
  	distance_to_end DOUBLE PRECISION
  );")
	dbExecute(con, query)
	
	query <- paste0("INSERT INTO ", 
									table_random_od_points,
									" (geom, line_id, distance_to_start, distance_to_end)
  SELECT geom, line_id, distance_to_start, distance_to_end
  FROM ", 
									table_all_points,
									" WHERE id IN (", 
									paste(random_indices, collapse = ", "), ");")
	dbExecute(con, query)
}


# Documentation: psql_compare_directly_reachable
# Usage: psql_compare_directly_reachable(con, table_reachable_flows, 
# table_random_reachable_flows,table_reachable_flows_compared)
# Description: Creates a table for the random OD-points
# Args/Options: con, table_reachable_flows, table_random_reachable_flows,
# table_reachable_flows_compared
# Returns: ...
# Output: ...
# Action: Executing a psql-query
psql_compare_directly_reachable <- function(con,
																						table_reachable_flows,
																						table_random_reachable_flows,
																						table_reachable_flows_compared) {
	query <- paste0("
  UPDATE ", table_reachable_flows_compared,
									" SET density = ", table_reachable_flows_compared,".density +
  	CASE
  WHEN b.reachable_count IS NULL THEN 0
  WHEN b.reachable_count >= COALESCE(a.reachable_count, 0) THEN 1
  ELSE 0
  END
  FROM
  (SELECT flow1, reachable_count FROM ", table_reachable_flows, ") AS a
  FULL OUTER JOIN
  (SELECT flow1, reachable_count FROM ", table_random_reachable_flows, ") AS b ON a.flow1 = b.flow1
  WHERE ",
									table_reachable_flows_compared, ".flow1 = COALESCE(a.flow1, b.flow1);")
	dbExecute(con, query)
}

# Documentation: psql_get_number_of_common_flows
# Usage: psql_get_number_of_common_flows(con, table_k_nearest_flows, table_common_flows)
# Description: Get number of common flows
# Args/Options: con, table_k_nearest_flows, table_common_flows
# Returns: ...
# Output: ...
# Action: psql-query
psql_get_number_of_common_flows <- function(con, table_k_nearest_flows, table_common_flows) {
	# table_k_nearest_flows = char_random_k_nearest_flows
	# table_common_flows = char_random_common_flows
	query <- paste0("DROP TABLE IF EXISTS ", table_common_flows)
	dbExecute(con, query)
	
	query <- paste0("create table ", table_common_flows, " as
  WITH ClosestFlows AS (
    SELECT flow_ref, ARRAY_AGG(flow_other ORDER BY nd) AS closest_flows
    FROM ", table_k_nearest_flows, "
    GROUP BY flow_ref
  )
  SELECT 
    f1.flow_ref AS flow1, 
    f2.flow_ref AS flow2, 
    cardinality(
      ARRAY(
        SELECT unnest(f1.closest_flows) 
        INTERSECT 
        SELECT unnest(f2.closest_flows)
      )
    ) AS common_flows
  FROM ClosestFlows f1
  CROSS JOIN ClosestFlows f2
  WHERE f1.flow_ref < f2.flow_ref;")
	dbExecute(con, query)
}


# Documentation: psql_get_number_directly_reachable_flows
# Usage: psql_get_number_directly_reachable_flows(con, k, table_common_flows, 
# table_reachable_flows)
# Description: Get number of directly reachable flows
# Args/Options: con, k, table_common_flows, 
# table_reachable_flows
# Returns: ...
# Output: ...
# Action: psql-query
psql_get_number_directly_reachable_flows <- function(con, k, table_common_flows, 
																										 table_reachable_flows) {
	# Create the column 'directly_reachable'
	query <- paste0("ALTER TABLE ",  table_common_flows,
									" ADD COLUMN directly_reachable VARCHAR(255);")
	dbExecute(con, query)
	
	query <- paste0("UPDATE ", table_common_flows,
									" SET directly_reachable = CASE
  WHEN common_flows > ", k/2, " THEN 'yes' ELSE 'no' END;")
	dbExecute(con, query)

	
	query <- paste0("DROP TABLE IF EXISTS ", table_reachable_flows)
	dbExecute(con, query)
	# Count the number of directly reachable flows
	query <- paste0("CREATE TABLE ", table_reachable_flows,
									" AS SELECT flow1, COUNT(*) as reachable_count FROM ",
									table_common_flows,
									" WHERE directly_reachable = 'yes' 
  GROUP BY flow1;")
	dbExecute(con, query)
}

# Documentation: psql_create_index
# Usage: psql_create_index(con, char_table, col)
# Description: Creates a index for chosen table on chosen column
# Args/Options: con, char_table, col
# Returns: ...
# Output: ...
# Action: psql-query
psql_create_index <- function(con, table, col) {
	
	char_idx <- paste0("idx_", table)
	
	query <- paste0("DROP INDEX IF EXISTS ", char_idx)
	dbExecute(con, query)
	
	query <- paste0("CREATE INDEX ", 
									char_idx,
									" ON ",  
									table, 
									" USING btree (", 
									paste(col, collapse = ", "), 
									");")
	
	dbExecute(con, query)
}



# Documentation: psql_get_k_nearest_flows
# Usage: psql_get_k_nearest_flows(con, k, table_flows, table_k_nearest_flows)
# Description: Gets k nearest flows
# Args/Options: con, k, table_flows, table_k_nearest_flows
# Returns: ...
# Output: ...
# Action: psql-query
psql_get_k_nearest_flows <- function(con, k, table_flows, table_k_nearest_flows) {
	
	query <- paste0("DROP TABLE IF EXISTS ", table_k_nearest_flows)
	dbExecute(con, query)
	
	query <- paste0("create table ", table_k_nearest_flows, " as
  								WITH SymmetricFlows AS (
  									SELECT flow_m AS flow_ref, flow_n AS flow_other, nd FROM ",
									table_flows, "
  									UNION
  									SELECT flow_n AS flow_ref, flow_m AS flow_other, nd FROM ",
									table_flows, "
  								),
  								RankedFlows AS (
  									SELECT
  									flow_ref,
  									flow_other,
  									nd,
  									ROW_NUMBER() OVER (PARTITION BY flow_ref ORDER BY nd ASC) as rn
  									FROM SymmetricFlows
  								)
  								SELECT
  								flow_ref,
  								flow_other,
  								nd
  								FROM
  								RankedFlows
  								WHERE
  								rn <= ", k, ";")
	dbExecute(con, query)
}



# Documentation: psql_calc_nd
# Usage: psql_calc_nd(con, char_mapped_points, char_osm2po_subset, char_dist_mat)
# Description: Execute query calculating the NDs between all Os and Ds
# Args/Options: con, char_mapped_points, char_osm2po_subset, char_dist_mat
# Returns: ...
# Output: ...
# Action: Executing a psql-query
psql_calc_nd <- function(con, table_mapped_points, 
												 table_network, 
												 table_dist_mat, table_nd) {
	# table_mapped_points <- char_random_o_points
	# table_network <- char_osm2po_subset
	# table_dist_mat <- char_dist_mat_red_no_dup
	# table_nd <- char_random_o_nd
	query <- paste0("DROP TABLE IF EXISTS ", table_nd)
	dbExecute(con, query)
	
	query <- paste0("CREATE TABLE ", table_nd ," AS SELECT m1.id as o_m, 
  m2.id as o_n,
  LEAST(
  	m1.distance_to_start + m2.distance_to_start + COALESCE(pi_pk.m, 0),
  	m1.distance_to_end + m2.distance_to_end + COALESCE(pj_pl.m, 0),
  	m1.distance_to_start + m2.distance_to_end + COALESCE(pi_pl.m, 0),
  	m1.distance_to_end + m2.distance_to_start + COALESCE(pj_pk.m, 0)
  ) AS nd
  FROM ", table_mapped_points, " m1
  CROSS JOIN ", table_mapped_points, " m2
  INNER JOIN ", table_network, " e_ij ON m1.line_id = e_ij.id
  INNER JOIN ", table_network, " e_kl ON m2.line_id  = e_kl.id
  INNER JOIN ", table_dist_mat, " pi_pk ON pi_pk.source = LEAST(e_ij.source, e_kl.source) AND pi_pk.target = GREATEST(e_ij.source, e_kl.source)
  INNER JOIN ", table_dist_mat, " pj_pl ON pj_pl.source = LEAST(e_ij.target, e_kl.target) AND pj_pl.target = GREATEST(e_ij.target, e_kl.target)
  INNER JOIN ", table_dist_mat, " pi_pl ON pi_pl.source = LEAST(e_ij.source, e_kl.target) AND pi_pl.target = GREATEST(e_ij.source, e_kl.target)
  INNER JOIN ", table_dist_mat, " pj_pk ON pj_pk.source = LEAST(e_ij.target, e_kl.source) AND pj_pk.target = GREATEST(e_ij.target, e_kl.source)
  where m1.id < m2.id;")
	dbExecute(con, query)
}



# Documentation: psql_calc_flow_nds
# Usage: psql_calc_flow_nds(con, table_o_nds, table_d_nds, table_flow_nds)
# Description: Execute query calculating the NDs between all flows
# Args/Options: con, table_o_nds, table_d_nds, table_flow_nds
# Returns: ...
# Output: ...
# Action: Executing a psql-query
psql_calc_flow_nds <- function(con, table_o_nds, table_d_nds, table_flow_nds) {
	query <- paste0("DROP TABLE IF EXISTS ", table_flow_nds)
	dbExecute(con, query)
	
	query <- paste0("create table ",
									table_flow_nds,
									" as select o_points.o_m  as flow_m, o_points.o_n  as flow_n, 
  o_points.nd + d_points.nd as nd from ", 
									table_o_nds,
									" o_points inner join ", 
									table_d_nds, 
									" d_points on o_points.o_m = d_points.d_m and o_points.o_n = d_points.d_n;")

	dbExecute(con, query)
}




# Documentation: psql_get_max_of_num_col
# Usage: psql_get_max_of_num_col(con, num_col, table)
# Description: Get max value of numerical column of certain table
# Args/Options: con, num_col, table
# Returns: ...
# Output: ...
# Action: Executing a psql-query
psql_get_max_of_num_col <- function(con, num_col, table) {
	query <- paste0("SELECT MAX(", num_col, ") FROM ",
									table, ";")
	
	result <- dbSendQuery(con, query)
	int_max <- dbFetch(result) %>% as.numeric
	return(int_max)
}



# Documentation: helper_function_common_flows
# Usage: helper_function_common_flows(i, thresh.common)
# Description: Calculates common flows for each possible flow-pair
# Args/Options: i, thresh.common
# Returns: data.table
# Output: ...
# Action: ...
helper_function_common_flows <- function(i, thresh.common = 1L) {
	flow.i = dt_knn_rand_split[[i]]
	# compute amount of common flows per flow pair
	common.flows = lapply((i + 1L):int_n_ids, function(j) {
		n.common = length(intersect(flow.i, dt_knn_rand_split[[j]]))
		if (n.common >= thresh.common) {
			return(c(dt_knn_rand_flow_ids[i], dt_knn_rand_flow_ids[j], n.common))
		} else {
			return(NULL)
		}
	})

	# get rid of empty pairs
	cf = unlist(common.flows)
	lcf = length(cf)
	
	# if a flow pair exists return it, otherwise return NULL
	if (lcf > 0L) {
		cf = matrix(cf, ncol = 3L, byrow = TRUE)
		return(cf)
	} else {
		return(NULL)
	}
}


# Documentation: helper_function_calc_nd
# Usage: helper_function_calc_nd(start_id, end_id, conn_info)
# Description: Calculates NDs between O- & D-points using parallel connections
# Args/Options: start_id, end_id, conn_info
# Returns: ...
# Output: ...
# Action: Executes psql-statement
helper_function_calc_nd <- function(table_points, table_network, table_dist_mat,
																		table_nd, start_id, end_id) {
	
	con <- dbConnect(RPostgres::Postgres(),
									 dbname = dbname,
									 host = host,
									 port = port,
									 user = user,
									 password = pw)
	
	
	range_str <- paste0("generate_series(", start_id, ",", end_id, ")")
	
	
	# query <- "set enable_seqscan='off'"
	# dbExecute(con, query)
	
	query <- paste0("DO $$
	DECLARE
	    m1_id INTEGER;
	BEGIN
	    FOR m1_id IN SELECT ", range_str, "
	    LOOP
	        INSERT INTO ", table_nd, "(o_m, o_n, nd)
				  SELECT m1.id as o_m, 
				           m2.id as o_n,
					        CAST(
  					        LEAST(
  				  	m1.distance_to_start + m2.distance_to_start + COALESCE(pi_pk.m, 0),
  				  	m1.distance_to_end + m2.distance_to_end + COALESCE(pj_pl.m, 0),
  				  	m1.distance_to_start + m2.distance_to_end + COALESCE(pi_pl.m, 0),
  				  	m1.distance_to_end + m2.distance_to_start + COALESCE(pj_pk.m, 0)
  				  ) AS INTEGER
  				) AS nd
				  FROM ", table_points, " m1
				  CROSS JOIN ", table_points, " m2
				  INNER JOIN ", table_network, " e_ij ON m1.line_id = e_ij.id
				  INNER JOIN ", table_network, " e_kl ON m2.line_id  = e_kl.id
				  INNER JOIN ", table_dist_mat, " pi_pk ON pi_pk.source =
				  LEAST(e_ij.source, e_kl.source) AND pi_pk.target = 
				  GREATEST(e_ij.source, e_kl.source)
				  INNER JOIN ", table_dist_mat, " pj_pl ON pj_pl.source = 
				  LEAST(e_ij.target, e_kl.target) AND pj_pl.target = 
				  GREATEST(e_ij.target, e_kl.target)
				  INNER JOIN ", table_dist_mat, " pi_pl ON pi_pl.source = 
				  LEAST(e_ij.source, e_kl.target) AND pi_pl.target = 
				  GREATEST(e_ij.source, e_kl.target)
				  INNER JOIN ", table_dist_mat, " pj_pk ON pj_pk.source = 
				  LEAST(e_ij.target, e_kl.source) AND pj_pk.target = 
				  GREATEST(e_ij.target, e_kl.source)
	        WHERE m1.id = m1_id and m1.id < m2.id;
	
	        --RAISE NOTICE 'Verarbeitung abgeschlossen für m1_id: %', m1_id;
	    END LOOP;
	END $$;")
	DBI::dbExecute(con, query)
	DBI::dbDisconnect(con)
}


# Documentation: psql_calc_and_create_nd_tables
# Usage: psql_calc_and_create_nd_tables(con, table_points, table_nd)
# Description: Drops ND-table and creates new
# Args/Options: con, table_points, table_nd
# Returns: ...
# Output: ...
# Action: Executes psql-statement
psql_parallel_calc_and_create_nd_tables <- function(con, table_points, table_nd) {
  psql_drop_table_if_exists(con, table = table_nd)
  
  query <- paste0("CREATE UNLOGGED TABLE ", table_nd,
  								" (o_m INTEGER,
  										 o_n INTEGER,
  										 nd INTEGER);")
  dbExecute(con, query)
  
  
  int_max_id <- RPostgres::dbReadTable(con, table_points) %>%
  	select(id) %>%
  	max()

  int_id_range <- 1:int_max_id
  int_range_size <- ceiling(length(int_id_range) / int_numb_cores)
  int_id_chunks <-
  	split(int_id_range, ceiling(seq_along(int_id_range) / int_range_size))
  
  # plan(multisession, workers = int_numb_cores) 
  # t1 <- proc.time()
  # furrr::future_map(id_ranges, function(range) {
  # 	run_procedure_for_range(min(range), max(range), conn_info)
  # })
  testlist <- parallel::mclapply(int_id_chunks, function(range){
  	helper_function_calc_nd(table_points = table_points,
  													table_network = char_osm2po_subset,
  													table_dist_mat = char_dist_mat,
  													table_nd = table_nd,
  													start_id = min(range),
  													end_id = max(range))
    
  }, mc.cores = int_numb_cores)
  # furrr::future_map(id_ranges, function(range) {
  #   run_procedure_for_range(min(range), max(range), conn_info)
  # })	
  # t2 <- proc.time()
  # t2-t1
}