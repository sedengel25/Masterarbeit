################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions that are needed over and over again
################################################################################
# Documentation: sample_subset
# Usage: sample_subset(dt)
# Description: Takes a subset of dt based on a random selection dep. on size
# Args/Options: dt
# Returns: datatable
# Output: ...
sample_subset <- function(dt, size) {
	vector_rand_inx <- sample(1:nrow(dt), size = size)
	
	dt_sub <- dt %>%
		slice(vector_rand_inx)
	
	return(dt_sub)
}


# Documentation: shp_network_to_linestrings
# Usage: shp_network_to_linestrings(shp_file)
# Description: Takes a shp-streetnetwork and returns all linestrings contained
# Args/Options: shp_file
# Returns: sf-linestrings
# Output: ...
shp_network_to_linestrings <- function(shp_file) {
	shp <- st_read(shp_file)
	sf_road_segments_mls <- shp$geometry
	list_sf_road_segments_ls <- split_multiline_in_line(sf_road_segments_mls)
	sf_ls <- st_sfc(list_sf_road_segments_ls)
	
	return(sf_ls)
}


# Documentation: split_multiline_in_line
# Usage: split_multiline_in_line(sf_mls)
# Description: Splits sf-multilinestrigns into sf-linestrings
# Args/Options: sf_mls
# Returns: list
# Output: ...
split_multiline_in_line <- function(sf_mls) {
	list_of_ls <- list()
	idx <- 1
	for(i in 1:length(sf_mls)){
		for(j in 1:length(st_cast(sf_mls[i], "LINESTRING"))){
			ls <- st_cast(sf_mls[i], "LINESTRING")[j] 
			list_of_ls[idx] <- ls
			idx <- idx + 1
		}
	}
	
	return(list_of_ls)
}



# Documentation: get_cleaned_trip_data
# Usage: get_cleaned_trip_data(path_clean, path_org)
# Description: Combines the dt with ride & id of clean trips with org. data
# Args/Options: path_clean, path_org
# Returns: dt
# Output: ...
# Action: ...
get_cleaned_trip_data <- function(path_clean, path_org) {
	# Read in clean scooter trip data (only 'id' and 'ride)
	dt_clean <- read_rds(path_clean)
	
	# Read in full original data
	dt_org <- read_rds(path_org)
	
	# Merge at 'id' and 'ride'
	dt <- dt_clean %>%
		inner_join(dt_org, c("id", "ride"))
	
	return(dt)
}



# Documentation: transform_num_to_WGS84
# Usage: transform_num_to_WGS84(dt, coords)
# Description: Transforms num-coords of dt into wgs84
# Args/Options: dt, coords
# Returns: sf_object
# Output: ...
# Action: ...
transform_num_to_WGS84 <- function(dt, coords) {
	sp_points <- SpatialPoints(coords = dt[,..coords],
														 proj4string = CRS("+proj=longlat +datum=WGS84"))
	
	# Transform to UTM Zone 32N
	# sp_utm32 <- spTransform(sp_points, CRS("+proj=utm +zone=32 +datum=WGS84"))
	
	# Convert back to an sf object
	sf_points <- st_as_sf(sp_points)
	
	
	return(sf_points)
}

# Documentation: connect_to_postgresql_db
# Usage: connect_to_postgresql_db(user, pw, dbname, host)
# Description: Creates connection to PostgreSQL-DB
# Args/Options: user, pw, dbname, host
# Returns: DB-Connection
# Output: ...
# Action: ...
connect_to_postgresql_db <- function(user, pw, dbname, host) {
	# Create a connection to the PostgreSQL database
	drv <- RPostgres::Postgres()
	con <- dbConnect(drv, 
									 dbname = dbname, 
									 host = host, 
									 user = user, 
									 password = pw,
									 port = 5432)
	return(con)
}



# Documentation: psql_drop_old_if_new_exists
# Usage: psql_drop_old_if_new_exists(con, old_table, new_table)
# Description: Drops old table if new adjusted table exists
# Args/Options: con, old_table, new_table
# Returns: ...
# Output: ...
# Action: psql-query
psql_drop_old_if_new_exists <- function(con, old_table, new_table) {
	query <- paste0("
		DO $$
		BEGIN
		  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' 
		  AND tablename = '", new_table, "') THEN
		    DROP TABLE IF EXISTS ", old_table, ";
		  END IF;
		END
		$$;")
	dbExecute(con, query)
}


# Documentation: psql_remove_duplicates
# Usage: psql_remove_duplicates(con, old_table, new_table, col)
# Description: Creates new table without duplicates based on old table
# Args/Options: con, old_table, new_table, col
# Returns: ...
# Output: ...
# Action: psql-query
psql_remove_duplicates <- function(con, old_table, new_table, col) {
	query <- paste0("DROP TABLE IF EXISTS ", new_table)
	dbExecute(con, query)
	
	query <- paste0(
		"CREATE TABLE ", new_table,
		" AS SELECT DISTINCT ON (", paste(col, collapse = ", "), ") * FROM ", 
		old_table,
		";"
	)
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


# Documentation: psql_create_index
# Usage: psql_create_index(con, char_table, col)
# Description: Creates a index for chosen table on chosen column
# Args/Options: con, char_table, col
# Returns: ...
# Output: ...
# Action: psql-query
psql_create_spatial_index <- function(con, table) {
	
	char_idx <- paste0(table, "_geometry_idx")
	
	query <- paste0("DROP INDEX IF EXISTS ", char_idx)
	dbExecute(con, query)
	
	
	name_geom_col <- psql_get_name_of_geom_col(con, table)
	query <- paste0("CREATE INDEX ON ",  table, " USING GIST (",
									name_geom_col,");")
	dbExecute(con, query)
}


# Documentation: psql_where_source_smaller_target
# Usage: psql_where_source_smaller_target(con, old_table, new_table)
# Description: Creates new table only with rows where source < target
# Args/Options: con, old_table, new_table
# Returns: ...
# Output: ...
# Action: psql-query
psql_where_source_smaller_target <- function(con, old_table, new_table) {
	query <- paste0("CREATE TABLE ", new_table,
									" AS SELECT * FROM ", 
									old_table, 
									" WHERE source < target;")
	
	dbExecute(con, query)
}



# Documentation: psql_check_indexes
# Usage: psql_check_indexes(con, char_table)
# Description: Returns indexes of table
# Args/Options: con, char_table
# Returns: ...
# Output: ...
# Action: psql-query
psql_check_indexes <- function(con, char_table) {
	query <- paste0("SELECT
  tablename,
  indexname,
  indexdef
  FROM
  pg_indexes
  WHERE
  tablename = '", char_table,"';")
	
	result <- dbSendQuery(con, query)
	data <- dbFetch(result)
	return(data)
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


# Documentation: psql_get_number_of_common_flows
# Usage: psql_get_number_of_common_flows(con, table_k_nearest_flows, table_common_flows)
# Description: Get number of common flows
# Args/Options: con, table_k_nearest_flows, table_common_flows
# Returns: ...
# Output: ...
# Action: psql-query
psql_get_number_of_common_flows <- function(con, table_k_nearest_flows, table_common_flows) {

	query <- paste0("DROP TABLE IF EXISTS ", table_common_flows)
	dbExecute(con, query)
	
	query <- pste0("CREATE TEMP TABLE TempClosestFlows AS
		SELECT flow_ref, ARRAY_AGG(flow_other) AS closest_flows
		FROM ",
	  table_k_nearest_flows,
	  " GROUP BY flow_ref;")
	dbExecute(con, query)
	
	query <- paste0("CREATE TABLE ",
		table_common_flows,
		" AS WITH Precomputed AS (
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
		FROM TempClosestFlows f1
		CROSS JOIN TempClosestFlows f2
		WHERE f1.flow_ref < f2.flow_ref
	)
	SELECT *
		FROM Precomputed
	WHERE common_flows > ", int_k/2, ";")
	dbExecute(con, query)
	
	query <- paste0("DROP TABLE TempClosestFlows;")
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

	# Count the number of directly reachable flows
	query <- paste0("CREATE TABLE ", table_reachable_flows,
									" AS SELECT flow1, COUNT(*) as reachable_count FROM ",
									table_common_flows,
									" WHERE directly_reachable = 'yes' 
  GROUP BY flow1;")
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
  ,
  CASE 
    WHEN m1.distance_to_start + m2.distance_to_start + COALESCE(pi_pk.m, 0) = 
         LEAST(
           m1.distance_to_start + m2.distance_to_start + COALESCE(pi_pk.m, 0),
           m1.distance_to_end + m2.distance_to_end + COALESCE(pj_pl.m, 0),
           m1.distance_to_start + m2.distance_to_end + COALESCE(pi_pl.m, 0),
           m1.distance_to_end + m2.distance_to_start + COALESCE(pj_pk.m, 0)
         ) THEN pi_pk.source
    WHEN m1.distance_to_end + m2.distance_to_end + COALESCE(pj_pl.m, 0) = 
         LEAST(
           m1.distance_to_start + m2.distance_to_start + COALESCE(pi_pk.m, 0),
           m1.distance_to_end + m2.distance_to_end + COALESCE(pj_pl.m, 0),
           m1.distance_to_start + m2.distance_to_end + COALESCE(pi_pl.m, 0),
           m1.distance_to_end + m2.distance_to_start + COALESCE(pj_pk.m, 0)
         ) THEN pj_pl.source
    WHEN m1.distance_to_start + m2.distance_to_end + COALESCE(pi_pl.m, 0) = 
         LEAST(
           m1.distance_to_start + m2.distance_to_start + COALESCE(pi_pk.m, 0),
           m1.distance_to_end + m2.distance_to_end + COALESCE(pj_pl.m, 0),
           m1.distance_to_start + m2.distance_to_end + COALESCE(pi_pl.m, 0),
           m1.distance_to_end + m2.distance_to_start + COALESCE(pj_pk.m, 0)
         ) THEN pi_pl.source
    ELSE pj_pk.source
  END AS shortest_path_source,
  CASE 
    WHEN m1.distance_to_start + m2.distance_to_start + COALESCE(pi_pk.m, 0) = 
         LEAST(
           m1.distance_to_start + m2.distance_to_start + COALESCE(pi_pk.m, 0),
           m1.distance_to_end + m2.distance_to_end + COALESCE(pj_pl.m, 0),
           m1.distance_to_start + m2.distance_to_end + COALESCE(pi_pl.m, 0),
           m1.distance_to_end + m2.distance_to_start + COALESCE(pj_pk.m, 0)
         ) THEN pi_pk.target
    WHEN m1.distance_to_end + m2.distance_to_end + COALESCE(pj_pl.m, 0) = 
         LEAST(
           m1.distance_to_start + m2.distance_to_start + COALESCE(pi_pk.m, 0),
           m1.distance_to_end + m2.distance_to_end + COALESCE(pj_pl.m, 0),
           m1.distance_to_start + m2.distance_to_end + COALESCE(pi_pl.m, 0),
           m1.distance_to_end + m2.distance_to_start + COALESCE(pj_pk.m, 0)
         ) THEN pj_pl.target
    WHEN m1.distance_to_start + m2.distance_to_end + COALESCE(pi_pl.m, 0) = 
         LEAST(
           m1.distance_to_start + m2.distance_to_start + COALESCE(pi_pk.m, 0),
           m1.distance_to_end + m2.distance_to_end + COALESCE(pj_pl.m, 0),
           m1.distance_to_start + m2.distance_to_end + COALESCE(pi_pl.m, 0),
           m1.distance_to_end + m2.distance_to_start + COALESCE(pj_pk.m, 0)
         ) THEN pi_pl.target
    ELSE pj_pk.target
  END AS shortest_path_target

  FROM ", table_mapped_points, " m1
  CROSS JOIN ", table_mapped_points, " m2
  INNER JOIN ", table_network, " e_ij ON m1.line_id = e_ij.id
  INNER JOIN ", table_network, " e_kl ON m2.line_id  = e_kl.id
  INNER JOIN ", table_dist_mat, " pi_pk ON pi_pk.source = LEAST(e_ij.source, e_kl.source) AND pi_pk.target = GREATEST(e_ij.source, e_kl.source)
  INNER JOIN ", table_dist_mat, " pj_pl ON pj_pl.source = LEAST(e_ij.target, e_kl.target) AND pj_pl.target = GREATEST(e_ij.target, e_kl.target)
  INNER JOIN ", table_dist_mat, " pi_pl ON pi_pl.source = LEAST(e_ij.source, e_kl.target) AND pi_pl.target = GREATEST(e_ij.source, e_kl.target)
  INNER JOIN ", table_dist_mat, " pj_pk ON pj_pk.source = LEAST(e_ij.target, e_kl.source) AND pj_pk.target = GREATEST(e_ij.target, e_kl.source)
  where m1.id < m2.id;")
	cat(query)
	dbExecute(con, query)
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


# Documentation: psql_count_rows
# Usage: psql_count_rows(con, table)
# Description: Get number of rows of a table
# Args/Options: con, table
# Returns: ...
# Output: ...
# Action: Executing a psql-query
psql_count_rows <- function(con, table) {
	query <- paste0("SELECT COUNT(*) FROM ", table)
	res <- dbSendQuery(con, query)
	data <- dbFetch(res)
	int_count <- data %>% pull %>% as.numeric
	return(int_count)
}
# Documentation: psql_get_srid
# Usage: psql_get_srid(con, table)
# Description: Get SRID of psql-table
# Args/Options: con, table
# Returns: ...
# Output: ...
# Action: Execute psql-query 
psql_get_srid <- function(con, table) {
	name_geom_col <- psql_get_name_of_geom_col(con, table)
	query <- paste0("SELECT ST_SRID(", name_geom_col, ") FROM ", table, " LIMIT 1;")
	srid <- dbGetQuery(con, query) %>% as.integer
	return(srid)
}
# Documentation: psql_set_srid
# Usage: psql_set_srid(con, table)
# Description: Set SRID of psql-table
# Args/Options: con, table
# Returns: ...
# Output: ...
# Action: Execute psql-query 
psql_set_srid <- function(con, table, srid) {
	name_geom_col <- psql_get_name_of_geom_col(con, table)
	query <- paste0("UPDATE ",
									table,
									" SET ",
									name_geom_col,
									" = ST_SetSRID(",
									name_geom_col,
									",",
									srid,
									");")
	dbExecute(con, query)
}

# Documentation: psql_get_name_of_geom_col
# Usage: psql_get_name_of_geom_col(con, table)
# Description: Get name of the geometry column
# Args/Options: con, table
# Returns: ...
# Output: ...
# Action: Execute psql-query 
psql_get_name_of_geom_col <- function(con, table) {
	query <- paste0("SELECT column_name
  FROM information_schema.columns
  WHERE table_schema = 'public' AND table_name = '",
									table,
									"' AND udt_name = 'geometry';")
	
	col_name <- dbGetQuery(con, query) %>% as.character()
	return(col_name)
}

# Documentation: psql_update_srid
# Usage: psql_update_srid(con, table, crs)
# Description: Update crs of geom-colun of chosen table
# Args/Options: con, table, crs
# Returns: ...
# Output: ...
# Action: Executing a psql-query
psql_update_srid <- function(con, table, crs) {
	name_geom_col <- psql_get_name_of_geom_col(con, table)
	query <- paste0("SELECT UpdateGeometrySRID('",
									table,
									"','",
									name_geom_col,
									"',",
									crs,
									");")
	dbExecute(con, query)
}


# Documentation: psql_rename_table
# Usage: psql_rename_table(con, table_old_name, table_new_name)
# Description: Renames table
# Args/Options: con, table_old_name, table_new_name
# Returns: ...
# Output: ...
# Action: Executing a psql-query
psql_rename_table <- function(con, table_old_name, table_new_name){
	query <- paste0("ALTER TABLE ", table_old_name,
									" RENAME TO ",table_new_name, ";")
	dbExecute(con, query)
}


# Documentation: cmd_write_sql_file
# Usage: cmd_write_sql_file(char_cmd_psql_shp_to_sql)
# Description: Writes sql file based on temp batch-file
# Args/Options: char_cmd_psql_shp_to_sql
# Returns: ...
# Output: ...
# Action: Executing a cmd-statement to run a sql-file
cmd_write_sql_file <- function(char_cmd_psql_shp_to_sql) {
	temp_batch_file <- tempfile(pattern = "command", fileext = ".bat")
	writeLines(char_cmd_psql_shp_to_sql,
						 temp_batch_file)
	system(paste("cmd /c", temp_batch_file))
}

# Documentation: cmd_execute_sql_file
# Usage: cmd_execute_sql_file(path_to_sql_file)
# Description: Runs sql file
# Args/Options: path_to_sql_file
# Returns: ...
# Output: ...
# Action: Executing a cmd-statement to run a sql-file
cmd_execute_sql_file <- function(path_to_sql_file) {
	Sys.setenv(PGPASSWORD = pw)
	cmd_psql <- sprintf('"%s" -h %s -p %s -d %s -U %s -f "%s"', 
											file_exe_psql, host, port, dbname, user, path_to_sql_file)
	
	system(cmd_psql)
	Sys.unsetenv("PGPASSWORD")
}



# Documentation: get_packages_used
# Usage: get_packages_used(file)
# Description: Analyszes the functions used in the provided file
# Args/Options: file
# Returns: dataframe
# Output: ...
# Action: ...
get_packages_used <- function(file) {
	list_packages <- list.functions.in.file(file, 
																					alphabetic = TRUE) 
	
	list_packages_long <- lapply(names(list_packages), function(x) {
		data.frame(Paket = x, Funktion = list_packages[[x]], stringsAsFactors = FALSE)
	})
	
	df <- do.call(rbind, list_packages_long)
	
	return(df)
}


# Documentation: cmd_write_sql_dump
# Usage: cmd_write_sql_dump(table, data_sub_folder)
# Description: Creates sql-dump of table and writes to dockerbuild
# Args/Options: table, data_sub_folder
# Returns: ...
# Output: ...
# Action: Executes cmd statement
cmd_write_sql_dump <- function(table, data_sub_folder) {
	char_batch <- paste0(
		' "',
		file_exe_pg_dump,
		'" ',
		" -h ", host, 
		" -U ", user,
		" -d ", dbname,
		" -t ", table,
		' > "', paste0(path_dockerbuild, "data/", table, ".sql"),
		'"'
	)
	char_batch <- gsub("\t|\n", "", char_batch)
	cat(char_batch)
	file_batch_dist_mat <- here::here(data_sub_folder,
																		paste0(table, ".bat"))
	write(char_batch, file_batch_dist_mat)
	
	Sys.setenv(PGPASSWORD = pw)
	system(file_batch_dist_mat)
	Sys.unsetenv("PGPASSWORD")
}
