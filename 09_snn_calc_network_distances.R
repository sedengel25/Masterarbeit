################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file calc. network dist. based on mapped OP-points and distance-matrix
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/00_config_psql.R")

dt_network <- RPostgres::dbReadTable(con, "col_2po_4pgr_subset") %>%
	mutate(m = km*1000) %>%
	select(id, source, target, m) 


dt_mapped_o <- RPostgres::dbReadTable(con, "col_mapped_o_points") %>%
	select(id, line_id, distance_to_start, distance_to_end)



dt_mapped_d <- RPostgres::dbReadTable(con, "col_mapped_d_points") %>%
	select(id, line_id, distance_to_start, distance_to_end)


dt_dist_mat <- RPostgres::dbReadTable(con, "distance_matrix_reduced_wo_dup")

# RPostgres::dbWriteTable(con, "distance_matrix_reduced", dt_dist_mat, overwrite = TRUE)


dt_nd_o <- data.table(
	om = integer(),
	on = integer(),
	nd = numeric()
)

int_idx <- 1

for(om in dt_mapped_o$id){
	om <- 1
	
	edge_ij <- dt_mapped_o[dt_mapped_o$id == om, "line_id"]
	
	ls_om <- dt_mapped_o[dt_mapped_o$id == om, "distance_to_start"]
	
	le_om <- dt_mapped_o[dt_mapped_o$id == om, "distance_to_end"]
	
	p_i <- dt_network[dt_network$id == edge_ij, "source"]
	
	p_j <- dt_network[dt_network$id == edge_ij, "target"]
	
	ons <- dt_mapped_o %>%
		filter(id > om) %>%
		select(id) %>%
		pull
	

	for(on in ons){
		on <- 2
		print(on)
		edge_mn <- dt_mapped_o[dt_mapped_o$id == on, "line_id"]
		
		ls_on <- dt_mapped_o[dt_mapped_o$id == on, "distance_to_start"]
		
		le_on <- dt_mapped_o[dt_mapped_o$id == on, "distance_to_end"]
		
		p_k <- dt_network[dt_network$id == edge_mn, "source"]
		
		p_l <- dt_network[dt_network$id == edge_mn, "target"]
		
		
		query <- paste0("SELECT m FROM distance_matrix_reduced_wo_dup WHERE source =
											LEAST(",p_i,", ",p_k,") AND target = GREATEST(", p_i,
										", ", p_k, ");")
		result <- dbSendQuery(con, query)
		nd_pi_pk <- dbFetch(result) %>% as.numeric
		dbClearResult(result)
		if(is.na(nd_pi_pk)){
			print("next")
			next
		}

		
		query <- paste0("SELECT m FROM distance_matrix_reduced_wo_dup WHERE source =
											LEAST(",p_j,", ",p_l,") AND target = GREATEST(", p_j,
										", ", p_l, ");")
		result <- dbSendQuery(con, query)
		nd_pj_pl <- dbFetch(result) %>% as.numeric
		dbClearResult(result)
		if(is.na(nd_pj_pl)){
			print("next")
			next
		}
		
		query <- paste0("SELECT m FROM distance_matrix_reduced_wo_dup WHERE source =
											LEAST(",p_i,", ",p_l,") AND target = GREATEST(", p_i,
										", ", p_l, ");")
		result <- dbSendQuery(con, query)
		nd_pi_pl <- dbFetch(result) %>% as.numeric
		dbClearResult(result)
		if(is.na(nd_pi_pl)){
			print("next")
			next
		}
		
		
		query <- paste0("SELECT m FROM distance_matrix_reduced_wo_dup WHERE source =
											LEAST(",p_j,", ",p_k,") AND target = GREATEST(", p_j,
										", ", p_k, ");")
		result <- dbSendQuery(con, query)
		nd_pj_pk <- dbFetch(result) %>% as.numeric
		dbClearResult(result)
		if(is.na(nd_pj_pk)){
			print("next")
			next
		}

		nd_om_on_1 <- ls_om + ls_on + nd_pi_pk
		nd_om_on_2 <- le_om + le_on + nd_pj_pl
		nd_om_on_3 <- ls_om + le_on + nd_pi_pl
		nd_om_on_4 <- le_om + ls_on + nd_pj_pk
		
		nd_om_on <- min(nd_om_on_1, nd_om_on_2, nd_om_on_3, nd_om_on_4)
		print(nd_om_on)

		# dt_nd_o[int_idx, "om"] <- om
		# dt_nd_o[int_idx, "om"] <- on
		# dt_nd_o[int_idx, "nd"] <- nd_om_on
		# 
		# 
		# int_idx <- int_idx + 1
	}
	break
}

dt_nd_o
