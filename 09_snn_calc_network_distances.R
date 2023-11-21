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


dt_dist_mat <- RPostgres::dbReadTable(con, "distance_matrix_reduced")

summary(dt_dist_mat)


# filter_select <- function(filter, select) {
#   dt_object <- dt %>%
#   	filter(id == filter) %>%
#   	select(select)
#   
#   return(dt_object)
# }

for(i in nrow(dt_mapped_o)){
	
	on <- dt_mapped_d[i, "id"]
	
	edge_ij <- dt_mapped_d[dt_mapped_d$id == om, "line_id"]
	
	le_os <- dt_mapped_d[dt_mapped_d$id == om, "distance_to_start"]
	
	le_om <- dt_mapped_d[dt_mapped_d$id == om, "distance_to_end"]
	
	p_k <- dt_network[dt_network$id == edge_ij, "source"]
	
	p_l <- dt_network[dt_network$id == edge_ij, "target"]
	
	
	for(m in nrow(dt_mapped_o)){
		
		on <- dt_mapped_d[m, "id"]
		
		edge_mn <- dt_mapped_d[dt_mapped_d$id == on, "line_id"]
		
		le_os <- dt_mapped_d[dt_mapped_d$id == on, "distance_to_start"]
		
		le_om <- dt_mapped_d[dt_mapped_d$id == on, "distance_to_end"]
		
		p_k <- dt_network[dt_network$id == edge_mn, "source"]
		
		p_l <- dt_network[dt_network$id == edge_mn, "target"]
		
		nd_pi_pk <- dt_dist_mat[dt_dist_mat$source == p_i & 
															dt_dist_mat$target == p_k, "m"]


		

		
		# nd_om_on <- ls_om + ls_on 
		
	}
	break
}