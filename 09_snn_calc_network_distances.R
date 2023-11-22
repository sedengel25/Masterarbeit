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


query <- "SELECT m1.id as o_m, 
m2.id as o_n,
LEAST(
	m1.distance_to_start + m2.distance_to_start + COALESCE(pi_pk.m, 0),
	m1.distance_to_end + m2.distance_to_end + COALESCE(pj_pl.m, 0),
	m1.distance_to_start + m2.distance_to_end + COALESCE(pi_pl.m, 0),
	m1.distance_to_end + m2.distance_to_start + COALESCE(pj_pk.m, 0)
) AS nd
FROM 
col_mapped_o_points m1
CROSS JOIN col_mapped_o_points m2
INNER JOIN col_2po_4pgr_subset e_ij ON m1.line_id = e_ij.id
INNER JOIN col_2po_4pgr_subset e_kl ON m2.line_id  = e_kl.id
INNER JOIN distance_matrix_reduced pi_pk ON pi_pk.source = LEAST(e_ij.source, e_kl.source) AND pi_pk.target = GREATEST(e_ij.source, e_kl.source)
INNER JOIN distance_matrix_reduced pj_pl ON pj_pl.source = LEAST(e_ij.target, e_kl.target) AND pj_pl.target = GREATEST(e_ij.target, e_kl.target)
INNER JOIN distance_matrix_reduced pi_pl ON pi_pl.source = LEAST(e_ij.source, e_kl.target) AND pi_pl.target = GREATEST(e_ij.source, e_kl.target)
INNER JOIN distance_matrix_reduced pj_pk ON pj_pk.source = LEAST(e_ij.target, e_kl.source) AND pj_pk.target = GREATEST(e_ij.target, e_kl.source)
where m1.id < m2.id;"

