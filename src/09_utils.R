################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 09_snn_calc_network_distances.R
#############################################################################
# Documentation: calc_nd
# Usage: calc_nd(con, char_mapped_points, char_osm2po_subset, char_dist_mat)
# Description: Execute query calculating the NDs between all Os and Ds
# Args/Options: con, char_mapped_points, char_osm2po_subset, char_dist_mat
# Returns: ...
# Output: ...
# Action: Executing a psql-query
calc_nd <- function(con, char_mapped_points, char_osm2po_subset, char_dist_mat, char_nd) {
	query <- paste0("CREATE TABLE ", char_nd ," AS SELECT m1.id as o_m, 
  m2.id as o_n,
  LEAST(
  	m1.distance_to_start + m2.distance_to_start + COALESCE(pi_pk.m, 0),
  	m1.distance_to_end + m2.distance_to_end + COALESCE(pj_pl.m, 0),
  	m1.distance_to_start + m2.distance_to_end + COALESCE(pi_pl.m, 0),
  	m1.distance_to_end + m2.distance_to_start + COALESCE(pj_pk.m, 0)
  ) AS nd
  FROM ", char_mapped_points, " m1
  CROSS JOIN", char_mapped_points, " m2
  INNER JOIN ", char_osm2po_subset, " e_ij ON m1.line_id = e_ij.id
  INNER JOIN ", char_osm2po_subset, " e_kl ON m2.line_id  = e_kl.id
  INNER JOIN ", char_dist_mat, " pi_pk ON pi_pk.source = LEAST(e_ij.source, e_kl.source) AND pi_pk.target = GREATEST(e_ij.source, e_kl.source)
  INNER JOIN ", char_dist_mat, " pj_pl ON pj_pl.source = LEAST(e_ij.target, e_kl.target) AND pj_pl.target = GREATEST(e_ij.target, e_kl.target)
  INNER JOIN ", char_dist_mat, " pi_pl ON pi_pl.source = LEAST(e_ij.source, e_kl.target) AND pi_pl.target = GREATEST(e_ij.source, e_kl.target)
  INNER JOIN ", char_dist_mat, " pj_pk ON pj_pk.source = LEAST(e_ij.target, e_kl.source) AND pj_pk.target = GREATEST(e_ij.target, e_kl.source)
  where m1.id < m2.id;")
	
	dbExecute(con, query)
}
