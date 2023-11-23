################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file calc. network dist. between flows and visualizes k-nearest flows
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/00_config_psql.R")
# source("./src/10_utils.R")

char_city_prefix <- "col"
char_mapped_o_points <- paste0(char_city_prefix, "_mapped_o_points")
char_mapped_d_points <- paste0(char_city_prefix, "_mapped_d_points")
char_flows_nd <- paste0(char_city_prefix, "_flows_nd")
char_common_flows <- paste0(char_city_prefix, "_common_flows")
char_vis <- paste0(char_city_prefix, "_vis_flows")

# Create table to check whether calculating flow-nds worked: CHECK
# query <- paste0("CREATE TABLE col_vis_flows AS
# SELECT origin.id,
#   ST_MakeLine(origin.closest_point_on_line , dest.closest_point_on_line) AS line_geom
# FROM ",  char_mapped_o_points, " origin
# INNER JOIN ", char_mapped_d_points, " dest ON origin.id = dest.id;")
# 
# dbExecute(con, query)

k <- 5
char_k_nearest_flows <- paste0(char_city_prefix,"_",k ,"_nearest_flows")
# Get k nearest flows for each flow
query <- paste0("create table ", char_k_nearest_flows, " as
								WITH SymmetricFlows AS (
									SELECT flow_m AS flow_ref, flow_n AS flow_other, nd FROM ",
								char_flows_nd, "
									UNION
									SELECT flow_n AS flow_ref, flow_m AS flow_other, nd FROM ",
								char_flows_nd, "
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
cat(query)
dbExecute(con, query)
psql_create_index(con, char_k_nearest_flows, col = c("flow_ref", "flow_other"))


query <- paste0("create table ", char_common_flows, " as
WITH ClosestFlows AS (
  SELECT flow_ref, ARRAY_AGG(flow_other ORDER BY nd) AS closest_flows
  FROM ", char_k_nearest_flows, "
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
cat(query)
dbExecute(con, query)
#psql_check_indexes(con, char_common_flows)
psql_create_index(con, char_common_flows, col = c("flow1", "flow2"))
