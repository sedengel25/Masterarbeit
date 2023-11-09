################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# TEST
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")



# Read shp-file for considered city
tibble_cologne <- read_csv("C:/r_projects/Masterarbeit/hh_2po_4pgr_202311091246.csv")
dt_cologne <- tibble_cologne %>% as.data.table


dt_cologne <- dt_cologne %>%
	dplyr::rename(geometry = geom_way)

dt_cologne$geometry <- st_as_sfc(dt_cologne$geometry)

sf_cologne <- st_as_sf(dt_cologne)


tbl_graph_cologne <- as_sfnetwork(sf_cologne) 

sf_nodes <- tbl_graph_cologne %>%
	st_as_sf

sf_nodes <- sf_nodes %>%
	mutate(id = 1:nrow(sf_nodes))

sf_edges <- tbl_graph_cologne %>%
	activate(edges) %>%
	as.data.table %>%
	st_as_sf



# sf_cologne <- sf_cologne %>%
# 	filter(kmh <= 50)

#Wenn nur Straßen mit kmh <= 50 kmh, dann entstehen 3382 cluster, was der
# # Anzahl an cluster entspricht, wenn ich das shp von Open Data Köln nehme
# network_cologne <- as_sfnetwork(sf_cologne)
# 
# clusters(network_cologne)
# # Nur noch 492 cluster, wovon eins aus 172044 von 208604 Kanten entspricht
# dt_cologne$target %>% unique %>% length
# max(dt_cologne$target)



tibble_shortest_path <- read_csv(path_csv_cologne_shortest_paths)

# Um die Kante aus sf_edges zu bekommen, muss man 'id' wählen
# Um die Nodes aus sf_nodes zu bekommen, muss man 'source' & 'target' wählen

head(tibble_shortest_path)
tibble_shortest_path_sorted <- tibble_shortest_path %>%
	arrange(table_id, id)

dt <- data.table(
	from = tibble_shortest_path_sorted$node,
	to = lead(tibble_shortest_path_sorted$node),
	edge_id = tibble_shortest_path_sorted$edge,
	edge_weight_km = tibble_shortest_path_sorted$cost
)
write_rds(dt, "dt_shortest_paths.rds")
test <- head(dt) %>%
	filter(edge_id != -1)


dt <- dt %>% 
	inner_join(sf_edges %>% select(id, geometry), by = c("edge_id" = "id")) %>%
	left_join(sf_nodes, by = c("from" = "id")) %>%
	left_join(sf_nodes, by = c("to" = "id"))


colnames(dt) <- c("from", "to", "edge_id", "edge_weight_km", "geom_edge", "geom_from", "geom_to")
head(dt)	

write_rds(dt, "dt_shortest_paths.rds")
rm(tibble_shortest_path_sorted)


rm(tbl_graph_cologne)
dt <- dt %>%
	distinct()
final_network <- dt %>% st_as_sf %>% as_sfnetwork()
final_network %>% clusters
