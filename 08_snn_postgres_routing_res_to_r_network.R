################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This turns the network created using postgresql & PostGIS into a R-sf-network
################################################################################
# Um die Kante aus sf_edges zu bekommen, muss man 'id' wählen
# Um die Nodes aus sf_nodes zu bekommen, muss man 'source' & 'target' wählen
source("./src/00_config.R")
source("./src/00_utils.R")
# source("./src/08_utils.R")

dt_shortest_paths <- read_csv("shortest_paths_cologne_edgeonly_202311141501.csv") %>%
	as.data.table %>%
	distinct
dt_cologne <- read_csv("col_2po_4pgr_202311141523.csv") %>%
	as.data.table


dt <- dt_cologne %>%
	inner_join(dt_shortest_paths, by = c("id" = "edge"))

dt <- dt %>%
	select(source, target, km, geom_way)

colnames(dt) <- c("from", "to", "km", "geometry")
sf_network <- dt %>%
	st_as_sf(wkt = "geometry") %>%
	as_sfnetwork

sf_network %>% clusters


