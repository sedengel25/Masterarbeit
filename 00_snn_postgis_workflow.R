################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file calculates the network distances between all OD-pairs
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")


dt <- get_cleaned_trip_data(path_clean = path_dt_clustered_voi_cologne_06_05,
														path_org = path_dt_voi_cologne_06_05)


# Select morning-rush-dt to get a smaller dt
dt_wd_morning_rush <- dt %>%
	mutate(weekday = wday(start_time,week_start = 1),
				 start_hour = hour(start_time)) %>%
	filter(weekday <= 5,
				 start_hour > 6 & start_hour <= 10)




# Transform numeric coordinates into wgs84 coords
sf_origin <- transform_num_to_WGS84(dt = dt_wd_morning_rush,
																		coords = c("start_loc_lon",
																							 "start_loc_lat"))

sf_origin <- sf_origin %>%
	mutate(id = 1:nrow(sf_origin))

sf_origin <- st_transform(sf_origin, crs = 32632)

sf_dest <- transform_num_to_WGS84(dt = dt_wd_morning_rush,
																	coords = c("dest_loc_lon",
																						 "dest_loc_lat"))

sf_dest <- sf_dest %>%
	mutate(id = 1:nrow(sf_dest))

sf_dest <- st_transform(sf_dest, crs = 32632)


# Configure connection to PostgreSQL database
user <- "postgres"
password <- "Wurstsalat1996"
dbname <- "cologne"
host <- "localhost" # could be localhost or an IP address

# Create a connection to the PostgreSQL database
drv <- RPostgres::Postgres()
con <- dbConnect(drv, 
								 dbname = dbname, 
								 host = host, 
								 user = user, 
								 password = password,
								 port = 5432)


# Create point-table for origin-points
if (dbExistsTable(con, "origin_points_cologne_morning_rush")) {
	dbRemoveTable(con, "origin_points_cologne_morning_rush")
	dbWriteTable(con, 'origin_points_cologne_morning_rush', sf_origin)
} else {
	dbWriteTable(con, 'origin_points_cologne_morning_rush', sf_origin)
}

# Create point-table for dest-points
if (dbExistsTable(con, "dest_points_cologne_morning_rush")) {
	dbRemoveTable(con, "dest_points_cologne_morning_rush")
	dbWriteTable(con, 'dest_points_cologne_morning_rush', sf_dest)
} else {
	dbWriteTable(con, 'dest_points_cologne_morning_rush', sf_dest)
}

# Bring geometry-column of line-table and point-table to the same name
# dbExecute(con, "
# ALTER TABLE col_2po_4pgr RENAME COLUMN geom_way TO geometry;
# ")


# Bring line-table and point-table to the same crs
# dbExecute(con, "
#   ALTER TABLE col_2po_4pgr
#   ALTER COLUMN geometry TYPE geometry(LINESTRING, 32632)
#   USING ST_Transform(geometry, 32632);
# ")


# Create geometry-index for all tables involved
dbExecute(con, "
CREATE INDEX ON col_2po_4pgr USING GIST (geometry);
")

dbExecute(con, "
CREATE INDEX ON origin_points_cologne_morning_rush 
USING GIST (geometry);
")

dbExecute(con, "
CREATE INDEX ON dest_points_cologne_morning_rush 
USING GIST (geometry);
")

# Map origin points
dbExecute(con, "
DROP TABLE IF EXISTS mapped_origin_points;
")

dbExecute(con, "
CREATE TABLE mapped_origin_points AS
SELECT
  point.id,
  line.id AS line_id,
  ST_ClosestPoint(line.geometry, point.geometry) AS closest_point_on_line,
  ST_Distance(line.geometry, point.geometry) AS distance_to_line,
  ST_Distance(ST_StartPoint(line.geometry), ST_ClosestPoint(line.geometry, point.geometry)) AS distance_to_start,
  ST_Distance(ST_EndPoint(line.geometry), ST_ClosestPoint(line.geometry, point.geometry)) AS distance_to_end
FROM
  origin_points_cologne_morning_rush AS point
CROSS JOIN LATERAL
  (SELECT id, geometry
   FROM col_2po_4pgr
   ORDER BY geometry <-> point.geometry
   LIMIT 1) AS line;
")


# Map destination points
dbExecute(con, "
DROP TABLE IF EXISTS mapped_dest_points;
")

dbExecute(con, "
CREATE TABLE mapped_dest_points AS
SELECT
  point.id,
  line.id AS line_id,
  ST_ClosestPoint(line.geometry, point.geometry) AS closest_point_on_line,
  ST_Distance(line.geometry, point.geometry) AS distance_to_line,
  ST_Distance(ST_StartPoint(line.geometry), ST_ClosestPoint(line.geometry, point.geometry)) AS distance_to_start,
  ST_Distance(ST_EndPoint(line.geometry), ST_ClosestPoint(line.geometry, point.geometry)) AS distance_to_end
FROM
  dest_points_cologne_morning_rush AS point
CROSS JOIN LATERAL
  (SELECT id, geometry
   FROM col_2po_4pgr
   ORDER BY geometry <-> point.geometry
   LIMIT 1) AS line;
")



# Drop the existing bbox temp table if it exists
dbExecute(con, "
DROP TABLE IF EXISTS bbox_geom;
")

# Calculate the bounding box with the correct SRID
dbExecute(con, "
WITH points AS (
	SELECT geometry FROM origin_points_cologne_morning_rush opcmr 
	UNION ALL
	SELECT geometry FROM dest_points_cologne_morning_rush dpcmr 
),
bbox AS (
	SELECT ST_SetSRID(ST_Extent(geometry), 32632) as geom
	FROM points
)
SELECT * INTO TEMP bbox_geom FROM bbox;
")


# Create a new subset table after dropping the old one if it exists
dbExecute(con, "
DROP TABLE IF EXISTS col_2po_4pgr_subset;
")

dbExecute(con, "
CREATE TABLE col_2po_4pgr_subset AS
SELECT *
	FROM col_2po_4pgr
WHERE ST_Intersects(col_2po_4pgr.geometry, (SELECT geom FROM bbox_geom));
")

################################################################################
# Python
################################################################################
library(reticulate)
# install_miniconda()
conda_list(conda = "C:/Users/Seppi/AppData/Local/r-miniconda/_conda.exe")
# py_install(packages = c("networkx"))
# nx <- import("networkx")
# pd <- import("pandas")
source_python("src/import_py_dijkstra_functions.py")
# py_run_string("import networkx as nx")
# py_run_string("G = nx.path_graph(5)")
# py_run_string("number_of_nodes = G.number_of_nodes()")
# py$number_of_nodes
# py_run_file()
