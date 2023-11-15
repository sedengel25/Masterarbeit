################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file calculates the network distances between all OD-pairs
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")

# Read in OD-points
sf_origin <- read_rds("C:/r_projects/Masterarbeit/data/processed/08/sf_origin.rds")
sf_origin <- sf_origin %>%
	mutate(id = 1:nrow(sf_origin)) 





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

# Create point-table
if (dbExistsTable(con, "origin_points_cologne_morning_rush")) {
	dbRemoveTable(con, "origin_points_cologne_morning_rush")
	dbWriteTable(con, 'origin_points_cologne_morning_rush', sf_origin)
}

# Bring geometry-column of line-table and point-table to the same name
# dbExecute(con, "
# ALTER TABLE col_2po_4pgr RENAME COLUMN geom_way TO geometry;
# ")


# Bring line-table and point-table to the same crs
dbExecute(con, "
  ALTER TABLE col_2po_4pgr
  ALTER COLUMN geometry TYPE geometry(LINESTRING, 32632)
  USING ST_Transform(geometry, 32632);
")

dbExecute(con, "
DROP TABLE IF EXISTS mapped_od_points_with_dist_to_startend;
")

dbExecute(con, "
CREATE TABLE mapped_od_points_with_dist_to_startend AS
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
