source("./src/00_config.R")
source("./src/00_utils.R")

# Load your linestring network as an sf object (replace with your data)
# For demonstration, let's assume your linestrings are stored in 'linestrings_sf'

dt_origin <- read_rds(path_dt_mapped_origin)

sf_origin <- st_as_sf(dt_origin)

sf_origin <- sf_origin %>%
	mutate(id = c(1:nrow(sf_origin)))


sf_ls_network <-
	shp_network_to_linestrings(shp_file = path_shp_file_köln_strassen)
st_crs(sf_ls_network) <- st_crs(sf_origin)

# Create a convex hull around all linestrings to get a bounding polygon
bounding_polygon <- st_convex_hull(st_union(sf_ls_network))


# Define the size of the grid cells (adjust as needed)
grid_size <- 2000  # Adjust to control the size of each buffer

# Create a grid of non-overlapping cells within the bounding polygon
grid_cells <- st_make_grid(bounding_polygon, cellsize = c(grid_size, grid_size))

ggplot() +
	geom_sf(data = bounding_polygon) +
	geom_sf(data = grid_cells) +
	geom_sf(data = grid_cells[44], aes(fill = "red")) +
	geom_sf(data = sf_ls_network)

# Initialize a list to store the fully covered linestrings
fully_covered_linestrings <- list()

# Loop through each grid cell
for (i in 1:length(grid_cells)) {
	i <- 44
	# Calculate a buffer around the grid cell
	buffer <- st_buffer(grid_cells[i], dist = grid_size / 2)  # Half the cell size
	
	# Identify linestrings that intersect with the buffer
	linestrings_intersec <- st_intersection(sf_ls_network, buffer)
	
	# Filter and store only fully covered linestrings
	fully_covered <- intersected_linestrings[
		st_covers(buffer, intersected_linestrings),
	]
	
	fully_covered_linestrings[[i]] <- fully_covered
}

# Combine the fully covered linestrings from all cells into one sf object
final_fully_covered <- do.call(rbind, fully_covered_linestrings)

# Now, final_fully_covered contains the fully covered linestrings in non-overlapping buffers
