################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains spatial analysis of the cleaned data
################################################################################
source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/07_utils.R")


# Read in clean data
dt_clean <- read_rds(file_rds_dt_clustered_bolt_berlin_06_05)
dt_org <- read_rds(file_rds_dt_bolt_berlin_06_05)



###############################################################################
# Shiny
################################################################################
# Define the UI
ui <- fluidPage(
	titlePanel("My Leaflet Map in Shiny"),  # Add a title panel 
	
	# Create a full-page map output
	tags$style(type = "text/css", "#mymap {height: calc(100vh - 80px) !important;}"),
	
	leafletOutput("mymap")  # This is the map output from server side
)

# Define the server logic
server <- function(input, output) {
	
	# Render Leaflet map
	output$mymap <- renderLeaflet({
		
		# Your existing working leaflet code
		leaflet(dt_analysis) %>%
			addTiles() %>%
			setView(lng = 13.404954, lat = 52.520008, zoom = 11) %>%
			addHeatmap(
				lng = ~start_loc_lon,
				lat = ~start_loc_lat,
				max = 100,
				radius = 5,
				blur = 10,
				group = "Heatmap"
			)
	})
}

# Create the Shiny app and run it
shinyApp(ui, server)



dt_analysis <- dt_analysis %>%
	mutate(day_and_hour = paste0(day, "-", start_hour))



time_points <- dt_analysis %>%
	arrange(day, start_hour) %>%
	select(day_and_hour) %>%
	distinct() %>%
	pull(day_and_hour)

generate_heatmap <- function(dt, time_point) {
	dt_filtered <- dt %>%
		filter(day_and_hour == time_point)
	print(dt_filtered)
  heatmap <- leaflet(dt_filtered) %>%
  	addTiles() %>%
  	setView(lng = 13.404954, lat = 52.520008, zoom = 11) %>%
  	addHeatmap(
  		lng = ~start_loc_lon,
  		lat = ~start_loc_lat,
  		max = 10,
  		radius = 10,
  		blur = 10,
  		group = "Heatmap"
  	) %>%
  	addControl(
  		html = time_point,  # This is the text that will display
  		position = "bottomleft"  # You can choose where to position the label
  	)
  
  return(heatmap)
}

for(i in 1:length(time_points)){
	time <- time_points[i]
	plot <- generate_heatmap(dt = dt_analysis, time_point = time)
	file <- sprintf(paste0(path_heatmaps_bolt_berlin_05_12, "/frame_%s.png"), time)
	mapshot(plot, file = file)
}




# Load images
image_files <- list.files(path_heatmaps_bolt_berlin_05_12, full.names = TRUE, pattern = "*.png")
images <- image_read(image_files)

# Animate images
animation <- image_animate(images, fps = 1)  # fps controls the speed of the animation

# Save animation
image_write(animation,paste0(path_heatmaps_bolt_berlin_05_12, "/heatmap.gif"))


