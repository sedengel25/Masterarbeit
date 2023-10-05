write_rds_auto <- function(obj, path) {
	# Get the object name
	obj_name <- deparse(substitute(obj))
	
	# Construct the full file path with the object name as filename
	full_path <- file.path(path, paste0(obj_name, ".rds"))
	
	# Write the object to the .rds file
	write_rds(obj, full_path)
	
	cat(paste("Saved", obj_name, "to", full_path))
}

# Example usage
df <- data.frame(a = 1:5, b = 6:10)
save_rds_auto(df, "C:/your_directory_path_here")
