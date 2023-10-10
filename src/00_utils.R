################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions that are needed over and over again
################################################################################

# Documentation: sample_subset
# Usage: sample_subset(dt)
# Description: Takes a subset of dt based on a random selection dep. on size
# Args/Options: dt
# Returns: datatable
# Output: ...
sample_subset <- function(dt, size) {
	vector_rand_inx <- sample(1:nrow(dt), size = size)
	
	dt_sub <- dt %>%
		slice(vector_rand_inx)
	
	return(dt_sub)
}
