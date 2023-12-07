################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains all functions to execute 10_snn_flow_nd.R
#####################################################################
# Documentation: calc_rk 
# Usage: calc_fk(k)
# Description: Returns F_k (https://sci-hub.ee/10.1007/s11004-011-9325-x)
# Args/Options: k
# Returns: F_k
# Output: ...
# Action: ...
calc_fk <- function(k) {
	k_value <- k * factorial(2 * k) * (pi^0.5) / ((2^k * factorial(k))^2)
	return(k_value)
}

# Documentation: calc_rk
# Usage: calc_rk(k)
# Description: Calculates R_k from the snn_flow-Paper
# Args/Options: k
# Returns: R_k
# Output: ...
# Action: ...
calc_rk <- function(k) {
	true_value <- k
	ratio_value <- (true_value + 1 - ((2 * true_value + 1) / (2 * true_value))^2 *
										calc_fk(true_value)^2) / 
		(true_value - calc_fk(true_value)^2)
	cat("Ratio Value for k =", true_value, "is", ratio_value, "\n")
	return(ratio_value)
}



