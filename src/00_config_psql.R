################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# This file contains only the credentials needed to connect to a psql-db
#############################################################################
# Configure connection to PostgreSQL database
user <- "postgres"
pw <- "Wurstsalat1996"
dbname <- "snn"
host <- "localhost" 
port <- "5432"
con <- connect_to_postgresql_db(user = user,
																pw = pw,
																dbname = dbname,
																host = host)

# # MobiWerk-PSQL-Server
# con <- dbConnect(MariaDB(), 
# 								 dbname = "scootercrawler", # replace with your database name
# 								 host = "localhost",     # local end of the SSH tunnel
# 								 port = 5433,            # local port you used in the SSH tunnel
# 								 user = "scootercrawler", # your MySQL username
# 								 password = "mX6djK2yOLyJnfKRzB") # your MySQL password
# 
# # Now you can run queries using con
