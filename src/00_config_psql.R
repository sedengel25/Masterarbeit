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
