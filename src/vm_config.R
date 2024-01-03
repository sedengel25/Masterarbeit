################################################################################
# Big Data Analytics in Transportation (TU Dresden)
# Master Thesis
# Configuration file for the packages needed and the filepath specifications
################################################################################

# Load libraries
library(dplyr)
library(R.utils)
library(RPostgres)
library(data.table)
library(DBI)
library(parallel)
library(readr)



# Umgebungsvariablen abrufen
user <- Sys.getenv("POSTGRES_USER")
pw <- Sys.getenv("POSTGRES_PASSWORD")
dbname <- Sys.getenv("POSTGRES_DB")
host <- Sys.getenv("POSTGRES_HOST")
port <- Sys.getenv("POSTGRES_PORT")

# Verbindungsaufbau
con <- dbConnect(RPostgres::Postgres(),
                 dbname = dbname,
                 host = host,
                 port = port,
                 user = user,
                 password = pw)

# con <- dbConnect(RPostgres::Postgres(),
#                  dbname = "postgres",
#                  host = "localhost",
#                  port = 5432,
#                  user = "postgres",
#                  password = "Wurstsalat1996")


