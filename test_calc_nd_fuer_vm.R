source("./src/00_config.R")
source("./src/00_utils.R")
source("./src/00_config_psql.R")
source("./src/09_utils.R")

library(parallel)
library(furrr)

# Gesamtbereich Ihrer IDs definieren
dt_mapped_origins <- RPostgres::dbReadTable(con, "col_5000_origin_mapped_wd_m") %>%
	select(id, line_id, distance_to_start, distance_to_end)

ids <- dt_mapped_origins$id

gesamtbereich <- 1:max(ids)

# Funktion, um die Prozedur für einen Bereich von IDs auszuführen
run_procedure_for_range <- function(start_id, end_id, conn_info) {
	conn <- DBI::dbConnect(RPostgres::Postgres(), 
												 dbname = conn_info$dbname, 
												 host = conn_info$host,
												 user = conn_info$user,
												 password = conn_info$pw,
												 port = 5432
	)
	
	range_str <- paste0("generate_series(", start_id, ",", end_id, ")")
	
	query <- paste0("DO $$
DECLARE
    m1_id INTEGER;
BEGIN
    FOR m1_id IN SELECT ", range_str, "
    LOOP
        INSERT INTO ergebnisse(o_m, o_n, nd)
        SELECT m1.id as o_m, 
               m2.id as o_n,
               LEAST(
                  m1.distance_to_start + m2.distance_to_start + COALESCE(pi_pk.m, 0),
                  m1.distance_to_end + m2.distance_to_end + COALESCE(pj_pl.m, 0),
                  m1.distance_to_start + m2.distance_to_end + COALESCE(pi_pl.m, 0),
                  m1.distance_to_end + m2.distance_to_start + COALESCE(pj_pk.m, 0)
               ) AS nd
        FROM col_5000_origin_mapped_wd_m m1
        CROSS JOIN col_5000_origin_mapped_wd_m m2
        INNER JOIN col_2po_4pgr_subset e_ij ON m1.line_id = e_ij.id
        INNER JOIN col_2po_4pgr_subset e_kl ON m2.line_id = e_kl.id
        INNER JOIN col_5000_dist_mat pi_pk ON pi_pk.source = LEAST(e_ij.source, e_kl.source) AND pi_pk.target = GREATEST(e_ij.source, e_kl.source)
        INNER JOIN col_5000_dist_mat pj_pl ON pj_pl.source = LEAST(e_ij.target, e_kl.target) AND pj_pl.target = GREATEST(e_ij.target, e_kl.target)
        INNER JOIN col_5000_dist_mat pi_pl ON pi_pl.source = LEAST(e_ij.source, e_kl.target) AND pi_pl.target = GREATEST(e_ij.source, e_kl.target)
        INNER JOIN col_5000_dist_mat pj_pk ON pj_pk.source = LEAST(e_ij.target, e_kl.source) AND pj_pk.target = GREATEST(e_ij.target, e_kl.source)
        WHERE m1.id = m1_id and m1.id < m2.id;

        -- Gib eine Meldung für jede verarbeitete ID aus
        --RAISE NOTICE 'Verarbeitung abgeschlossen für m1_id: %', m1_id;
    END LOOP;
END $$;")
	cat(query)
	DBI::dbExecute(conn, query)
	DBI::dbDisconnect(conn)
}

# Ihre Datenbankverbindungsdaten
conn_info <- list(dbname = "postgres", 
									host = "localhost", 
									user = "postgres",
									pw = "Wurstsalat1996")

# Anzahl der parallelen Tasks
anzahl_parallel <- detectCores() - 1

# Bereiche für jeden parallelen Task erstellen
range_size <- ceiling(length(gesamtbereich) / anzahl_parallel)
id_ranges <- split(gesamtbereich, ceiling(seq_along(gesamtbereich) / range_size))

#Test 
handlers(global = TRUE)  # Globaler Fortschrittsanzeiger
plan(multisession, workers = anzahl_parallel)  # Anzahl der Worker anpassen
furrr::future_map(id_ranges, function(range) {
	run_procedure_for_range(min(range), max(range), conn_info)
})


