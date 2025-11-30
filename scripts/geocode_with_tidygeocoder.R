#!/usr/bin/env Rscript
# Geocode cleaned Olympic host cities using tidygeocoder (Nominatim / OSM)
#
# Reads `data/olympic_host_cities_normalized_clean.csv`, loads the JSON cache
# at `data/geocoded_hosts.json`, geocodes missing or non-`ok` entries, and
# writes the updated JSON cache plus a CSV summary `data/geocoded_hosts.csv`.

install_if_missing <- function(pkgs) {
  to_install <- pkgs[!(pkgs %in% installed.packages()[, "Package"]) ]
  if (length(to_install) > 0) {
    install.packages(to_install, repos = "https://cloud.r-project.org")
  }
}

install_if_missing(c("tidygeocoder", "jsonlite", "dplyr", "readr"))

library(tidygeocoder)
library(jsonlite)
library(dplyr)
library(readr)

INPUT_CSV <- "data/olympic_host_cities_normalized_clean.csv"
CACHE_JSON <- "data/geocoded_hosts.json"
OUT_CSV <- "data/geocoded_hosts.csv"

if (!file.exists(INPUT_CSV)) stop("Input CSV not found: ", INPUT_CSV)

# load or create cache
cache <- list()
if (file.exists(CACHE_JSON)) {
  # read as named list; fromJSON simplifies to list/data.frame; keep as list
  cache <- fromJSON(CACHE_JSON, simplifyVector = FALSE)
}

df <- read_csv(INPUT_CSV, col_types = cols(.default = "c"))

make_key <- function(year, city, country) {
  paste0(ifelse(is.na(year), "", trimws(year)), "|", ifelse(is.na(city), "", trimws(city)), "|", ifelse(is.na(country), "", trimws(country)))
}

entries <- list()

for (i in seq_len(nrow(df))) {
  row <- df[i, ]
  year <- row$Year %||% ""
  city <- row$City %||% ""
  country <- row$Country %||% ""
  key <- make_key(year, city, country)

  if (!is.null(cache[[key]])) {
    # keep existing cache entry
    entries[[key]] <- cache[[key]]
    next
  }

  query_parts <- c()
  if (!is.na(city) && nchar(trimws(city)) > 0) query_parts <- c(query_parts, trimws(city))
  if (!is.na(country) && nchar(trimws(country)) > 0) query_parts <- c(query_parts, trimws(country))
  query <- paste(query_parts, collapse = ", ")

  entry <- list(
    key = key,
    year = as.character(year),
    city = as.character(city),
    country = as.character(country),
    query = query,
    lat = NULL,
    lon = NULL,
    provider = "osm",
    status = "pending",
    timestamp = format(Sys.time(), tz = "UTC", usetz = TRUE),
    raw = NULL
  )

  # if query empty, mark no_query and add
  if (query == "") {
    entry$status <- "no_query"
    entries[[key]] <- entry
    next
  }

  # attempt geocode via tidygeocoder (osm / nominatim)
  cat(sprintf("Geocoding (%s): %s\n", key, query))
  tryCatch({
    # tidygeocoder returns a tibble; use full_results=TRUE to capture raw
    res <- geo(address = query, method = "osm", full_results = TRUE, limit = 1)
    if (nrow(res) > 0 && !is.na(res$lat[1]) && !is.na(res$long[1])) {
      entry$lat <- as.numeric(res$lat[1])
      entry$lon <- as.numeric(res$long[1])
      entry$status <- "ok"
      # keep some raw details if available
      raw <- res[1, setdiff(names(res), c("lat","long"))]
      entry$raw <- as.list(raw)
    } else {
      entry$status <- "not_found"
    }
  }, error = function(e) {
    entry$status <- "error"
    entry$error <- as.character(e)
  })

  entries[[key]] <- entry

  # be polite to Nominatim
  Sys.sleep(1)
}

# merge entries with existing cache (prefer existing values)
for (k in names(entries)) cache[[k]] <- entries[[k]]

# write JSON cache
dir.create(dirname(CACHE_JSON), showWarnings = FALSE, recursive = TRUE)
write(toJSON(cache, pretty = TRUE, auto_unbox = TRUE, null = "null"), CACHE_JSON)

# write CSV summary
all_keys <- names(cache)
rows <- lapply(all_keys, function(k) {
  item <- cache[[k]]
  data.frame(
    key = item$key %||% k,
    year = item$year %||% "",
    city = item$city %||% "",
    country = item$country %||% "",
    query = item$query %||% "",
    lat = ifelse(is.null(item$lat), NA, item$lat),
    lon = ifelse(is.null(item$lon), NA, item$lon),
    provider = item$provider %||% "",
    status = item$status %||% "",
    timestamp = item$timestamp %||% "",
    stringsAsFactors = FALSE
  )
})
out_df <- do.call(rbind, rows)
write_csv(as.data.frame(out_df), OUT_CSV)

cat(sprintf("Geocoding finished. Wrote %d entries to %s and %s\n", length(cache), CACHE_JSON, OUT_CSV))
