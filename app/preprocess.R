library(dplyr)
library(duckdbfs)
library(ggplot2)
library(mapgl)
library(glue)
library(memoise)

CACHE = tempfile()
endpoint = "minio.carlboettiger.info"

#Sys.setenv("LD_LIBRARY_PATH" = "/opt/conda/lib:$LD_LIBRARY_PATH")

duckdb_secrets()
hectres_h8 = 737327.598	/ 10000

pad_pmtiles = glue("https://{endpoint}/public-biodiversity/pad-us-4/pad-us-4.pmtiles")
tract_pmtiles = glue("https://{endpoint}/public-social-vulnerability/2022/SVI2022_US_tract.pmtiles")

# gbif = open_dataset("s3://public-gbif/hex")
# Full PAD variables
pad_us = open_dataset(glue("https://{endpoint}/public-biodiversity/pad-us-4/pad-us-4.parquet"), recursive = FALSE) |>
  select(GAP_Sts, Mang_Type, row_n, geom, GIS_Acres)

# hexed data
tracts_z8 = open_dataset(glue("https://{endpoint}/public-social-vulnerability/2022-tracts-h3-z8.parquet"), recursive = FALSE) |>
            mutate(h8 = tolower(h8))
pad_z8 =  open_dataset(glue("https://{endpoint}/public-biodiversity/pad-us-4/pad-h3-z8.parquet"), recursive = FALSE)
mobi = open_dataset(glue("https://{endpoint}/public-mobi/hex/all-richness-h8.parquet"), recursive = FALSE) |> select(richness = Z, h8)
svi = open_dataset(glue("https://{endpoint}/public-social-vulnerability/2022/SVI2022_US_tract.parquet"), recursive = FALSE)  |> 
  select(FIPS, RPL_THEMES) |>
  filter(RPL_THEMES > 0)

database = tracts_z8 |>
  left_join(mobi, by = "h8") |>
  left_join(pad_z8, by = "h8") |>
  inner_join(svi, by = "FIPS") |>
  left_join(pad_us, by = "row_n")



### Helper functions

protected_area <- memoise(
  function(state = "California", column = "GAP_Sts") {
    df <- database
    if (state != "All") {
      df <- df |> filter(STATE ==  state)
    }
    df |>
      group_by(.data[[column]]) |>
      summarise(acres = n() * hectres_h8 * 2.47105,
                richness = mean(richness),
                social_vulnerability = mean(RPL_THEMES),
              .groups = "drop") |>
      mutate(percent = acres / sum(acres)) |>
      collect()
})


compute_gdf <- memoise(
  function(state = "California") {
    df <- database |> select(-geom)
    if (state != "All") {
      df <- df |> filter(STATE ==  state)
    }
    # |>inner_join(select(pad_us, row_n, geom)) |> to_sf(crs = "EPSG:4326")
    return(df)
  }
)

# cache = cache_filesystem(CACHE)

mapgl_filter <- function(df, column) {
  values <- df |> pull(column)
  list("in", list("get", column), list("literal", values))
}


GAP_fill_color = match_expr(
    column = "GAP_Sts",
    values = c("1", "2", "3", "4"),
    stops = c("#26633d", "#879647", "#bdcf72", "#6d6e6d"),
    default = "#D3D3D3"
)

pmtiles_map <- function (pmtiles = pad_pmtiles,
                         layer = "padus4",
                         id_col = "row_n",
                         map_filter = NULL,
                         fill_color = GAP_fill_color) {
  maplibre(center=c(-112., 38.), zoom = 5, maxZoom = 12) |>
              add_fill_layer(
                id = "pmtiles_layer",
                source = list(type = "vector",
                              url = paste0("pmtiles://", pmtiles)),
                source_layer = layer, # baked in to pmtiles
                filter = map_filter,
                fill_opacity = 0.5,
                tooltip = "Unit_Nm",
                fill_color = fill_color
              )
}

hex_map <- function(state = "California",
                    tooltip = "Unit_Nm",
                    fill_color = GAP_fill_color) {

  path <- "public-data/cache/hexes-ex5.h3j"
  df <- database
  if (state != "All") {
    df <- df |> filter(STATE ==  state)
  }
  df |>
    select(-geom) |>
    filter(STATE == state) |> 
    tidyr::replace_na(list("GAP_Sts" = "5")) |>
    rename(h3id = h8) |> 
    to_h3j(glue("s3://{path}"))

  endpoint <- Sys.getenv("AWS_PUBLIC_ENDPOINT")
  url <- glue("https://{endpoint}/{path}")

  maplibre(center=c(-105., 38.), zoom = 5) |>
    add_h3j_source("h3j_source", url = url)  |>
    add_fill_layer(
      id = "h3j_layer",
      source = "h3j_source",
      tooltip = tooltip,
      fill_color = fill_color,
      fill_opacity = 0.5
    )
}

gg_GAP_colors <- function() {
  scale_fill_manual(
  values = c("1" = "#26633d",
             "2" = "#879647",
             "3" = "#bdcf72",
             "4" = "#6d6e6d",
             "5" = "#D3D3D3"))
}

get_states <- memoise(function() {
  states <- glue::glue("https://{endpoint}/public-social-vulnerability/2022/SVI2022_US_county.parquet") |>
    duckdbfs::open_dataset(recursive = FALSE) |>
    dplyr::distinct(STATE) |>
    dplyr::arrange(STATE) |>
    dplyr::pull(STATE)

  c("All", states)
})

