library(dplyr)
library(duckdbfs)
library(ggplot2)
library(mapgl)
library(glue)
library(memoise)
duckdb_secrets()



## Globals
CACHE = tempfile()
endpoint = "minio.carlboettiger.info"
hectres_h8 = 737327.598	/ 10000

pad_pmtiles = glue("https://{endpoint}/public-biodiversity/pad-us-4/pad-us-4.pmtiles")
tract_pmtiles = glue("https://{endpoint}/public-social-vulnerability/2022/SVI2022_US_tract.pmtiles")



load_data <- memoise(function() {

duckdb_secrets()
pad_us = open_dataset(glue("https://{endpoint}/public-biodiversity/pad-us-4/pad-us-4.parquet"), recursive = FALSE) |>
  select(GAP_Sts, Mang_Type, row_n, geom, GIS_Acres)

tracts_z8 = open_dataset(glue("https://{endpoint}/public-social-vulnerability/2022-tracts-h3-z8.parquet"), recursive = FALSE) |>
            mutate(h8 = tolower(h8))
pad_z8 =  open_dataset(glue("https://{endpoint}/public-biodiversity/pad-us-4/pad-h3-z8.parquet"), recursive = FALSE)
mobi = open_dataset(glue("https://{endpoint}/public-mobi/hex/all-richness-h8.parquet"), recursive = FALSE) |>
  select(richness = Z, h8)
svi = open_dataset(glue("https://{endpoint}/public-social-vulnerability/2022/SVI2022_US_tract.parquet"), recursive = FALSE) |> 
  select(FIPS, RPL_THEMES) |>
  filter(RPL_THEMES > 0)

database = tracts_z8 |>
  left_join(mobi, by = "h8") |>
  left_join(pad_z8, by = "h8") |>
  inner_join(svi, by = "FIPS") |>
  left_join(pad_us, by = "row_n")

database
})

database <- load_data()


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
    return(df)
  }
)

mapgl_filter <- memoise(
  function(df, column) {
    values <- df |> pull(column)
    list("in", list("get", column), list("literal", values))
  }
)

gap_fill_color = mapgl::match_expr(
    column = "GAP_Sts",
    values = c("1", "2", "3", "4"),
    stops = c("#26633d", "#879647", "#bdcf72", "#6d6e6d"),
    default = "#D3D3D3"
)

manager_fill_color = mapgl::match_expr(
    column = "Mang_Type",
    values = c("JNT", "TERR", "STAT", "FED", "UNK", "LOC", "PVT", "DIST", "TRIB", "NGO"),
    stops = c("#DAB0AE", "#A1B03D", "#A1B03D",  "#529642", "#bbbbbb",
              "#365591", "#7A3F1A", "#0096FF", "#BF40BF", "#D77031"),
    default = "#D3D3D3"
)

map_palette <- function(color_by) {
  switch(color_by,
         "GAP_Sts" = gap_fill_color,
         "Mang_Type" = manager_fill_color)
}

compute_filter <- memoise(
  function(state, gap_codes){
    compute_gdf(state) |> 
      filter(GAP_Sts %in% gap_codes) |>
      mapgl_filter("row_n")
})

pmtiles_map <- function (pmtiles = pad_pmtiles,
                         layer = "padus4",
                         id_col = "row_n",
                         map_filter = NULL,
                         color_by = "GAP_Sts",
                         state = "All") {

  fill_color <- map_palette(color_by)
  bounds <- get_state_geom(state)
  maplibre(bounds = bounds, maxZoom = 12) |>
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


hex_render <- memoise(
  function(state = "California",
           gap_codes = c("1", "2", "3", "4")){

    gaps <- paste0(gap_codes, collapse="")
    path <- glue("public-data/cache/pad-us/hexes-{state}-{gaps}.h3j")
    database |>
      select(-geom) |>
      filter(STATE == state, GAP_Sts %in% gap_codes) |> 
      tidyr::replace_na(list("GAP_Sts" = "5")) |>
      rename(h3id = h8) |>
      to_h3j(glue("s3://{path}"))

    endpoint <- Sys.getenv("AWS_PUBLIC_ENDPOINT", 
                           Sys.getenv("AWS_S3_ENDPOINT"))
    url <- glue("https://{endpoint}/{path}")

    url

})

hex_map <- memoise(
  function(state = "California",
           tooltip = "Unit_Nm",
           color_by = "GAP_Sts",
           gap_codes = c("1", "2", "3")) {

  url <- hex_render(state, gap_codes)
  print(url)
  fill_color <- map_palette(color_by)
  bounds <- get_state_geom(state)

  maplibre(bounds = bounds, maxZoom = 12) |>
    add_h3j_source("h3j_source", url = url)  |>
    add_fill_layer(
      id = "h3j_layer",
      source = "h3j_source",
      tooltip = tooltip,
      fill_color = fill_color,
      fill_opacity = 0.5
    )
})

gg_GAP_colors <- function() {
  scale_fill_manual(
  values = c("1" = "#26633d",
             "2" = "#879647",
             "3" = "#bdcf72",
             "4" = "#6d6e6d",
             "5" = "#D3D3D3"))
}

compute_total <- memoise(
  function(state, gap_codes) {
    df <- protected_area(state, "GAP_Sts")
    total <- df |> filter(GAP_Sts %in% gap_codes) |> pull(percent) |> sum() |> round(3)
    total * 100
  }
)

get_states <- memoise(function() {
  states <- glue::glue("https://{endpoint}/public-social-vulnerability/2022/SVI2022_US_county.parquet") |>
    duckdbfs::open_dataset(recursive = FALSE) |>
    dplyr::distinct(STATE) |>
    dplyr::arrange(STATE) |>
    dplyr::pull(STATE)

  c("All", states)
})

get_state_geom <- memoise(function(state, county = "All", fips = NULL) {
  df <- 
    glue::glue("https://{endpoint}/public-social-vulnerability/2022/SVI2022_US_county.parquet") |>
    duckdbfs::open_dataset(recursive = FALSE) |>
    select(STATE, COUNTY, FIPS, geom = Shape)

  if (state != "All") {
    df <- df |> dplyr::filter(STATE == state)
  }

  if (county != "All") {
    df <- df |> dplyr::filter(COUNTY == county)
  }

  if (!is.null(fips)) {
    df <- df |> filter(FIPS %in% fips)
  }

  df |> duckdbfs::to_sf(crs = "EPSG:4326")
})
