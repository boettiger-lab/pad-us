source("app/preprocess.R")

protected_area("California", "GAP_Sts") |>
  tidyr::replace_na(list(GAP_Sts = "5")) |>
  ggplot(aes(GAP_Sts, percent, fill = GAP_Sts)) + 
  geom_col(aes(x="")) + coord_polar(theta = "y") +
  gg_GAP_colors() +
  theme_void(26)

protected_area("California", "GAP_Sts") |>
  tidyr::replace_na(list(GAP_Sts = "5")) |>
  ggplot(aes(GAP_Sts, acres, fill = GAP_Sts)) + 
  geom_col() +
  gg_GAP_colors() +
  theme_minimal(26)

protected_area("All", "GAP_Sts") |>
ggplot(aes(GAP_Sts, richness, fill = GAP_Sts)) +
geom_col(show.legend = FALSE) + theme_void(26) +
ggtitle("Biodiversity") + gg_GAP_colors()

protected_area("California", "GAP_Sts") |>
ggplot(aes(GAP_Sts, social_vulnerability, fill = GAP_Sts)) +
geom_col(show.legend = FALSE) + theme_void(26) + 
ggtitle("social vulnerability") + gg_GAP_colors()



gdf <- compute_gdf("All") |> filter(GAP_Sts %in% c("1", "2"))
map_filter <- mapgl_filter(gdf, "row_n")
pmtiles_map(pad_pmtiles, "padus4", "row_n", map_filter, GAP_fill_color)





hex_map(state = "California",
        tooltip = "Unit_Nm",
        fill_color = "darkgreen")


## GAP information not available for PMTiles
gdf <- compute_gdf()
fips <- mapgl_filter(gdf, "FIPS")
pmtiles_map(tracts_pmtiles, "svi", "FIPS", fips, "darkgray")


df <- protected_area("All", "GAP_Sts")
df |> filter(GAP_Sts %in% c("1", "2", "3")) |> pull(percent) |> sum() |> round(3)


df <- 
  tracts_z8 |> 
  filter(STATE == "California") |> 
  left_join(mobi) |> 
  left_join(pad_z8) |> 
  inner_join(svi) |> 
  left_join(select(pad_us, GAP_Sts, Mang_Type, row_n), by = "row_n") |>
  group_by(GAP_Sts) |>
  summarise(area = n() * hectres_h8 * 2.47105, .groups = "drop") |>
  mutate(area_percent = area / sum(area) ) |> 
  collect()


ca_FIPS = open_dataset("s3://public-social-vulnerability/2022-tracts-h3-z8.parquet", recursive = FALSE) |> filter(STATE == "California") |> distinct(FIPS)
tracts_z10 = open_dataset("s3://public-social-vulnerability/2022-tracts-h3-z10.parquet", recursive = FALSE) |> inner_join(ca_FIPS) |> write_dataset("test.parquet")
open_dataset("test.parquet") |> mutate(h10 = unnest(h10)) |> write_dataset("ca_h10.parquet")
pad_z10 =  open_dataset("s3://public-biodiversity/pad-us-4/pad-h3-z10.parquet", recursive = FALSE)
pad_z10 |> mutate(h10 = unnest(h10)) |> write_dataset("pad_h10.parquet")
pad = open_dataset("pad_h10.parquet") |> inner_join(select(pad_us, row_n, GAP_Sts, SHAPE_Area, GIS_Acres, State_Nm), "row_n")

open_dataset("ca_h10.parquet") |>
  left_join(pad) |> group_by(GAP_Sts) |> 
  summarise(area = n() * 0.015047502 * 2.47105, 
            .groups = "drop") |>
  mutate(area_percent = area / sum(area))


pad_us |> 
  filter(State_Nm == "CA") |>
   group_by(GAP_Sts) |>
   summarise(acres = sum(GIS_Acres), area = sum(SHAPE_Area) * 0.000247105, .groups = "drop") |>
   arrange(GAP_Sts)

df <- compute_gdf() |>
      as_tibble() |>
      na.omit() |>
      mutate(vulnerability = cut(.data[["RPL_THEMES"]],
                                  breaks = c(0, .25, .50, .75, 1),
                            mappinginequality      labels = c("Q1-Lowest", "Q2-Low",
                                            "Q3-Medium", "Q4-High"))
      )

df |>
  ggplot(aes(x = vulnerability, y = richness, fill = vulnerability)) +
  geom_boxplot(alpha = 0.5) +
  geom_jitter(width = 0.2, alpha = 0.5) + theme_bw(base_size = 18) +
  theme(legend.position = "none")
