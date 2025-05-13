library(shiny)
library(bslib)
library(ggplot2)
library(dplyr)
library(mapgl)
library(duckdbfs)
library(shinybusy)
source("preprocess.R")
#source("geolocate.R")

# Define the UI
ui <- page_sidebar(
  fillable = FALSE, # do not squeeze to vertical screen space
  #tags$head(css),
  titlePanel("Protected Area Explorer"),
  shinybusy::add_busy_spinner(),

  layout_columns(
    selectInput("state", "Selected state:", get_states(), selected = "Colorado"),
    uiOutput("box"),
    col_widths = c(8, 4),
    row_heights = c("150px"),
  ),
  #textInput("county", "Selected county", "Alameda County"),

  layout_columns(
    card(maplibreOutput("map")),
    card(plotOutput("plot2"), plotOutput("plot3")),
    col_widths = c(8, 4),
    row_heights = c("700px"),
    max_height = "900px"
  ),

  card(
      card_header("Errata"),
      shiny::markdown(readr::read_file("footer.md")),
  ),
  sidebar = sidebar(
    open = FALSE, width = 250,
    input_switch("switch", "hex mode"),
    checkboxGroupInput(
      "gap_codes",
      "GAP codes",
      c( 
        "1" = "1",
        "2" = "2",
        "3" = "3",
        "4" = "4"
      ), selected = c("1", "2")),
    input_dark_mode(id = "mode"),
  ),
  theme = bs_theme(version = "5")
)



# Define the server
server <- function(input, output, session) {

  output$box <- renderUI({
    req(input$state)
    df <- protected_area(input$state, "GAP_Sts")
    total <- df |> filter(GAP_Sts %in% input$gap_codes) |> pull(percent) |> sum() |> round(3)
    total <- total * 100

    value_box(glue("Protected Area:"), glue("{total}%"), showcase = plotOutput("plot"))
  })

  output$map <- renderMaplibre({

    print(input$gap_codes)

    if (!input$switch) {
      gdf <- compute_gdf(input$state) |> filter(GAP_Sts %in% input$gap_codes)
      map_filter <- mapgl_filter(gdf, "row_n")
      m <- pmtiles_map(pad_pmtiles, "padus4", "row_n", map_filter, GAP_fill_color)

    } else {

      m <- hex_map(state = input$state, "Unit_Nm", GAP_fill_color)
    }

    m |>  
      add_geolocate_control() |>
      add_geocoder_control()

  })

  output$plot <- renderPlot(
    {
      protected_area(input$state, "GAP_Sts") |>
        tidyr::replace_na(list(GAP_Sts = "5")) |>
        ggplot(aes(GAP_Sts, percent, fill = GAP_Sts)) + 
        geom_col(aes(x=""), show.legend = FALSE) + coord_polar(theta = "y") +
        gg_GAP_colors() +
        theme_void(26)
    })

    output$plot2 <- renderPlot(
    {
      protected_area(input$state, "GAP_Sts") |>
        tidyr::replace_na(list(GAP_Sts = "5")) |>
        ggplot(aes(GAP_Sts, acres, fill = GAP_Sts)) +
        geom_col(show.legend = FALSE) +
        gg_GAP_colors() +
        theme_minimal(26)
    })

  output$plot3 <- renderPlot(
    {
      protected_area(input$state, "GAP_Sts") |>
      ggplot(aes(GAP_Sts, richness, fill = GAP_Sts)) +
      geom_col(show.legend = FALSE) + theme_void() +
      ggtitle("Biodiversity") + gg_GAP_colors()
     
    })
}

# Run the app
shinyApp(ui = ui, server = server)
