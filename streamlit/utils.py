import ibis
from ibis import _
from variables import *
import altair as alt

def tpl_style(ids, paint):
    style =  {
    "version": 8,
    "sources": {
        "tpl": {
            "type": "vector",
            "url": "pmtiles://" + pmtiles,
            "attribution": "TPL"
        },
    },
    "layers": [{
            "id": "tpl",
            "source": "tpl",
            "source-layer": "tpl",
            "type": "fill",
            'filter': ['in', ['get', 'TPL_ID'], ["literal", ids]],
            "paint": {
                "fill-color": paint,
                "fill-opacity": 1
            }
        }]
    }
    return style
        
def get_ids(state_choice, gap_choice, year_range):

    min_year, max_year = year_range
    gdf = (database.filter(_.GAP_Sts.isin(gap_choice))
           .filter(_.Close_Year>=(min_year))
           .filter(_.Close_Year<=(max_year))
          )
    if state_choice[0] != "All":
        gdf = gdf.filter(_.State.isin(state_choice))

    
    return gdf


def fit_bounds(state_choice, m):
    if state_choice[0] != "All":
        gdf = tpl.filter(_.State.isin(state_choice))
        bounds = list(gdf.execute().total_bounds)
        m.fit_bounds(bounds) # need to zoom to filtered area
        return

def get_summary_chart(gdf, style_choice, session_state, paint):
    z8_hex_m2 = 737327.598
    total_area_m2 = gdf.count().execute()*z8_hex_m2
    group_col = style_choice_columns[style_choice]
    df = gdf.group_by(group_col).agg(percent = _.count()*z8_hex_m2/total_area_m2).execute()
    get_donut(df, style_choice, group_col, paint)

    metrics = [metric_columns[keys[0]] for keys in session_state.items() if keys[1]]
    if metrics:
        for metric_col in metrics:
            df = gdf.group_by(group_col).agg(_[metric_col].mean()).execute()
            get_bar(df, style_choice, group_col, metric_col, paint)
    return 

def get_bar(df, style_choice, group_col, metric_col, paint):
    domain = [stop[0] for stop in paint['stops']]
    range_ = [stop[1] for stop in paint['stops']]
    chart = (alt.Chart(df)
                .mark_bar(stroke="black", strokeWidth=0.1)
                .encode(
                    x=alt.X(f"{group_col}:N", axis=alt.Axis(title=style_choice)),
                    y=alt.Y(f"Mean({metric_col}):Q"),
                    tooltip=[alt.Tooltip(group_col, type="nominal"), alt.Tooltip(f"Mean({metric_col})", type="quantitative")],
                    color=alt.Color(
                            f"{group_col}:N",
                            legend=alt.Legend(title=style_choice,symbolStrokeWidth=0.5),
                            scale=alt.Scale(domain=domain, range=range_)
                         ),
                    )
             .properties(title=f"Mean {metric_col}")
            )
    st.altair_chart(chart)
    return 
 

def get_donut(df, style_choice, group_col, paint):
    domain = [stop[0] for stop in paint['stops']]
    range_ = [stop[1] for stop in paint['stops']]
    base = alt.Chart(df).encode(alt.Theta("percent:Q").stack(True))
    chart = (alt.Chart(df)
                .mark_arc(innerRadius=40, outerRadius=100, stroke="black", strokeWidth=0.1)
                .encode(
                    theta=alt.Theta("percent:Q", stack=True),
                    color = alt.Color(f"{group_col}:N",
                            legend=alt.Legend(title=style_choice,symbolStrokeWidth=0.5),
                            scale=alt.Scale(domain=domain, range=range_)
                         ),
                    tooltip=[
                        alt.Tooltip(group_col, type="nominal"), 
                        alt.Tooltip("percent", type="quantitative", format=",.1%"),
                        ]
                    )
            )

    st.altair_chart(chart)
    return 
 


def get_legend(paint):
    """
    Generates a legend dictionary with color mapping and formatting adjustments.
    """
    legend = {cat: color for cat, color in paint['stops']}
    position, fontsize, bg_color = 'bottom-left', 15, 'white'
    bg_color = 'rgba(255, 255, 255, 0.6)'
    fontsize = 12
    return legend, position, bg_color, fontsize

