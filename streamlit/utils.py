import ibis
from ibis import _
from variables import *
import altair as alt
    
def get_counties(state_selection):
    if state_selection != 'All':
        counties = database.filter(_.state_name == state_selection).select('county').distinct().order_by('county').execute()
        counties = ['All'] + counties['county'].tolist()
    else: 
        counties = None
    return counties
     
def get_ids(state_choice, county_choice, year_range):
    min_year, max_year = year_range
    gdf = (database.filter(_.Close_Year>=(min_year))
           .filter(_.Close_Year<=(max_year))
          )
    if state_choice[0] != "All":
        gdf = gdf.filter(_.state_name.isin(state_choice))
        if (county_choice != "All") and (county_choice[0]):
            gdf = gdf.filter(_.county == county_choice)
    return gdf

def get_landvote(gdf, style_choice):
    group_col = style_choice_columns[style_choice]
    # ids = gdf.filter(_.measure_status == 'Pass').filter(_.measure_year < _.Close_Year).execute()
    # ids = gdf.filter(_.measure_status == 'Pass').execute()
    # ids = ids['TPL_ID'].tolist()
    # ids = list(set(ids)) # get unique 
    landvote_df = gdf.filter(_.measure_status == 'Pass')
    landvote_df = landvote_df.group_by(group_col).agg(total_amount = _.Amount.sum(),
                                                 total_approved = _.measure_amount.sum())
    print(landvote_df.head().execute())
    return landvote_df, group_col

def fit_bounds(state_choice, county_choice, m):
    if state_choice[0] != "All":
        gdf = con.read_parquet(tpl_landvote_joined_url).filter(_.state_name.isin(state_choice))
        if (county_choice != "All") and (county_choice[0]):
            gdf = gdf.filter(_.county == county_choice)
        bounds = list(gdf.execute().total_bounds)
        m.fit_bounds(bounds) # need to zoom to filtered area
        return

def get_summary_chart(gdf, style_choice, session_state, paint):
    z8_hex_m2 = 737327.598
    total_area_m2 = gdf.count().execute()*z8_hex_m2
    group_col = style_choice_columns[style_choice]
    df = gdf.group_by(group_col).agg(percent = _.count()*z8_hex_m2/total_area_m2).execute()
    get_donut(df, style_choice, group_col, paint)
    metrics = [metric_columns[keys[0]] for keys in session_state.items() if (keys[1] and keys[1] in metric_columns)]
    if metrics:
        for metric_col in metrics:
            # if metric_col == 'measure_status':
            #     total_measures = gdf.select(_[metric_col]).count().execute()
            #     df = gdf.group_by(group_col).agg(((_[metric_col]=='Pass').count()/total_measures).name(metric_col)).execute()
            # else:
            df = gdf.group_by(group_col).agg(_[metric_col].mean().name(metric_col)).execute()
            get_bar(df, style_choice, group_col, metric_col, paint)
    return 

def get_bar(df, style_choice, group_col, metric_col, paint):
    domain = [stop[0] for stop in paint['stops']]
    range_ = [stop[1] for stop in paint['stops']]
    chart = (alt.Chart(df)
                .mark_bar(stroke="black", strokeWidth=0.1)
                .encode(
                    x=alt.X(f"{group_col}:N", axis=alt.Axis(title=style_choice)),
                    y=alt.Y(f"{metric_col}:Q"),
                    tooltip=[alt.Tooltip(group_col, type="nominal"), alt.Tooltip(metric_col, type="quantitative")],
                    color=alt.Color(
                            f"{group_col}:N",
                            legend=alt.Legend(title=style_choice,symbolStrokeWidth=0.5),
                            scale=alt.Scale(domain=domain, range=range_)
                         ),
                    )
             .properties(title=f"{metric_col}")
            )
    st.altair_chart(chart, use_container_width = True)
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

    st.altair_chart(chart, use_container_width = True)
    return 
    
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
            "source-layer": source_layer_name,
            "type": "fill",
            'filter': ['in', ['get', 'TPL_ID'], ["literal", ids]],
            "paint": {
                "fill-color": paint,
                "fill-opacity": 1
            }
        }]
    }
    return style

    
def get_legend(paint):
    """
    Generates a legend dictionary with color mapping and formatting adjustments.
    """
    legend = {cat: color for cat, color in paint['stops']}
    position, fontsize, bg_color = 'bottom-left', 15, 'white'
    bg_color = 'rgba(255, 255, 255, 0.6)'
    fontsize = 12
    return legend, position, bg_color, fontsize



@st.cache_data
def tpl_summary(_df):
    summary = _df.group_by(_.Manager_Type).agg(Amount = _.Amount.sum())
    # summary = _df.filter(~_.Amount.isnull()).group_by(_.Manager_Type).agg(Amount = _.Amount.sum())
    public_dollars = round( summary.filter(_.Manager_Type.isin(["FED", "STAT", "LOC", "DIST"])).agg(total = _.Amount.sum()).to_pandas().values[0][0] )
    private_dollars = round( summary.filter(_.Manager_Type.isin(["PVT", "NGO"])).agg(total = _.Amount.sum()).to_pandas().values[0][0] )
    tribal_dollars = summary.filter(_.Manager_Type.isin(["TRIB"])).agg(total = _.Amount.sum()).to_pandas().values[0][0] 
    tribal_dollars = tribal_dollars if tribal_dollars else round(tribal_dollars)
    total_dollars = round( summary.agg(total = _.Amount.sum()).to_pandas().values[0][0] )
    return public_dollars, private_dollars, tribal_dollars, total_dollars

@st.cache_data
def calc_delta(_df):
    deltas = (_df
     .group_by(_.Manager_Type, _.Close_Year)
     .agg(Amount = _.Amount.sum())
     #.filter(_.Manager_Type.isin(["FED"]))
     # .order_by(_.Close_Year)
     .mutate(total = _.Amount.cumsum(order_by=_.Close_Year, group_by=_.Manager_Type))
     .mutate(lag = _.total.lag(1))
     .mutate(delta = (100*(_.total - _.lag) / _.total).round(2)  )
     .filter(_.Close_Year >=2019)
     .select(_.Manager_Type, _.Close_Year, _.total, _.lag, _.delta)
    )
    public_delta = deltas.filter(_.Manager_Type.isin(["FED", "STAT", "LOC", "DIST"])).to_pandas().delta[0]
    private_delta = deltas.filter(_.Manager_Type.isin(["PVT", "NGO"])).to_pandas().delta[0]
    trib_delta = deltas.filter(_.Manager_Type=="TRIB").to_pandas()
    trib_delta = 0 if trib_delta.empty else trib_delta.delta[0]
    #total_dollars = round( summary.agg(total = _.Amount.sum()).to_pandas().values[0][0] )

    return public_delta, private_delta, trib_delta
    
@st.cache_data
def get_area_totals(_df, column):
    return _df.group_by(_[column]).agg(area = _.Shape_Area.sum() / (100*100)).to_pandas()


# @st.cache_data
def bar(area_totals, column, paint):
    domain = [stop[0] for stop in paint['stops']]
    range_ = [stop[1] for stop in paint['stops']]
    plt = alt.Chart(area_totals).mark_bar().encode(
            x=column,
            y=alt.Y("area"),
            color=alt.Color(column).scale(domain = domain, range = range_)
        ).properties(height=350)
    return plt
#bar

# +

@st.cache_data
def calc_timeseries(_df, column):
    timeseries = (
        _df
        .filter(~_.Close_Year.isnull())
        .filter(_.Close_Year > 0)
        .group_by([_.Close_Year, _[column]])
        .agg(Amount = _.Amount.sum())
        .mutate(Close_Year = _.Close_Year.cast("int"),
                Amount = _.Amount.cumsum(group_by=_[column], order_by=_.Close_Year))
        
        .to_pandas()
    )
    return timeseries


@st.cache_data
def chart_time(timeseries, column, paint):
    domain = [stop[0] for stop in paint['stops']]
    range_ = [stop[1] for stop in paint['stops']]
    # use the colors 
    plt = alt.Chart(timeseries).mark_line().encode(
        x='Close_Year:O',
        y = alt.Y('Amount:Q'),
            color=alt.Color(column,scale= alt.Scale(domain=domain, range=range_))
    ).properties(height=350)
    return plt
