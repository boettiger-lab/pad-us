import streamlit as st
import leafmap.maplibregl as leafmap
from cng.h3 import *
# from variables import *
from utils import *


## App Layout ## 
st.set_page_config(layout="wide",
                   page_title="TPL Conservation Almanac",
                   page_icon=":globe:")

'''
# TPL Conservation Almanac
A data visualization tool built for the Trust for Public Land
'''
from datetime import time

with st.sidebar:
    show_landvote = st.toggle('Track Landvote')
    style_choice = st.radio("Color by:", style_options)
    paint = style_options[style_choice]
    st.divider()

    st.markdown("Filters")
    state_choice = st.selectbox("State", states,index = 6, placeholder='Pick a state')
    one_state = state_choice[0] != 'All'
    counties = get_counties(state_choice)
    if one_state:
        county_choice = st.selectbox("County", counties, index = 0, placeholder='Select a county')
    else:
        county_choice = None
    year_range = st.slider(
    "Year", min_value = 1998, max_value = 2021, value=(1998, 2021)
)
    st.divider()
    st.markdown("Summary Charts")
    # show_landvote = st.toggle("Landvote Measures", key = 'landvote')
    show_mobi = st.toggle("Biodiversity", key = 'mobi')
    show_svi = st.toggle("SVI", key = 'svi')

# even if we only pick 1 state or 1 gap code, make them lists so it still works with our functions 
if isinstance(state_choice, str):
    state_choice = [state_choice]  # convert single string to list

## Respond to sidebar
m = leafmap.Map(style = "positron")

# get all the ids that correspond to the filter
# gdf = get_ids(state_choice, gap_choice, year_range)
gdf = get_ids(state_choice, county_choice, year_range)
ids = gdf.execute()['TPL_ID'].tolist()
unique_ids = list(set(ids))

m.add_pmtiles(pmtiles, style=tpl_style(unique_ids, paint), opacity=0.5, tooltip=True, fit_bounds=True)
legend, position, bg_color, fontsize = get_legend(paint)
m.add_legend(legend_dict = legend, position = position, bg_color = bg_color, fontsize = fontsize)
#zoom to state(s)
fit_bounds(state_choice, county_choice, m)


## Render display panels
col1, col2 = st.columns([2,1])

with col1:
    m.to_streamlit()

with col2:
    get_summary_chart(gdf,style_choice,st.session_state, paint)
    if show_landvote:
        landvote_df, group_col = get_landvote(gdf, style_choice)
        get_bar(landvote_df, style_choice, group_col, 'total_amount', paint)
        get_bar(landvote_df, style_choice, group_col, 'total_approved', paint)



######### carl's code
from ibis import _
@st.cache_resource
def tpl_database(parquet):
    # gdf = ibis.read_parquet(parquet)  
    # gdf = con.read_parquet(parquet).mutate(geom = _.geom.convert("EPSG:4326","ESRI:102039"))
    gdf = con.read_parquet(parquet)
    return gdf



df = tpl_database(tpl_geom_url)

# +

public_dollars, private_dollars, tribal_dollars, total_dollars = tpl_summary(df)

# + 
public_delta, private_delta, trib_delta = calc_delta(df)
# -


with st.container():
    col1, col2, col3, col4 = st.columns(4)
    col1.metric(label=f"Public", value=f"${public_dollars:,}", delta = f"{public_delta:}%")
    col2.metric(label=f"Private", value=f"${private_dollars:,}", delta = f"{private_delta:}%")
    col3.metric(label=f"Tribal", value=f"${tribal_dollars:,}", delta = f"{trib_delta:}%")
    col4.metric(label=f"Total", value=f"${total_dollars:,}")    

selected = style_options[style_choice]
column = selected["property"]
colors = dict(selected["stops"])


# +
area_totals = get_area_totals(df,column)


timeseries = calc_timeseries(df, column)

# +
st.divider()

with st.container():
    plt1, plt2 = st.columns(2)
    
    with plt1:
        "Total Area protected (hectares):"
        st.altair_chart(bar(area_totals, column, paint))
    with plt2:
        "Annual investment ($) in protected area"
        st.altair_chart(chart_time(timeseries, column, paint))


# # +


