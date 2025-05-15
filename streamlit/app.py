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

    style_choice = st.radio("Color by:", style_options)
    paint = style_options[style_choice]
    st.divider()

    st.markdown("Filters")

    gap_choice = st.pills(
        "GAP Codes",
        ["1", "2", "3", "4"],
        default=["1", "2"],
        selection_mode="multi"
    )
    print(gap_choice)
    state_choice = st.selectbox("State selection", states,index = 6)
    # state_choice = st.multiselect("State selection", states, default = "All",placeholder='Select an option')

    year_range = st.slider(
    "Year", min_value = 1998, max_value = 2021, value=(1998, 2021)
)
    st.divider()
    st.markdown("Summary Charts")
    show_mobi = st.toggle("Biodiversity", key = 'mobi')
    show_svi = st.toggle("SVI", key = 'svi')

# even if we only pick 1 state or 1 gap code, make them lists so it still works with our functions 
if isinstance(state_choice, str):
    state_choice = [state_choice]  # convert single string to list

## Respond to sidebar
m = leafmap.Map(style = "positron")

# get all the ids that correspond to the filter
gdf = get_ids(state_choice, gap_choice, year_range)
ids = gdf.execute()['TPL_ID'].tolist()
unique_ids = list(set(ids))


m.add_pmtiles(pmtiles, style=tpl_style(unique_ids, paint), opacity=0.5, tooltip=True, fit_bounds=True)
legend, position, bg_color, fontsize = get_legend(paint)
m.add_legend(legend_dict = legend, position = position, bg_color = bg_color, fontsize = fontsize)
#zoom to state(s)
fit_bounds(state_choice, m)


## Render display panels
col1, col2 = st.columns([2,1])

with col1:
    m.to_streamlit()

with col2:
    get_summary_chart(gdf,style_choice,st.session_state, paint)
