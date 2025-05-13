import streamlit as st
import leafmap.maplibregl as leafmap
import ibis
from ibis import _
import os
from cng.utils import *
from cng.h3 import *

con = ibis.duckdb.connect(extensions = ["spatial", "h3"])
# install_h3(con)
set_secrets(con)
endpoint = os.environ["AWS_S3_ENDPOINT"]


## Initialize Data ##
pad_pmtiles   = f"https://{endpoint}/public-biodiversity/pad-us-4/pad-us-4.pmtiles"
tract_pmtiles = f"https://{endpoint}/public-social-vulnerability/2022/SVI2022_US_tract.pmtiles"
pad_us    = con.read_parquet(f"https://{endpoint}/public-biodiversity/pad-us-4/pad-us-4.parquet").select("GAP_Sts", "Mang_Type", "row_n", "geom", "GIS_Acres","State_Nm")
tracts_z8 = con.read_parquet(f"https://{endpoint}/public-social-vulnerability/2022-tracts-h3-z8.parquet").mutate(h8 = _.h8.lower())
pad_z8    = con.read_parquet(f"https://{endpoint}/public-biodiversity/pad-us-4/pad-h3-z8.parquet")
mobi      = con.read_parquet(f"https://{endpoint}/public-mobi/hex/all-richness-h8.parquet").select( "h8", "Z").rename(richness = "Z")
svi       = con.read_parquet(f"https://{endpoint}/public-social-vulnerability/2022/SVI2022_US_tract.parquet").select("FIPS", "RPL_THEMES").filter(_.RPL_THEMES > 0)

database = (
  tracts_z8
  .left_join(mobi, "h8")
  .left_join(pad_z8, "h8")
  .inner_join(svi, "FIPS")
  .left_join(pad_us, "row_n")
)

states = (
    "All", "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA",
    "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "PR", "RI", "SC", "SD", "TN", "TX", "UT", "VT",
    "VA", "WA", "WV", "WI", "WY"
)

gap_paint = {
        'property': 'GAP_Sts',
        'type': 'categorical',
        'stops': [
            ["1", "#26633d"],
            ["2", "#879647"],
            ["3", "#BBBBBB"],
            ["4", "#F8F8F8"],
        ]
        }


def pad_style(gap_codes,state):
    if state[0] == "All":
        state = list(states[1:])  # pick all states

    style =  {
    "version": 8,
    "sources": {
        "pad": {
            "type": "vector",
            "url": "pmtiles://" + pad_pmtiles,
            "attribution": "US PAD v4"
        },
    },
    "layers": [{
            "id": "public",
            "source": "pad",
            "source-layer": "padus4",
            "type": "fill",
            'filter': [
                'all',
                ['match', ['get', 'State_Nm'], [*state], True, False],
                ['match', ['get', 'GAP_Sts'], gap_codes, True, False],
            ],
            "paint": {
                "fill-color": gap_paint,
                "fill-opacity": 0.5
            }
        }]
    }
    return style


## App Layout ## 
st.set_page_config(layout="wide", page_title="CA Protected Areas Explorer", page_icon=":globe:")


with st.sidebar:
    hex_mode = st.toggle("hex mode")
    gap_codes = st.pills(
        "GAP Codes",
        ["1", "2", "3", "4"],
        default=["1", "2"],
        selection_mode="multi"
    )
    state = st.selectbox("State selection",states, index = 6)

    # even if we only pick 1 state or 1 gap code, make them lists so it still works with our functions 
    if isinstance(state, str):
        state = [state]  # convert single string to list

    if isinstance(gap_codes, str):
        gap_codes = [gap_codes]

## Respond to sidebar
m = leafmap.Map(style = "positron")
m.add_pmtiles(pad_pmtiles, style=pad_style(gap_codes,state), opacity=0.5, tooltip=True, fit_bounds=True)

# get bounds for the state(s) we selected
if state[0] != "All":
    bounds = pad_us.filter(_.State_Nm.isin([*state])).execute().total_bounds
    m.fit_bounds(list(bounds)) # need to zoom to filtered area

## Render display panels
col1, col2 = st.columns([2,1])

with col1:
    m.to_streamlit()