import os
import ibis
from ibis import _
import ibis.selectors as s
from cng.utils import *
from cng.h3 import *
from minio import Minio
import streamlit 
from datetime import timedelta
import re
duckdb_install_h3()

con = ibis.duckdb.connect("duck.db",extensions = ["spatial", "h3"])
set_secrets(con)

# Get signed URLs to access license-controlled layers
key = st.secrets["MINIO_KEY"]
secret = st.secrets["MINIO_SECRET"]
client = Minio("minio.carlboettiger.info", key, secret)

tracts_z8 = con.read_parquet("https://minio.carlboettiger.info/public-social-vulnerability/2022-tracts-h3-z8.parquet").select('FIPS','h8').mutate(h8 = _.h8.lower()).rename(FIPS_tract = "FIPS")
pad_z8 = con.read_parquet("https://minio.carlboettiger.info/public-biodiversity/pad-us-4/pad-h3-z8.parquet")
mobi = con.read_parquet("https://minio.carlboettiger.info/public-mobi/hex/all-richness-h8.parquet").select("h8", "Z").rename(richness = "Z")
svi = con.read_parquet("https://minio.carlboettiger.info/public-social-vulnerability/2022/SVI2022_US_tract.parquet").select("FIPS", "RPL_THEMES").filter(_.RPL_THEMES > 0).rename(svi = "RPL_THEMES").rename(FIPS_tract = "FIPS")
tpl_geom_url = "s3://shared-tpl/tpl.parquet"
tpl_geom = con.read_parquet(tpl_geom_url).mutate(geom = _.geom.convert("ESRI:102039", "EPSG:4326"))
tpl_landvote_joined_url = "s3://shared-tpl/tpl_almanac_landvote_h3_z8.parquet"
tpl_z8_url = "s3://shared-tpl/tpl_h3_z8.parquet"

landvote = (con.read_parquet("s3://shared-tpl/landvote_h3_z8.parquet")
            .rename(FIPS_county = "FIPS", measure_amount = 'Conservation Funds Approved', 
                    measure_status = "Status", measure_purpose = "Purpose",)
            .mutate(measure_year = _.Date.year()).drop('Date','geom'))
            
tpl_drop_cols = ['Reported_Acres','Close_Date','EasementHolder_Name',
        'Data_Provider','Data_Source','Data_Aggregator',
        'Program_ID','Sponsor_ID']

tpl_z8 = con.read_parquet(tpl_z8_url).mutate(h8 = _.h8.lower()).drop(tpl_drop_cols)

select_cols = ['fid','TPL_ID','landvote_id',
'state','state_name','county',
 'FIPS_county', 'FIPS_tract',
 'city','jurisdiction',
 'Close_Year', 'Site_Name',
 'Owner_Name','Owner_Type',
 'Manager_Name','Manager_Type',
 'Purchase_Type','EasementHolder_Type',
 'Public_Access_Type','Purpose_Type',
 'Duration_Type','Amount',
 'Program_Name','Sponsor_Name',
 'Sponsor_Type','measure_year',
 'measure_status','measure_purpose',
 'measure_amount',
 'richness','svi',
 'h8']

 # 'h8','geom']
  # ]


database = (
  tpl_z8.drop('State','County')
  .left_join(landvote, "h8").drop('h8_right')
  .left_join(mobi, "h8").drop('h8_right')
  # .left_join(pad_z8, "h8").drop('h8_right')
  # .inner_join(pad_us, "row_n")
  .left_join(tracts_z8, "h8").drop('h8_right')
  .inner_join(svi, "FIPS_tract")
).select(select_cols).distinct()

database_geom = (database.drop('h8').distinct().inner_join(tpl_geom.select('geom','TPL_ID','fid','Shape_Area'), [database.TPL_ID == tpl_geom.TPL_ID, database.fid == tpl_geom.fid])
            .mutate(acres = _.Shape_Area*0.0002471054)
            # .mutate(area = _.geom.area())
           ).distinct()

pmtiles = client.get_presigned_url(
    "GET",
    "shared-tpl",
    # "tpl_v2.pmtiles",
    "tpl_almanac_landvote_h3_z8.pmtiles",
    expires=timedelta(hours=2),
)
source_layer_name = re.sub(r'\W+', '', os.path.splitext(os.path.basename(pmtiles))[0]) #stripping hyphens to get layer name 


states = (
    "All", "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut",
    "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa",
    "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan",
    "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire",
    "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
    "Oklahoma", "Oregon", "Pennsylvania", "Puerto Rico", "Rhode Island", "South Carolina",
    "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
    "West Virginia", "Wisconsin", "Wyoming"
)

# Define color hex codes
darkblue = "#00008B"
blue = "#0096FF"
lightblue = "#ADD8E6"
darkgreen = "#006400"
grey = "#c4c3c3"
dark_grey = "#5a5a5a"
green = "#008000"
purple = "#800080"
darkred = "#8B0000"
orange = "#FFA500"
red = "#e64242"
yellow = "#FFFF00"
pink = '#FFC0CB'

style_options = {
    "Manager Type":  {
            'property': 'Manager_Type',
            'type': 'categorical',
            'stops': [
                ['FED', darkblue],
                ['STAT', blue],
                ['LOC', lightblue],
                ['DIST', darkgreen],
                ['UNK', dark_grey],
                ['JNT', pink],
                ['TRIB', purple],
                ['PVT', darkred],
                ['NGO', orange]
            ]
            },
    "Access": {
        'property': 'Public_Access_Type',
        'type': 'categorical',
        'stops': [
            ['OA', green],
            ['XA', red],
            ['UK', dark_grey],
            ['RA', orange]
        ]
    },
    "Purpose": {
        'property': 'Purpose_Type',
        'type': 'categorical',
        'stops': [
            ['FOR', green],
            ['HIST', red],
            ['UNK', dark_grey],
            ['OTH', grey],
            ['FARM', yellow],
            ['REC', blue],
            ['ENV', purple],
            ['SCE', orange],
            ['RAN', pink]
        ]
    },
    "Year": {
        'property': 'Close_Year',
        'type': 'categorical',
        'stops': [
            [1998, '#1f77b4'],  # blue
            [1999, '#ff7f0e'],  # orange
            [2000, '#2ca02c'],  # green
            [2001, '#d62728'],  # red
            [2002, '#9467bd'],  # purple
            [2003, '#8c564b'],  # brown
            [2004, '#e377c2'],  # pink
            [2005, '#7f7f7f'],  # gray
            [2006, '#bcbd22'],  # olive
            [2007, '#17becf'],  # cyan
            [2008, '#aec7e8'],  # light blue
            [2009, '#ffbb78'],  # light orange
            [2010, '#98df8a'],  # light green
            [2011, '#ff9896'],  # light red
            [2012, '#c5b0d5'],  # light purple
            [2013, '#c49c94'],  # light brown
            [2014, '#f7b6d2'],  # light pink
            [2015, '#c7c7c7'],  # light gray
            [2016, '#dbdb8d'],  # light olive
            [2017, '#9edae5'],  # light cyan
            [2018, '#393b79'],  # dark blue
            [2019, '#637939'],  # dark green
            [2020, '#8c6d31'],  # dark brown
            [2021, '#843c39'],  # dark red
        ]
    },
    # "Landvote Status": {
    #     'property': 'measure_status',
    #     'type': 'categorical',
    #     'stops': [
    #         ['Pass', "#417d41"],
    #         ['Fail', "#f3d3b1"],  
    #     ]
    # },
}

style_choice_columns = {'Manager Type': style_options['Manager Type']['property'],
              'Access' : style_options['Access']['property'],
              'Purpose': style_options['Purpose']['property'],
              'Year': style_options['Year']['property'],
              # 'Landvote Status':style_options['Landvote Status']['property'],
             }

metric_columns = {'svi': 'svi', 'mobi': 'richness', 'landvote':'measure_status'}


notused = {
    "Amount": ["interpolate",
                ['exponential', 1], 
                ["get", "Amount"],
                       0,	"#FCE2DC",
                34273487,	"#F8C3BF",
                68546973,	"#F4A5A2",
                102820460,	"#F08785",
                137093947,	"#EB6968",
                171367433,	"#DB5157",
                205640920,	"#BE4152",
                239914407,	"#A0304C",
                274187893,	"#832047",
                308461380,	"#661042",
                ] 
}

from langchain_openai import ChatOpenAI
import streamlit as st
# from langchain_openai.chat_models.base import BaseChatOpenAI

## dockerized streamlit app wants to read from os.getenv(), otherwise use st.secrets
import os
api_key = os.getenv("NRP_API_KEY")
if api_key is None:
    api_key = st.secrets["NRP_API_KEY"]

llm_options = {
    # "llama-3.3-quantized": ChatOpenAI(model = "cirrus", api_key=st.secrets['CIRRUS_LLM_API_KEY'], base_url = "https://llm.cirrus.carlboettiger.info/v1",  temperature=0),
    "llama3.3": ChatOpenAI(model = "llama3-sdsc", api_key=api_key, base_url = "https://llm.nrp-nautilus.io/",  temperature=0),
    "gemma3": ChatOpenAI(model = "gemma3", api_key=api_key, base_url = "https://llm.nrp-nautilus.io/",  temperature=0),
    # "DeepSeek-R1-Distill-Qwen-32B": BaseChatOpenAI(model = "DeepSeek-R1-Distill-Qwen-32B", api_key=api_key, base_url = "https://llm.nrp-nautilus.io/",  temperature=0),
    "watt": ChatOpenAI(model = "watt", api_key=api_key, base_url = "https://llm.nrp-nautilus.io/",  temperature=0),
    # "phi3": ChatOpenAI(model = "phi3", api_key=api_key, base_url = "https://llm.nrp-nautilus.io/",  temperature=0),
}
