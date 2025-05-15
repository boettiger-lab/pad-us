import os
import ibis
from ibis import _
from cng.utils import *
from cng.h3 import *
duckdb_install_h3()


con = ibis.duckdb.connect(extensions = ["spatial", "h3"])
# install_h3(con)
set_secrets(con)

endpoint = os.environ["AWS_S3_ENDPOINT"]

pad_pmtiles   = f"https://{endpoint}/public-biodiversity/pad-us-4/pad-us-4.pmtiles"
tract_pmtiles = f"https://{endpoint}/public-social-vulnerability/2022/SVI2022_US_tract.pmtiles"
pad_us    = con.read_parquet(f"https://{endpoint}/public-biodiversity/pad-us-4/pad-us-4.parquet").select("GAP_Sts", "Mang_Type", "row_n","State_Nm")
tracts_z8 = con.read_parquet(f"https://{endpoint}/public-social-vulnerability/2022-tracts-h3-z8.parquet").select('FIPS','h8').mutate(h8 = _.h8.lower())
pad_z8    = con.read_parquet(f"https://{endpoint}/public-biodiversity/pad-us-4/pad-h3-z8.parquet")
mobi      = con.read_parquet(f"https://{endpoint}/public-mobi/hex/all-richness-h8.parquet").select("h8", "Z").rename(richness = "Z")
svi       = con.read_parquet(f"https://{endpoint}/public-social-vulnerability/2022/SVI2022_US_tract.parquet").select("FIPS", "RPL_THEMES").filter(_.RPL_THEMES > 0)


from minio import Minio
import streamlit 
from datetime import timedelta

# Get signed URLs to access license-controlled layers
key = st.secrets["MINIO_KEY"]
secret = st.secrets["MINIO_SECRET"]
client = Minio("minio.carlboettiger.info", key, secret)

pmtiles = client.get_presigned_url(
    "GET",
    "shared-tpl",   
    # "tpl.pmtiles",
    "tpl_v2.pmtiles",
    # "tpl_h3_z8.pmtiles",
    expires=timedelta(hours=2),
)


tpl_parquet = client.get_presigned_url(
    "GET",
    "shared-tpl",
    "tpl_h3_z8.parquet",
    expires=timedelta(hours=2),
)

tpl = con.read_parquet(tpl_parquet).mutate(h8 = _.h8.lower())

database = (
  tpl
  .left_join(mobi, "h8")
  .left_join(pad_z8, "h8")
  .inner_join(pad_us, "row_n")
  .left_join(tracts_z8, "h8")
  .inner_join(svi, "FIPS")
)

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
blue = "#0000FF"
lightblue = "#ADD8E6"
darkgreen = "#006400"
grey = "#808080"
green = "#008000"
purple = "#800080"
darkred = "#8B0000"
orange = "#FFA500"
red = "#FF0000"
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
                ['UNK', grey],
                ['JNT', green],
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
            ['UK', grey],
            ['RA', orange]
        ]
    },
    "Purpose": {
        'property': 'Purpose_Type',
        'type': 'categorical',
        'stops': [
            ['FOR', green],
            ['HIST', red],
            ['UNK', grey],
            ['OTH', grey],
            ['FARM', yellow],
            ['REC', blue],
            ['ENV', purple],
            ['SCE', orange],
            ['RAN', pink]
        ]
    }
}
