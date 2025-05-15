import ibis
from ibis import _
from variables import *

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
        
def get_ids(state_choice, gap_choice):
    gdf = database.filter(_.GAP_Sts.isin(gap_choice))
    if state_choice[0] != "All":
        gdf = gdf.filter(_.State.isin(state_choice))
    return gdf.execute()


def fit_bounds(state_choice, m):
    if state_choice[0] != "All":
        gdf = tpl.filter(_.State.isin(state_choice))
        bounds = list(gdf.execute().total_bounds)
        m.fit_bounds(bounds) # need to zoom to filtered area
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

