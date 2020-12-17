# SciHub API
scihub_root_uri = "https://scihub.copernicus.eu/dhus/api/stub/products"
scihub_geo_filter = "?filter=(%20footprint:%22Intersects({0})%22)%20AND%20(%20%20(platformname:Sentinel-1%20AND%20producttype:SLC%20AND%20sensoroperationalmode:IW))&offset=0&limit=1000&sortedby=ingestiondate&order=desc"

# GRQ params
GRQ_ACQUISITION_INDEX = "grq_v2.0_acquisition-s1-iw_slc"
SIZE = 1000