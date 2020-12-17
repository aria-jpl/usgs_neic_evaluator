#!/usr/bin/env python

'''
Generates the AOI Tracks for coseismic events.

Looks at all acquisitions over the event displacement and calculates the union
for each track. This union defines the bounding box for the AOI Tracks.
'''

import json
import geojson
from shapely.ops import cascaded_union
import constants
import requests
import shapely.geometry
from shapely import wkt
from collections import defaultdict
from shapely.geometry import shape
from elasticsearch import Elasticsearch
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def main(event_polygon, extended_event_polygon):

    track_data = []
    count = 0
    tracks = defaultdict(dict)
    #GRQ_URL = 'https://100.67.35.28/es/'
    GRQ_URL = 'http://100.67.35.28:9200'

    grq = Elasticsearch(GRQ_URL, verify_certs=False)
    if not grq.ping():
       print("Failed to connect to host.")
       return 1

    '''
    get acquisitions from es
    compare return count with count from tosca
    '''

    # Convert the event polygon into a shapely polygon
    tmp_polygon = convertToPolygon(event_polygon)

    doc = {"query":{"filtered":{"query":{"bool":{"must":[{"term":{"dataset.raw":"acquisition-S1-IW_SLC"}}]}},"filter":{"geo_shape":{"location":{"shape":{"type":"polygon","coordinates":event_polygon}}}}}},"size":500,"sort":[{"_timestamp":{"order":"desc"}}],"fields":["_timestamp","_source"]}
    res = grq.search(index="grq_v2.0_acquisition-s1-iw_slc", body=doc)
    # Query scihub for acquisitions that intersect with event geojson
    #products = queryScihub(tmp_polygon)

    # Get track numbers and polygons for all acquisitions then create a list for each track and populate
    # the lists with acquisition polygons over those tracks
    #for product in products['products']:
    print("Number of acquisitions over event:")
    print(res['hits']['total'])
    for product in res['hits']['hits']:
        try:
            track_number, polygon, orbit_direction, acq_id = getAcqInfo(product["_source"])
            print("Track number: " + str(track_number))
            if track_number in tracks:
                tracks[track_number]["polygons"].append(polygon)
                tracks[track_number]["acq_id"].append(acq_id)
            else:
                tracks[track_number] = {"polygons": [],"orbit_direction": "", "acq_id": []}
                tracks[track_number]["polygons"].append(polygon)
                tracks[track_number]["orbit_direction"] = orbit_direction
                tracks[track_number]["acq_id"].append(acq_id)
            count = count + 1
        except:
            print("Failed to parse acquisition metadata for: {}".format(product))
            pass
    print("Number of acquisitions that intersect with event polygon: " + str(count))
    #print(tracks)
    tmp_intersect = convertToPolygon(extended_event_polygon)
    # Build track data objects {[track number, union polygon, orbit direction], ...}
    for track in tracks.copy():
        tmp = []
        boundary = cascaded_union(tracks[track]['polygons'])
        geojson = shapely.geometry.mapping(boundary)
        print(track)
        print(boundary)
        tmp_intersect = tmp_intersect.intersection(boundary)
        tmp_intersect_json = shapely.geometry.mapping(tmp_intersect)
        poly_json = json.dumps(tmp_intersect_json)
        print(poly_json)
        #print(tmp_intersect)
        track_json = json.dumps(geojson)
        tmp.append(track)
        tmp.append(track_json)
        tmp.append(tracks[track]['orbit_direction'])
        track_data.append(tmp)
        #print(tmp)
    print("This is what is sent out")
    tmp_intersect_json = shapely.geometry.mapping(tmp_intersect)
    poly_json = json.dumps(tmp_intersect_json)
    print(poly_json)
    aoi = poly_json
    print(track_data)
    return track_data, aoi

def queryScihub(polygon):
    uri = buildURI(polygon)
    print("SciHub query uri: {}".format(uri))
    response = requests.get(uri, auth=('hysds-dev', 's1a4aria'))
    if response.status_code != 200:
        response.raise_for_status()
    return json.loads(response.content)

def buildURI(polygon):
    return constants.scihub_root_uri + constants.scihub_geo_filter.format(polygon)

def convertToPolygon(event_polygon):
    event_geojson = {"type": "Polygon", "coordinates": None}
    event_geojson["coordinates"] = event_polygon
    s = json.dumps(event_geojson)
    poly = shape(geojson.loads(s))
    polygon = wkt.loads(str(poly))
    return polygon

def getAcqInfo(product):
    acq_id = product['id']
    track_number = product['metadata']['track_number']
    orbit_direction = product['metadata']['direction']
    polygon = convertToPolygon(product['location']['coordinates'][0])
    # acq_id = product['identifier']
    # indexes = product['indexes'][0]
    # children = indexes['children'][5]
    # track = indexes['children'][17]
    # orbit = indexes['children'][9]
    # track_number = track['value']
    # orbit_direction = orbit['value']
    # polygon = wkt.loads(children['value'])
    # print(polygon)
    return track_number, polygon, orbit_direction, acq_id
#
# if __name__ == '__main__':
#     polygon = [[[121.97079663164914,24.800134496080535],[121.97079663164914,24.822078622176527],[121.9918251503259,24.822078622176527],[121.9918251503259,24.800134496080535],[121.97079663164914,24.800134496080535]]]
#     extended_event_polygon = [[[121.53817199170591,23.22459912782818],[121.53817199170591,25.341862614792756],[122.88342453539373,25.341862614792756],[122.88342453539373,23.22459912782818],[121.53817199170591,23.22459912782818]]]
#     track_data = main(polygon, extended_event_polygon)
#     #print(track_data)
