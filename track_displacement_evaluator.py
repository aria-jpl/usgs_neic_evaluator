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

def main(event_polygon):
    track_data = []
    count = 0
    tracks = defaultdict(dict)

    # Convert the event polygon into a shapely polygon
    tmp_polygon = convertToPolygon(event_polygon)

    # Query scihub for acquisitions that intersect with event geojson
    products = queryScihub(tmp_polygon)

    # Get track numbers and polygons for all acquisitions then create a list for each track and populate
    # the lists with acquisition polygons over those tracks
    for product in products['products']:
        track_number, polygon, orbit_direction, acq_id = getAcqInfo(product)
        if track_number in tracks:
            tracks[track_number]["polygons"].append(polygon)
            tracks[track_number]["acq_id"].append(acq_id)
        else:
            tracks[track_number] = {"polygons": [],"orbit_direction": "", "acq_id": []}
            tracks[track_number]["polygons"].append(polygon)
            tracks[track_number]["orbit_direction"] = orbit_direction
            tracks[track_number]["acq_id"].append(acq_id)
        count = count + 1

    print("Number of acquisitions that intersect with event polygon: " + str(count))

    # Build track data objects {[track number, union polygon, orbit direction], ...}
    for track in tracks.copy():
        tmp = []
        boundary = cascaded_union(tracks[track]['polygons'])
        geojson = shapely.geometry.mapping(boundary)
        track_json = json.dumps(geojson)
        tmp.append(track)
        tmp.append(track_json)
        tmp.append(tracks[track]['orbit_direction'])
        track_data.append(tmp)
        print(tmp)
    print("This is what is sent out")
    print(track_data)
    return track_data

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
    polygon = shape(geojson.loads(s))
    return polygon

def getAcqInfo(product):
    try:
        acq_id = product['identifier']
        indexes = product['indexes'][0]
        children = indexes['children'][5]
        track = indexes['children'][17]
        orbit = indexes['children'][9]
        track_number = track['value']
        orbit_direction = orbit['value']
        polygon = wkt.loads(children['value'])
    except:
        print("Failed to parse acquisition metadata for: {}".format(product))
        return 1
    return track_number, polygon, orbit_direction, acq_id