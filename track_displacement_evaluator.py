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
    tracks = defaultdict(list)

    tmp_polygon = convertToPolygon(event_polygon)

    # Query scihub for acquisitions that intersect with event geojson
    products = queryScihub(tmp_polygon)

    # Get track numbers and polygons for all acquisitions then create a list for each track and populate
    # the lists with acquisition polygons over those tracks
    for product in products['products']:
        track_number, polygon = getAcqInfo(product)
        tracks[track_number].append(polygon)
        count = count + 1
    print("Number of acquisitions that intersect with event polygon: " + str(count))

    # Build track data objects
    for track in tracks:
        tmp = []
        boundary = cascaded_union(tracks[track])
        geojson = shapely.geometry.mapping(boundary)
        track_json = json.dumps(geojson['coordinates'])
        tmp.append(track)
        tmp.append(track_json)
        tmp.append("orbit_direction")
        track_data.append(tmp)

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
        indexes = product['indexes'][0]
        children = indexes['children'][5]
        track = indexes['children'][17]
        track_number = track['value']
        polygon = wkt.loads(children['value'])
    except:
        print("Failed to parse acquisition metadata for: {}".format(product))
        return 1
    return track_number, polygon

#if __name__ == "__main__":
    #parser = argparse.ArgumentParser(description=__doc__)
    #parser.add_argument('-e', '--geojson', help='Event displacement geojson', dest='event_json', required=True)
    #args = parser.parse_args()
    #event_polygon = [[[-67.0533,-23.502637517046526],[-66.72651874777976,-23.555109606067123],[-66.43843413693213,-23.706314260514464],[-66.22339804073741,-23.938314065110365],[-66.10745891978212,-24.22347517092413],[-66.1051808443756,-24.527640451561513],[-66.2176293332974,-24.814135202132107],[-66.43187347642761,-25.048178536616874],[-66.72223616254469,-25.201180508738645],[-67.0533,-25.254362482953468],[-67.38436383745531,-25.201180508738645],[-67.67472652357239,-25.048178536616874],[-67.8889706667026,-24.814135202132107],[-68.00141915562439,-24.527640451561513],[-67.99914108021787,-24.22347517092413],[-67.88320195926258,-23.938314065110365],[-67.66816586306786,-23.706314260514464],[-67.38008125222024,-23.555109606067123],[-67.0533,-23.502637517046526]]]
    #track_data = main(event_polygon)
    #print("This is the final result:")
    #print(track_data)