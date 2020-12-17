#!/usr/bin/env python

'''
Takes in the usgs neic event object, then determines  if it
is relevant above the input filter criteria. If it passes
this filter, an aoi type for the event is created and submitted
to create_aoi
'''
from __future__ import division

from builtins import range
from past.utils import old_div
import os
import re
import sys
import json
import math
import argparse
import datetime
import dateutil.parser
from shapely.geometry import Polygon
import requests
import submit_create_aoi
import submit_slack_notification
import track_displacement_evaluator
import pytz

def main(event_path, depth_filter=None, mag_filter=None, alertlevel_filter=None, polygon_filter=None, slack_notification=None, water_filter=False, dynamic_threshold=False, create_aoi_version='master', days_pre_event=30, days_post_event=30):
    '''runs the filters on the input event. If it passes it generates appropriate metadata and submits the aoi'''
    event = get_event(event_path)
    print('found event: {0}'.format(event))
    # calculate relevant event information such as mag, extent, etc
    event_info = calculate_event_info(event)
    # determine if the event passes the requisite filters
    if not pass_filters(event_info, depth_filter, mag_filter, alertlevel_filter, polygon_filter, water_filter, dynamic_threshold):
        return

    # call displacement code
    event_tracks, aoi = track_displacement_evaluator.main(event['location']['coordinates'], event_info['location']['coordinates'])

    # submit job for event AOI
    params = build_params(event, event_info, days_pre_event, days_post_event, aoi, False)
    print("AOI:")
    print(params)
    # submit the aoi
    submit_create_aoi.main(params, create_aoi_version, 'factotum-job_worker-small', '8', 'create_neic_event_aoi')

    for event_track in event_tracks:
        # process the aoi params
        params = build_params(event, event_info, days_pre_event, days_post_event, event_track, True)
        print(params)
        #submit the aoi
        submit_create_aoi.main(params, create_aoi_version, 'factotum-job_worker-small' , '8', 'create_neic_event_aoi')

    #run slack notification
    #mlucas if slack_notification:
    #mlucas    run_slack_notification(event, slack_notification)

def pass_filters(event_info, depth_filter, mag_filter, alertlevel_filter, polygon_filter, water_filter, dynamic_threshold):
    '''runs all requisite filters, returning true if it needs to process, false if not'''
    #if it's a test, just pass it
    if event_info['id'] == 'USGS_NEIC_us1000test':
        return True
    #run polygon filter
    if polygon_filter:
        if not run_polygon_filter(event_info, polygon_filter):
            return False
    #run depth filter
    if depth_filter:
        if not run_depth_filter(event_info, float(depth_filter)):
            print('event failed depth filter.')
            return False
    # run water filter
    if water_filter:
        if not run_water_filter(event_info, float(water_filter)):
            print('event failed water mask.')
            return False
        print('event passed water mask.')
    # run dynamic thresholding
    #if dynamic_threshold:
    #    if run_dynamic_threshold(event_info):
    #        print('event meets dynamic threshold, submitting event.')
    #        return True
    #    else:
    #        print('event does not meet dynamic threshold. not submitting event.')
    #        return False
    if mag_filter: # run magnitude filter
        if event_info['mag'] >= mag_filter:
            print('event passes magnitude filter, processing')
            return True
        else:
            print('event fails magnitude filter. not processing')
            return False
    if alertlevel_filter: # run alertlevel filter
        if alertlevel_reaches(event_info['alert'], alertlevel_filter):
            print('event passes alertlevel filter, processing')
            return True
        else:
            print('event fails alertlevel filter. not processing.')
            return False
    print('event has not been excluded by filters. processing.')
    return True

def calculate_event_info(event):
    '''builds a dict of relevant event information, such as magnitude, region, etc, returns it as a dict'''
    event_id = get_met(event, 'id')
    event_mag = float(get_met(event, 'mag'))
    event_alertlevel = get_met(event, 'alert')
    event_location = get_met(event, 'epicenter')
    event_lat = event_location['coordinates'][1]
    event_lon = event_location['coordinates'][0]
    event_depth = float(event['metadata']['geometry']['coordinates'][2])
    #determine event extent
    event_geojson = determine_extent(event_lat, event_lon, event_mag)
    # call displacement_evaluator here
    return {'id':event_id, 'mag':event_mag, 'depth':event_depth, 'alertlevel':event_alertlevel, 'location':event_geojson, 'lat':event_lat, 'lon':event_lon}

def run_water_filter(event_info, amount):
    '''returns True if it passes the mask or fails to load/run the mask'''
    try:
       # lazy loading
       import lightweight_water_mask
       if lightweight_water_mask.get_land_area(event_info['location']) > amount:
            return True
    except Exception as err:
        print('Failed on water masking: {}'.format(err))
        return True
    return False

def run_depth_filter(event_info, depth_filter):
    '''returns True if it passes the mask, False otherwise. True == depth < depth_filter'''
    depth = float(event_info['depth'])
    if depth >= depth_filter:
        return False
    return True

def run_polygon_filter(event_info, polygon_filter):
    '''runs the event through a spatial filter. returns True if it passes, False otherwise.'''
    if type(polygon_filter) is str:
        polygon_filter = json.loads(polygon_filter) 
    event_geojson = event_info['location']['coordinates']
    if is_overlap(polygon_filter, event_geojson):
        return True
    return False

def is_overlap(geojson1, geojson2):
    '''returns True if there is any overlap between the two geojsons. The geojsons
    are just a list of coordinate tuples'''
    p1=Polygon(geojson1)
    p2=Polygon(geojson2)
    return p1.intersects(p2)

def run_dynamic_threshold(event_info):
    '''runs a series of filters, designed to pick up relevant events'''
    event_mag = event_info['mag']
    event_alertlevel = event_info['alertlevel']
    if event_mag >= 7.0:
        return True
    if event_mag >= 6.0 and alertlevel_reaches(event_alertlevel, 'yellow'):
        return True
    if alertlevel_reaches(event_alertlevel, 'red'):
        return True
    return False

def alertlevel_reaches(event_level, comparison_level):
    '''looks to see if the event alert level is at or above the comparison level, returns true if is is
    false otherwise'''
    if event_level is None:
        return False
    alert_dict = {'green':1, 'yellow':2, 'orange':3, 'red':4}
    if alert_dict[event_level] < alert_dict[comparison_level]:
        return False
    return True

def get_event(event_path):
    '''loads the event json as a dict'''
    event_filename = os.path.basename(event_path)
    event_ds_path = os.path.join(event_path, event_filename + '.dataset.json')
    event_met_path = os.path.join(event_path, event_filename + '.met.json')
    cwd = os.getcwd()
    event_object = {}
    with open(event_ds_path) as f:
        event_object = json.load(f)
    with open(event_met_path) as f:
        event_object['metadata'] = json.load(f)
    return event_object

def get_met(product, key):
    if key in list(product.keys()):
        return product[key]
    if '_source' in list(product.keys()) and key in list(product['_source'].keys()):
        return product['_source'][key]
    if '_source' in list(product.keys()) and 'metadata' in list(product['_source'].keys()) and key in list(product['_source']['metadata'].keys()):
        return product['_source']['metadata'][key]
    if 'metadata' in list(product.keys()) and key in list(product['metadata'].keys()):
        return product['metadata'][key]
    if 'metadata' in list(product.keys()) and 'properties' in list(product['metadata'].keys()) and key in list(product['metadata']['properties'].keys()):
        return product['metadata']['properties'][key]
    if 'properties' in list(product.keys()) and key in product['properties']:
        return product['properties'][key]
    return False

def shift(lat, lon, bearing, distance):
    R = 6378.1  # Radius of the Earth
    bearing = old_div(math.pi * bearing, 180)  # convert degrees to radians
    lat1 = math.radians(lat)  # Current lat point converted to radians
    lon1 = math.radians(lon)  # Current long point converted to radians
    lat2 = math.asin(math.sin(lat1) * math.cos(old_div(distance, R)) +
                     math.cos(lat1) * math.sin(old_div(distance, R)) * math.cos(bearing))
    lon2 = lon1 + math.atan2(math.sin(bearing) * math.sin(old_div(distance, R)) * math.cos(lat1),
                             math.cos(old_div(distance, R)) - math.sin(lat1) * math.sin(lat2))
    lat2 = math.degrees(lat2)
    lon2 = math.degrees(lon2)
    return [lon2, lat2]

def determine_extent(lat, lon, mag):
    lat = float(lat)
    lon = float(lon)
    mag = float(mag)
    distance = (mag - 5.0) / 2.0 * 150
    l = list(range(0, 361, 20))
    coordinates = []
    for b in l:
        coords = shift(lat, lon, b, distance)
        coordinates.append(coords)
    return {"coordinates": [coordinates], "type": "Polygon"}

def build_params(event, event_info, days_pre_event, days_post_event, event_track, isTrack):
    '''builds parameters for a job submission from the event, which creates the aoi,
    and returns those parameters'''
    #loads the config json
    current_dir = os.path.dirname(os.path.realpath(__file__))
    params_path = os.path.join(current_dir, 'config', 'aoi_params.json')
    params = json.load(open(params_path, 'r'))
    aoi_name = build_aoi_name(event, event_info, isTrack)
    #geojson_polygon = event_info['location']
    aoi_event_time = get_met(event, 'starttime')
    starttime = determine_time(aoi_event_time, -1 * float(days_pre_event))
    eventtime = get_met(event, 'starttime')
    endtime = determine_time(aoi_event_time, float(days_post_event))
    aoi_image_url = parse_browse_url(event)     
    event_metadata = build_event_metadata(event, event_info) #builds additional metadata to be displayed
    params['name'] = aoi_name + "_" + event_track[0]
    params['geojson_polygon'] = json.loads(str(event_track[1]))
    params['track_number'] = event_track[0]
    params['orbit_direction'] = event_track[2]
    params['starttime'] = starttime
    params['eventtime'] = eventtime
    params['endtime'] = endtime
    params['additional_metadata']['image_url']= aoi_image_url
    params['additional_metadata']['event_metadata'] = event_metadata
    #load account and username from context
    context = load_json('_context.json')
    params['account'] = context['account']
    params['username'] = context['username']
    return params

def load_json(file_path):
    '''load the file path into a dict and return the dict'''
    with open(file_path, 'r') as json_data:
        json_dict = json.load(json_data)
        json_data.close()
    return json_dict

def build_event_metadata(event, event_info):
    '''builds info that goes into the aoi met, event_metadata field, that is displayed'''
    event_met = {}
    event_met['event id'] = event_info['id']
    event_met['magnitude'] = event_info['mag']
    event_met['depth'] = event_info['depth']
    event_met['location'] = get_met(event, 'place')
    event_met['latitude'] = event_info['lat']
    event_met['longitude'] = event_info['lon']
    event_met['label'] = get_met(event, 'title')
    try:
        event_met['time'] = convert_epoch_time_to_utc(get_met(event, 'time'))
    except:
        pass
    event_met['pager_status'] = event_info['alertlevel']
    event_met['tsunami warning'] = get_met(event, 'tsunami')
    event_met['usgs information'] = 'https://earthquake.usgs.gov/earthquakes/eventpage/{0}'.format(event_info['id'])
    return event_met

def build_aoi_name(event, event_info, isTrack):
    '''attempts to build a readable event name'''
    if isTrack:
        try:
            id_str = get_met(event, 'id')
            place = get_met(event, 'place')
            regex = re.compile(' of (.*)[,]? (.*)')
            match = re.search(regex, place)
            location_str = '{0}_{1}'.format(match.group(1), match.group(2))
            location_str = location_str.replace(',','')
            mag = get_met(event, 'mag')
            mag_str = "{0:0.1f}".format(float(mag))
            return 'AOITRACK_eq_usgs_neic_pdl_{0}_{1}_{2}'.format(id_str, mag_str, location_str)
        except:
            return 'AOITRACK_eq_usgs_neic_pdl_{0}'.format(event_info['id'])
    else:
        try:
            id_str = get_met(event, 'id')
            place = get_met(event, 'place')
            regex = re.compile(' of (.*)[,]? (.*)')
            match = re.search(regex, place)
            location_str = '{0}_{1}'.format(match.group(1), match.group(2))
            location_str = location_str.replace(',','')
            mag = get_met(event, 'mag')
            mag_str = "{0:0.1f}".format(float(mag))
            return 'AOI_monitoring_{0}_{1}_{2}'.format(id_str, mag_str, location_str)
        except:
            return 'AOI_monitoring_{0}'.format(event_info['id'])

def convert_epoch_time_to_utc(epoch_timestring):
    dt = datetime.datetime.utcfromtimestamp(epoch_timestring).replace(tzinfo=pytz.UTC)
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] # use microseconds and convert to milli

def determine_time(time_string, offset):
    initial_time = dateutil.parser.parse(time_string).replace(tzinfo=pytz.UTC)
    final_time = initial_time + datetime.timedelta(days=offset)
    return final_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]

def parse_browse_url(event):
    '''Pull the event detail json from the feed and attempt to extract the shakemap. Return None if fails.'''
    try:
        url = event['properties']['detail']
        session = requests.session()
        response = session.get(url)
        json_data = json.loads(response.text)
        browse_url = json_data['properties']['products']['shakemap'][0]['contents']['download/tvmap.jpg']['url']
        return browse_url
    except:
        print('Failed to parse browse url')
        return None

def build_longlabel(event):
    estr = get_met(event, 'place') #ex: "69km WSW of Kirakira, Solomon Islands"
    regex = re.compile(' of (.*)[,]? (.*)')
    match = re.search(regex, estr)
    if match:
        product_name = '%s %s' % (match.group(1),match.group(2))
    else:
        product_name = estr
    product_name = product_name.replace(' ','_')
    return product_name.replace(',','')

def run_slack_notification(event, slack_notification):
    '''submit slack webhook, requires slack notification key'''
    event_vals = get_met(event, 'metadata')
    submit_slack_notification.slack_notify(event_vals, slack_notification)

def parser():
    '''
    Construct a parser to parse arguments
    @return argparse parser
    '''
    parse = argparse.ArgumentParser(description="Run PAGER query with given parameters")
    parse.add_argument("-e", "--event_path", required=True, help="path to the event file", dest="event_path")
    parse.add_argument("-t", "--depth_filter", required=False, default=None, help="Maximum depth filter in km", dest="depth_filter")
    parse.add_argument("-m", "--mag_filter", required=False, default=None, help="Minimum magnitude filter", dest="mag_filter")
    parse.add_argument("-a", "--alertlevel_filter", required=False, default=None, help="Minium pager alert level filter", choices=['green', 'yellow', 'orange', 'red'], dest="alertlevel_filter")
    parse.add_argument("-p", "--polygon_filter", required=False, default=None, help="Geojson polygon filter", dest="polygon_filter")
    parse.add_argument("-s", "--slack_notification", required=False, default=False, help="Key for slack notification, will notify via slack if provided.", dest="slack_notification")
    parse.add_argument("-w", "--water_filter", required=False, default=False, help="Water filter. If provided, use minimum number of square kilometers in the aoi required to pass the filter.", dest="water_filter")
    parse.add_argument("-d", "--dynamic_threshold", required=False, default=False, action='store_true', help="Flag for whether a dynamic threshold is used. Takes priority over pager & mag filters.", dest="dynamic_threshold")
    parse.add_argument("-r", "--create_aoi_version", required=False, default='master', help="Version of create_aoi to submit", dest="create_aoi_version")
    parse.add_argument("--days_pre_event", required=False, default=30, help="Days for the AOI to span pre-event", dest="days_pre_event")
    parse.add_argument("--days_post_event", required=False, default=30, help="Days for the AOI to span post-event", dest="days_post_event")
    return parse

if __name__ == '__main__':
    args = parser().parse_args()
    main(event_path=args.event_path, depth_filter=args.depth_filter, mag_filter=args.mag_filter, alertlevel_filter=args.alertlevel_filter, polygon_filter=args.polygon_filter, slack_notification=args.slack_notification, water_filter=args.water_filter, dynamic_threshold=args.dynamic_threshold, create_aoi_version=args.create_aoi_version, days_pre_event=args.days_pre_event, days_post_event=args.days_post_event) 
