#!/usr/bin/env python

'''
Submits a notification to the ARIA slack channel
'''
import re
import json
import argparse
import requests
import datetime

from hysds.celery import app

def slack_notify(event, key):
    '''
    submits a slack notification
    '''
    simple_channel_notification(event, key)
    slack_url = 'https://hooks.slack.com/services/{0}'.format(key)
    mag = event['properties']['mag']
    place = event['properties']['place']
    pager_status = event['properties']['alert']
    usgs_url = event['properties']['url']
    usgs_formatted_url = '<%s|USGS event page>' % usgs_url
    eventid = event['id']
    tsunami = event['properties']['tsunami']
    tsunami_status = 'negative'
    if tsunami != 0 and tsunami != '0':
        tsunami_status = 'positive'
    #determine hex color
    green_hex = '#40e54b'
    red_hex = '#f23e26'
    yellow_hex = '#fffb38'
    orange_hex = '#ffa735'
    color = green_hex
    if pager_status == 'red':
        color = red_hex
    elif pager_status == 'orange':
        color = orange_hex
    elif pager_status == 'yellow':
        color = yellow_hex
    productname = parse_product_name(event)
    event_dt = datetime.datetime.fromtimestamp(float(event['properties']['time']) / 1000)
    event_time = event_dt.strftime("%Y-%m-%dT%H:%M:%S")
    aoi_name = 'earthquake_%s_%s_%s' % (eventid, event_time[:10], productname.replace(' ','_'))
    readable_event_time = event_dt.strftime("%H:%M.%S")
    now = datetime.datetime.now()
    delta = now - event_dt
    min_ago = str(int(delta.total_seconds()/60))
    aria_base_url = app.conf['GRQ_ES_URL'].rstrip(':9200').replace('http://', 'https://')
    aria_url = "<" + aria_base_url + "/search/?source={%22query%22:{%22bool%22:{%22must%22:[{%22term%22:{%22dataset_type.raw%22:%22area_of_interest%22}},{%22query_string%22:{%22query%22:%22\%22" + aoi_name + "\%22%22,%22default_operator%22:%22OR%22}}]}},%22sort%22:[{%22_timestamp%22:{%22order%22:%22desc%22}}],%22fields%22:[%22_timestamp%22,%22_source%22]}|ARIA generated AOI>"
    #title
    text = "Automated Earthquake event notification"
    title = 'Alert: Magnitude %s detected %s with status: %s @channel' % (mag, place, pager_status)
    title_link = usgs_url
    attachment_text = "Event time: %s (%s min ago)\n Tsunami prediction: %s.\n%s\n%s" % (readable_event_time, min_ago, tsunami_status, aria_url, usgs_formatted_url)
    attachments = [{"title": title, "title_link": title_link, "text": attachment_text, "color": color}]
    slack_payload = {"text": text, "attachments": attachments}
    json_payload = json.dumps(slack_payload)
    r = requests.post(slack_url, data=json_payload, headers={'Content-Type': 'application/json'})
    if r.status_code != 200:
        r.raise_for_status()

def simple_channel_notification(event, key):
    '''just do a simple @ channel earthquake notification'''
    slack_url = 'https://hooks.slack.com/services/{0}'.format(key)
    pager_status = event['properties']['alert']
    mag = event['properties']['mag']
    place = event['properties']['place']
    eventid = event['id']
    #build the test submission
    test = ''
    if eventid == 'us1000test':
        test = ' *Test Notification* '
    submit_string = '<!channel> {0} {1} magnitude earthquake: {2}'.format(test, mag, place)
    data = json.dumps({'text': submit_string})
    r = requests.post(slack_url, data=data, headers={'Content-Type': 'application/json'})
    if r.status_code != 200:
        r.raise_for_status()

def parse_product_name(event):
    '''runs a regex over the place to create a product name'''
    estr = event['properties']['place'] #ex: "69km WSW of Kirakira, Solomon Islands"
    regex = re.compile(' of (.*)[,]? (.*)')
    match = re.search(regex, estr)
    if match:
        product_name = '%s %s' % (match.group(1),match.group(2))
    else:
        product_name = estr
    return product_name.replace(',','')   


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-e', '--event', help='PAGER json string', dest='event', required=True)
    parser.add_argument('-k', '--key', help='slack url key', dest='key', required=True)
    args = parser.parse_args()
    event = json.loads(args.event)
    print('event: %s' % event)
    slack_notify(event, args.key)
