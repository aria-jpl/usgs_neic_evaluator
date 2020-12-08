#!/usr/bin/env python

'''
Submits a standard create_aoi job via a REST call
'''

from __future__ import print_function
import os
import re
import json
import argparse
import requests
#import celeryconfig
from hysds.celery import app

def main(job_params, version, queue, priority, tags):
    '''
    submits a job to mozart to start pager job
    '''
    # submit mozart job
    job_submit_url = os.path.join(app.conf['MOZART_URL'], 'api/v0.2/job/submit')
    params = {
        'queue': queue,
        'priority': int(priority),
        'tags': '[{0}]'.format(parse_job_tags(tags)),
        'type': 'job-create_aoi_track:%s' % version,
        'params': json.dumps(job_params),
        'enable_dedup': False
    }
    print('submitting jobs with params: %s' %  json.dumps(params))
    r = requests.post(job_submit_url, params=params, verify=False)
    if r.status_code != 200:
        print('submission job failed')
        r.raise_for_status()
    result = r.json()
    if 'result' in list(result.keys()) and 'success' in list(result.keys()):
        if result['success'] == True:
            job_id = result['result']
            print('submitted create_aoi job version: %s job_id: %s' % (version, job_id))
        else:
            raise Exception('job not submitted successfully: %s' % result)
    else:
        raise Exception('job not submitted successfully: %s' % result)

def parse_job_tags(tag_string):
    if tag_string == None or tag_string == '':
        return ''
    tag_list = tag_string.split(',')
    tag_list = ['"{0}"'.format(tag) for tag in tag_list]
    return ','.join(tag_list)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-p', '--params', help='Input params dict', dest='params', required=True)
    parser.add_argument('-v', '--version', help='release version, eg "master" or "release-20180615"', dest='version', required=False, default='master')
    parser.add_argument('-q', '--queue', help='Job queue', dest='queue', required=False, default='factotum-job_worker-small')
    parser.add_argument('-p', '--priority', help='Job priority', dest='priority', required=False, default='5')
    parser.add_argument('-g', '--tags', help='Job tags. Use a comma separated list for more than one', dest='tags', required=False, default='neic_event_aoi')
    args = parser.parse_args()
    main(args.params, args.version, args.queue, args.priority, args.tags)
