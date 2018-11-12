#!/bin/bash

set -e

#check input args
if [ -z "$1" ] 
then
    echo "No path specified"
    exit 1
fi
if [ -z "$2" ]
then
    echo "No create_aoi release version specified"
    exit 1
fi
if [ -z "$3" ]
then
    echo "No pre-event range specified"
    exit 1
fi
if [ -z "$4" ]
then
    echo "No post-event range specified"
    exit 1
fi
if [ -z "$5" ]
then
    echo "No filter type specified"
    exit 1
fi
if [ -z "$6" ]
then
    echo "No water mask specified"
    exit 1
fi

filter_type=''
if [ "$5" == "dynamic" ]; then 
    filter_type='--dynamic_threshold'
fi

#if there is no filter
if [ "$5" == "none" ]; then
    DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    python ${DIR}/event_evaluator.py --event_path "${1}" --create_aoi_version "${2}" --days_pre_event "${3}" --days_post_event "${4}" --slack_notification "${7}"
    rm -rf ${1} #remove the event dir so it doesn't republish
    exit 0
fi

slack_notification=${7}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
python ${DIR}/event_evaluator.py --event_path "${1}" --create_aoi_version "${2}" --days_pre_event "${3}" --days_post_event "${4}" ${filter_type} --water_filter "${6}" --depth_filter 200 --slack_notification "${7}"
#remove the event dir so it doesnt republish
rm -rf ${1}

