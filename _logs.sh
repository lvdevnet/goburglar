#!/bin/sh
project=goburglar
seconds=${1:-60}
ts=$(date -d @$(($(date +%s)-$seconds)) -Ins |sed -e 's/,/./')
gcloud --project $project beta logging read \
  "logName=projects/$project/logs/appengine.googleapis.com%2Frequest_log and timestamp > \"$ts\"" \
  --limit 100 --format json --order ASC \
    | jq -r '.[]|"\n"+.timestamp,(.protoPayload|(.status|tostring)+" "+.resource,.line[].logMessage)'
