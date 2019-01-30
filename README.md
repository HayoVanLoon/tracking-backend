# Tracking Backend Demo

A demonstration AppEngine/Flask app that manages a user event data stream to 
BigQuery.

*Important: this app does not (yet) handle authentication and should not (yet) 
be deployed to any production environment.*

## Set up the environment
```bash
virtualenv -p python3.6 venv
source venv/bin/activate
pip install -r requirements.txt
``` 

## Start a local server
```bash
export GOOGLE_CLOUD_PROJECT='your-project'

source venv/bin/activate
python main.py
```

## Initialise Dataset
Any request to the /init endpoint will generate the following items:
* a dataset 'bobs_knob_shops', with
* two tables: 'events_0' and 'events_1'
```bash
curl http://localhost:8080/init
```

## Inserting events
Events are inserted using the ```/events``` endpoint. The endpoint accepts a 
POST call with a json body describing one or more events.

The following will insert the call described in the example json file. 
```bash
curl -X POST -i http://localhost:8080/events \
    -H "Content-Type: application/json" \
    --data "$(cat example.json)"
```

## Triggering the daily aggregation job
The following will trigger the daily aggregation job. It is designed to run 
every day; a few hours after midnight (UTC).
```bash
curl http://localhost:8080/events/aggregation
```  

## Spawn a demo dataset
You can spawn a demo with the ```/demo_init``` endpoint. This endpoint will 
initialise the dataset and fill it with some data.

The data is selected so you can observe the logic of the aggregation job. 
Initially there will be:
* events from yesterday's sessions in one table
* events from a session starting yesterday, continuing past midnight and thus 
split over two tables
* events for a few sessions in the current day table
* no sessions in the sessions table

After running the job there will be:
* all sessions closed before midnight in the sessions table
* all events from the session spanning midnight in the current day table
* today's events (untouched) in the current day table
* an empty table for yesterday / tomorrow  


## License
Copyright 2019 Hayo van Loon

Licensed under the Apache License, Version 2.0 (the "License"); you may not use 
this file except in compliance with the License. You may obtain a copy of the 
License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed 
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR 
CONDITIONS OF ANY KIND, either express or implied. See the License for the 
specific language governing permissions and limitations under the License.