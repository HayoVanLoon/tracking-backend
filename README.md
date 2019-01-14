# Tracking Backend Demo

A demonstration AppEngine/Flask app that handles the storage user event data in 
BigQuery.

## Start a Local Server
```bash
source venv/bin/activate
python main.py
```

## Initialise Dataset
A GET request to the /init endpoint will generate the following items in **the 
currently selected project**:
* a dataset 'bobs_knob_shops', with
* two tables: 'events_0' and 'events_1'

## Insert an Event
```bash
curl -X POST -i http://localhost:8080/events \
    -H "Content-Type: application/json" \
    --data "$(cat example.json)"
```

## License
Copyright 2019 Hayo van Loon

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.