#
#    Copyright 2019 Google LLC
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        https://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

import sys
import json

from googleapiclient import sample_tools
from googleapiclient.http import build_http

discovery_doc = "gmb_discovery.json"

def main(argv):
    # Use the discovery doc to build a service that we can use to make
    # MyBusiness API calls, and authenticate the user so we can access their
    # account
    service, flags = sample_tools.init(argv, "mybusiness", "v4", __doc__, __file__, scope="https://www.googleapis.com/auth/business.manage", discovery_filename=discovery_doc)

    # Get the list of accounts the authenticated user has access to
    output = service.accounts().list().execute()
    print("List of Accounts:\n")
    print(json.dumps(output, indent=2) + "\n")

    firstAccount = output["accounts"][0]["name"]

    # Get the list of locations for the first account in the list
   
    locationsList = service.accounts().locations().list(parent=firstAccount).execute()
    locationName = locationsList['locations'][0]['name']


    # request last day
    date_start = '2019-10-09T05:00:00Z'
    date_end = '2019-10-10T23:00:01Z'
    requestBody = {
      "locationNames":[locationName],
      "basicRequest": {
        "metricRequests": {
            "metric": "ALL"
        },
        "timeRange": {
          "startTime": date_start,
          "endTime": date_end
        }
      }
    }
    locationinsightReport = service.accounts().locations().reportInsights(name=firstAccount, body=requestBody).execute()
    print(json.dumps(locationinsightReport, indent=2)[0:1650])

if __name__ == "__main__":
  main(sys.argv)
