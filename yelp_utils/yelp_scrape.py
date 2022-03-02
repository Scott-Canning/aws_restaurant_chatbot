
import requests
import json
import pandas as pd
from datetime import datetime
import time

YELP_API_KEY = ''
QUERY_LIMIT = 50
ZIP_CODE = "11501"

class Yelp_Search:

    def __init__(self):
        self.yelp_api_key = YELP_API_KEY
        self.headers = {'Authorization': 'Bearer %s' % self.yelp_api_key}

    def filter_location(self, params):
        url = 'https://api.yelp.com/v3/businesses/search'
        output = self.request(url, params)
        return output

    def search_business_id(self, business_id):
        url = 'https://api.yelp.com/v3/businesses/' + business_id
        output = self.request(url)
        return output

    def request(self, url, search_params=None):
        # get request
        req = requests.get(url, params=search_params, headers=self.headers)
        # proceed only if the status code is 200
        status_code = req.status_code
        if status_code == 200:
            # dict
            parsed = json.loads(req.text)
            return parsed
        
        print("search error (see yelp_api.py):", status_code)

if __name__ == "__main__":
   
    file_name = "yelp_scrape_" + str(ZIP_CODE) + "_" + \
                str(datetime.today().strftime('%m-%d-%Y')) + ".csv"

    column_names = [    
                    "Business ID", 
                    "Alias", 
                    "Address", 
                    "Latitude",
                    "Longitude",
                    "Number_of_Reviews", 
                    "Rating", 
                    "Zip_Code",
                    "Cuisine"
                    ]

    df = pd.DataFrame(columns=column_names)

    search_terms = [
                    'Cafe', 
                    'Bar', 
                    'Sushi', 
                    'Pizza',
                    'Indian',
                    'Thai', 
                    'Italian', 
                    'French',
                    'Chinese',
                    'Mexican',
                    ]

    for i in range(len(search_terms)):
        term = search_terms[i]

        params = {
                  'limit': QUERY_LIMIT, 
                  'term': term, 
                  'location': ZIP_CODE
                  }

        search_object = Yelp_Search()
        resultJSON = search_object.filter_location(params)

        for index, item in enumerate(resultJSON['businesses']):
            location = ""

            if item['location']['address1']:
                location = item['location']['address1']

            if item['location']['address2']:
                location += ", " + item['location']['address2']

            if item['location']['address3']:
                location += ", " + item['location']['address3']

            if item['location']['city']:
                location += ", " + item['location']['city']

            if item['location']['state']:
                location += ", " + item['location']['state']

            df.loc[len(df.index)] = [
                item['id'],
                item['name'],
                location,
                item['coordinates']['latitude'],
                item['coordinates']['longitude'],
                item['review_count'],
                item['rating'],
                item['location']['zip_code'],
                term
            ]

        time.sleep(1)

    df.to_csv(file_name, index=False)