
import boto3
import datetime
import csv
from decimal import Decimal

#Access Credentials
a_key = ''
a_S_key = ''
region="us-east-1"

def main():
    with open('combined_csv.csv', newline='') as f:
        reader = csv.reader(f)
        restaurants = list(reader)
    restaurants = restaurants[1:]

    dynamodb = boto3.resource('dynamodb', aws_access_key_id=a_key, aws_secret_access_key=a_S_key, region_name=region)
    table = dynamodb.Table('yelp-restaurants')

    # 0 Business_ID
    # 1 Name
    # 2 Address
    # 3 Latitude
    # 4 Longitude
    # 5 Number_of_Reviews
    # 6 Rating
    # 7 Zip_Code
    # 8 Cuisine

    for restaurant in restaurants:
        tableEntry = {
            'restaurant_id': restaurant[0],
            'name': restaurant[1],
            'address': restaurant[2],
            'latitude': restaurant[3],
            'longitude': restaurant[4],
            'review_count': int(restaurant[5]),
            'rating': Decimal(str(restaurant[6])),
            'zip_code': restaurant[7],
            'cuisine': restaurant[8]
        }                

        table.put_item(
            Item={
                'insertedAtTimestamp': str(datetime.datetime.now()),
                'restaurant_id': tableEntry['restaurant_id'],
                'name': tableEntry['name'],
                'address': tableEntry['address'],
                'latitude': tableEntry['latitude'],
                'longitude': tableEntry['longitude'],
                'review_count': tableEntry['review_count'],
                'rating': tableEntry['rating'],
                'zip_code': tableEntry['zip_code'],
                'cuisine': tableEntry['cuisine']
            }
        )

if __name__ == "__main__":
    main()