import csv
import json

    # 0 Business_ID
    # 1 Name
    # 2 Address
    # 3 Latitude
    # 4 Longitude
    # 5 Number_of_Reviews
    # 6 Rating
    # 7 Zip_Code
    # 8 Cuisine

def main():
    with open('combined_csv.csv', newline='') as f:
        reader = csv.reader(f)
        restaurants = list(reader)
    restaurants = restaurants[1:]

    file_object = open('output.json', 'a')

    for restaurant in restaurants:      
        outputOne = { "index": { "_index": "restaurant", "_id": restaurant[0] } }
        file_object.write(json.dumps(outputOne) + "\n")

        outputTwo = { 'Cuisine': str(restaurant[8]) }
        file_object.write(json.dumps(outputTwo) + "\n")

    file_object.close()

if __name__ == "__main__":
    main()