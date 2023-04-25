import json
import requests
from pyspark.sql.types import *
from pyspark.sql.functions import *
from google.cloud import storage

def extract_reviews(json_response):
    reviews = json_response.get('reviews', [])
    extracted_reviews = {}
    for review in reviews:
        i = review['asin']['original']
        if not extracted_reviews.get(i):
            extracted_reviews[i] = [
            {'review': review['review'], 
            'rating': review['rating'], 
            'summary': review['title']
            }]
        else:
            extracted_reviews[i] += [
            {'review': review['review'], 
            'rating': review['rating'], 
            'summary': review['title']
            }]
    return extracted_reviews

def trigger_api(product_asin, page_no):
    '''
    This function accepts the product’s ASIN and the page number as arguments. 
    The country is set to “US” so the program will only work with the products 
    listed on Amazon’s US website. It invokes the API via the requests library 
    and returns an API response in JSON format. 
    '''
    querystring = {"page": str(page_no) ,  "country":"US" , "asin": product_asin}
    headers = {
    'x-rapidapi-host': "amazon-product-reviews-keywords.p.rapidapi.com",
    'x-rapidapi-key': "e155b49501msh0172d07b4bed653p11ccd6jsn2a9f2b62d0ee"
    }
    url = "https://amazon-product-reviews-keywords.p.rapidapi.com/product/reviews"
    response = requests.request("GET", url, headers=headers, params=querystring)
    if(200 == response.status_code):
        # print(response.text)
        return extract_reviews(json.loads(response.text))
    else:
        return None


# if __name__ == "__main__":
def step1(bucket_name, blob_name_api, service_account_key_file):
    li = ["B098F9B796"]
    # for testing purposes we are just using one single asin_id corresponding to a single product.
    m = {}
    for i in li:
        for j in range(1,5):
            temp = trigger_api(i,j)
            if temp == {}:
                continue
            if i in m.keys():
                m[i] += temp[i]
            else:
                m[i] = temp[i]
    storage_client = storage.Client.from_service_account_json(service_account_key_file)
    bucket = storage_client.bucket(bucket_name)
    json_str = json.dumps(m)
    blob = bucket.blob(blob_name_api)
    blob.upload_from_string(json_str)