import os

blob_name_static = "static.json"
blob_name_api = "api.json"

bucket_name = os.environ.get("GS_BUCKET_NAME")
service_account_key_file = os.environ.get("GS_SERVICE_ACCOUNT_KEY_FILE")

mongo_username = os.environ.get("MONGO_USERNAME")
mongo_password =  os.environ.get("MONGO_PASSWORD")
mongo_ip_address = os.environ.get("MONGO_IP")
database_name = os.environ.get("MONGO_DB_NAME")
collection_name = os.environ.get("MONGO_COLLECTION_NAME")


# blob_name_static = "static.json"
# blob_name_api = "api.json"

# bucket_name = "msds697"
# service_account_key_file = "msds-697-goal-diggers-cc16e1753aa9.json"


# mongo_username ="admin"
# mongo_password = "goal-diggers"
# mongo_ip_address = "msds697-goal-diggers.5f8jw.mongodb.net"
# database_name = "reviewdb"
# collection_name =  "amazon"