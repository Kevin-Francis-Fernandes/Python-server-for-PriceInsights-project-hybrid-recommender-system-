import csv
from pymongo import MongoClient
import os
import hashlib
import pandas as pd
import random
from datetime import datetime

def pseudonymize_email(email):
    # Using hashlib to generate a hash of the email address
    hashed_email = hashlib.md5(email.encode('utf-8')).hexdigest()
    
    # Take the first 8 characters of the hash as a pseudonymous value
    pseudonymous_value = hashed_email[:8]
    
    return pseudonymous_value

# Connect to MongoDB
try:
    # Connect to MongoDB using MongoClient
    client = MongoClient('mongodb+srv://kevinfrancisfernandes8:Kevin123@cluster0.e8uh7q9.mongodb.net/?retryWrites=true&w=majority')
    print("Connected to MongoDB")
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")


db = client['test']
collection = db['products']

# Define CSV file path
csv_file_path = 'output.csv'

# Define CSV header
csv_header = ['item_id','title', 'brand','user_id', 'rating', 'timestamp', 'sub_cat','main_cat','age','gender','location']

# Open CSV file for writing
with open(csv_file_path, 'w', newline='',encoding='utf-8') as csv_file:
    # Create CSV writer
    csv_writer = csv.writer(csv_file)
    
    # Write header to CSV
    csv_writer.writerow(csv_header)

    # Iterate through MongoDB documents
    for document in collection.find():
        # Iterate through all users in the 'users' field
        for user in document['usersInteraction']:
            # Extract fields from the document
            user_id = pseudonymize_email(user['email'])
           # url = document['url']
            item_id = str(document['_id'])
            title = document['title'].replace(',','').replace('\"','').replace('|','').lower()
            main_cat = document['main_cat']
            sub_cat = document['sub_cat']
            timestamp = document['updatedAt'].strftime('%Y-%m-%d %H:%M:%S') or document['createdAt'].strftime('%Y-%m-%d %H:%M:%S')
            rating = document.get('rating', 'N/A')
            brand=title.split()[0]
            age=user['age']
            gender= user['gender']
            location=user['location']
            # Write data to CSV
            csv_writer.writerow([ item_id, title, brand, user_id,rating, timestamp,sub_cat,main_cat, age,gender,location])

# Close MongoDB connection
client.close()

df = pd.read_csv('output.csv', encoding='latin1')

# Keep only unique rows
unique_df = df.drop_duplicates()

# Write the DataFrame with unique rows to a new CSV file
unique_df.to_csv('output.csv', index=False)
print(f"CSV file '{csv_file_path}' generated successfully.")
