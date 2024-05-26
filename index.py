import os
import pandas as pd
from popularity_based import *
from content_based import *
from collaborative_filtering import *
from hybrid_model import *
import hashlib
from flask import Flask, jsonify, request
import csv
from pymongo import MongoClient
from flask_cors import CORS

def pseudonymize_email(email):
    hashed_email = hashlib.md5(email.encode('utf-8')).hexdigest()
    return hashed_email[:8]

app = Flask(__name__)
CORS(app, resources={r"/": {"origins": ""}}, supports_credentials=True)

@app.route('/api/data', methods=['GET'])
def get_data():
    param = request.args.get('param', '')
    df = pd.read_csv("output.csv")
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df[df['timestamp'].dt.year > 2015]
    user_id_to_recommend = param
    user_interactions = df[df['user_id'] == user_id_to_recommend][['item_id', 'title', 'sub_cat', 'brand']].drop_duplicates('item_id')
    dataList = []

    try:
        hybrid_recommender = HybridRecommender(df, content_based_weight=0.6, collaborative_filtering_weight=0.4)
        top_n_recommendations_hybrid = hybrid_recommender.get_recommendations(user_id_to_recommend)
        recommendations = df[df['item_id'].isin(top_n_recommendations_hybrid)][['item_id', 'title', 'sub_cat', 'brand']].drop_duplicates('item_id')
        for i in recommendations['item_id']:
            dataList.append(i)
    except:
        print("User has not interacted")

    dataList.append("end")
    popularity_recommender = PopularityBasedRecommender(df)
    trending_items = popularity_recommender.get_trending_items(period=180, top_n=8)
    for i in trending_items['item_id']:
        dataList.append(i)

    return jsonify(dataList)

@app.route('/api/update', methods=['GET'])
def update_data():
    try:
        client = MongoClient(os.environ['MONGODB_URI'])
        print("Connected to MongoDB")
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")

    db = client['test']
    collection = db['products']
    csv_file_path = 'output.csv'
    csv_header = ['item_id', 'title', 'brand', 'user_id', 'rating', 'timestamp', 'sub_cat', 'main_cat', 'age', 'gender', 'location']

    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(csv_header)

        for document in collection.find():
            for user in document['usersInteraction']:
                user_id = pseudonymize_email(user['email'])
                item_id = str(document['_id'])
                title = document['title'].replace(',', '').replace('\"', '').replace('|', '').lower()
                main_cat = document['main_cat']
                sub_cat = document['sub_cat']
                timestamp = document['updatedAt'].strftime('%Y-%m-%d %H:%M:%S') or document['createdAt'].strftime('%Y-%m-%d %H:%M:%S')
                rating = document.get('rating', 'N/A')
                brand = title.split()[0]
                age = user['age']
                gender = user['gender']
                location = user['location']
                csv_writer.writerow([item_id, title, brand, user_id, rating, timestamp, sub_cat, main_cat, age, gender, location])

    client.close()
    df = pd.read_csv('output.csv', encoding='latin1')
    unique_df = df.drop_duplicates()
    unique_df.to_csv('output.csv', index=False)
    print(f"CSV file '{csv_file_path}' generated successfully.")
    return jsonify("hi")



@app.route('/', methods=['GET'])
def check():
    
    return jsonify("hi")

