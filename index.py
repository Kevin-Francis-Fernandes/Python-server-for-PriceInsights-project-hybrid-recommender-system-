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
    # Using hashlib to generate a hash of the email address
    hashed_email = hashlib.md5(email.encode('utf-8')).hexdigest()
        
    # Take the first 8 characters of the hash as a pseudonymous value
    pseudonymous_value = hashed_email[:8]
        
    return pseudonymous_value


app = Flask(__name__)
CORS(app,resources={r"/": {"origins": ""}},supports_credentials=True)

@app.route('/api/data', methods=['GET'])
def get_data():


    # Get the 'param' query parameter from the request
    param = request.args.get('param', '')
    
    # Read the pre-processed data
    df = pd.read_csv("output.csv") #,encoding='latin1')
    # print(df.columns)
    # Convert the 'timestamp' column to datetime

    df['timestamp'] = pd.to_datetime(df['timestamp'])




    # Filter the data to include only interactions after the year 2015
    df = df[df['timestamp'].dt.year > 2015]
    df.head()

    df.shape


    # Provide the user_id for which you want to get recommendations 
    # user_id_to_recommend = '766ee2a6'
    user_id_to_recommend = param
    # Get the user's interactions (Item_id, title, category, and brand)
    user_interactions = df[df['user_id'] == user_id_to_recommend][['item_id', 'title', 'sub_cat', 'brand']]\
        .drop_duplicates('item_id')
    # print(f"User {user_id_to_recommend} Interacted for the following products:")
    # print(user_interactions)

    dataList = []
    try:

        # Create an instance of the HybridRecommender class
        hybrid_recommender = HybridRecommender(df, content_based_weight=0.6, collaborative_filtering_weight=0.4)
        top_n_recommendations_hybrid = hybrid_recommender.get_recommendations(user_id_to_recommend)

        # Get the model's recommended products
        recommendations = df[df['item_id'].isin(top_n_recommendations_hybrid)][
            ['item_id', 'title', 'sub_cat', 'brand']].drop_duplicates('item_id')
        # print(f"Model recommends the following products to the user {user_id_to_recommend}:")
        # print(recommendations)

        
        for i in recommendations['item_id']:
            dataList.append(i)
    except:
        print("User has not interacted")



    dataList.append("end")





    # Instantiate the PersonalizedRecommender class
    popularity_recommender = PopularityBasedRecommender(df)

    # Get trending items in the last 15 days (e.g., top 10)
    trending_items = popularity_recommender.get_trending_items(period=180, top_n=8)

    # Print the recommendations
    # print("Trending Items:")
    # print(trending_items)
    for i in trending_items['item_id']:
        dataList.append(i)

    
    # print(dataList)
    return jsonify(dataList)

@app.route('/api/update', methods=['GET'])
def update_data():
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
    return jsonify("hi")

 

if __name__ == '__main__':
    app.run(debug=True)