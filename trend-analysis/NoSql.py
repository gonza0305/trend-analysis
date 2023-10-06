from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, window
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import pymongo

from pymongo.mongo_client import MongoClient

uri = "mongodb+srv://gonxo0305:UaCEnJ1Bpr3ouYH9@cluster0.mbygmjb.mongodb.net/?retryWrites=true&w=majority"

# Create a new client and connect to the server
client = MongoClient(uri)

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)


# Initialize Spark
spark = SparkSession.builder.master("local").appName("TwitterTrendingTopicsAPI").getOrCreate()

# Initialize MongoDB client
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")

# Create or use an existing database in MongoDB
db = mongo_client["trending_topics_db"]

# Define the trending topics collection
trending_topics_collection = db["trending_topics"]

# Load the Twitter data into a DataFrame (replace 'twitter_data.csv' with your data source)
twitter_data = spark.read.csv('twitter_data.csv', header=True)

# Define the schema of the DataFrame
twitter_data = twitter_data.withColumnRenamed("text", "tweet_text")
twitter_data = twitter_data.withColumnRenamed("created_at", "timestamp")

# Filter and preprocess the data to extract relevant information
twitter_data = twitter_data.select("timestamp", "tweet_text")

# Define the Flask app
app = Flask(__name__)

# Define the route to get trending topics
@app.route('/api/trending_topics', methods=['GET'])
def get_trending_topics():
    try:
        # User-provided topic from query parameter
        user_topic = request.args.get('user_topic')

        # Define the start and end dates for the last week
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)

        # Filter tweets from the last week
        twitter_data = twitter_data.filter((col("timestamp") >= start_date) & (col("timestamp") <= end_date))

        # Split tweet text into words
        twitter_data = twitter_data.withColumn("words", split(col("tweet_text"), "\\s+"))

        # Explode the array of words into individual rows
        twitter_data = twitter_data.select("timestamp", explode(col("words")).alias("word"))

        # Group by date and word, count the occurrences
        twitter_data = twitter_data.groupBy("timestamp", "word").count()

        # Calculate the slope of frequency of occurrence for each word
        window_spec = Window.partitionBy("word").orderBy("timestamp")
        twitter_data = twitter_data.withColumn("lag_count", col("count").lag().over(window_spec))
        twitter_data = twitter_data.withColumn("slope", (col("count") - col("lag_count")) / 7)

        # Find the top trending words
        top_trending_words = twitter_data.filter(col("slope").isNotNull()) \
            .orderBy("slope", ascending=False) \
            .limit(10)

        # Find the most trending topic from the dataset
        most_trending_topic = top_trending_words.select("word").limit(1).collect()[0]["word"]

        # Prepare the response
        response = {
            "user_provided_topic": user_topic,
            "most_trending_topic": most_trending_topic,
        }

        # Store the trending topics data in MongoDB
        trending_topics_collection.insert_one(response)

        return jsonify(response), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
