# Project1---YouTube-Data-Harvesting-and-Data-Warehousing
This ETL project involves building a simple UI with Streamlit, retrieving data from the YouTube API, storing it in a MongoDB data lake, migrating it to a SQL data warehouse, querying the data warehouse with SQL, and displaying the data in the Streamlit app.
## TOOLS USED:

### Google API Client
Google APIs give you programmatic access to Google Maps, Google Drive, YouTube, and many other Google products. To make coding against these APIs easier, Google provides client libraries that can reduce the amount of code you need to write and make your code more robust. In this project, GoogleApI client facilitates access to Youtube data like channel information, video details, comments etc. 

### PYTHON LANGUAGE
Python is a high-level, general-purpose programming language. Being easy to learn and understand, Python is used in this project for fetching the data using the Youtube API.

### Streamlit
Streamlit is a free and open-source framework to rapidly build and share beautiful machine learning and data science web apps. It is a Python-based library specifically designed for machine learning engineers.

### MongoDB 
MongoDB Compass, the GUI for MongoDB, is the easiest way to explore and manipulate your data. In this project, the data retrieved using the YouTube API is then stored to MongoDB using pymongo, a python Driver for MongoDB. The stored data can be viewed using the user-friendly MongoDB Compass.

### POSTGRESQL
PostgreSQL is an advanced relational database system.PostrgeSQL supports both relational (SQL) and non-relational (JSON) queries. In this project, the JSON data from MongoDB is migrated to POSTGRES.It provides an environment for storing and manipulating structured data with advanced SQL capabilities.

### LIBRARIES REQUIRED
1. googleapiclient
2. pymongo
3. psycopg2
4. pandas
5. streamlit
6. isodate

### PROJECT FEATURES:

1. Ability to input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes,         comments of each video) using Google API.
2. Option to store the data in a MongoDB database.
3. Ability to collect data for different YouTube channels.
4. Option to select a channel name and migrate its data from the data lake to a SQL database as tables.
5. Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.






 
