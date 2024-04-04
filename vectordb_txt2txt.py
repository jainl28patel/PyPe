import hnswlib
import numpy as np
import os, sys, json, requests
import pandas as pd
from datetime import datetime, timezone
from sentence_transformers import SentenceTransformer
from pymongo import MongoClient

# Connect to MongoDB
def connect_mongo():
    client = MongoClient('localhost', 27017)
    # delete the existing database
    client.drop_database('movie_database')
    # create a new database
    db = client['movie_database']
    return db.movie_texts

# Function to initialize the HNSW index
def init_hnsw(max_elements, dim=768):  # Adjust dimensions according to your model
    p = hnswlib.Index(space='l2', dim=dim)
    p.init_index(max_elements=max_elements, ef_construction=200, M=16)
    return p

model = SentenceTransformer('all-MiniLM-L6-v2')  # Example model
mongo_collection = connect_mongo()
hnsw_index = None # initialize in db_fill


def generate_vector(text):
    embeddings = model.encode(text, show_progress_bar=True)
    return np.array(embeddings)

def db_fill():
    global hnsw_index
    data_url = "https://raw.githubusercontent.com/weaviate-tutorials/edu-datasets/main/movies_data_1990_2024.json"
    resp = requests.get(data_url)
    df = pd.DataFrame(resp.json())

    vectors = np.array([]).reshape(0, model.get_sentence_embedding_dimension())
    ids = []

    for i, row in enumerate(df.itertuples(index=False)):
        src_text = f"Title: {row.title}; Overview: {row.overview}"
        
        embedding = generate_vector([src_text])
        if vectors.size == 0:
            vectors = embedding
        else:
            vectors = np.vstack((vectors, embedding))
        ids.append(i)

        if i == 0:
            dim = embedding.shape[1]
            hnsw_index = init_hnsw(max_elements=len(df), dim=dim)

        mongo_collection.insert_one({"_id": i, "text": src_text})

    hnsw_index.add_items(vectors, np.array(ids))

    print(f"Inserted {len(ids)} movies into MongoDB and hnswlib index.")

def query_text(input_text):
    input_embedding = model.encode([input_text])[0]
    
    labels, distances = hnsw_index.knn_query(input_embedding, k=1)
    nearest_vector_id = int(labels[0][0])  # Convert from numpy.uint64 to Python int
    
    result = mongo_collection.find_one({"_id": nearest_vector_id})
    if result:
        return result['text']
    else:
        return "No matching text found."

if __name__ == "__main__":
    db_fill()
    sample_query = "A green monster living in swamp"
    print(query_text(sample_query))