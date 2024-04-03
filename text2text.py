from utils import *
import os, sys, json, requests
import pandas as pd
import weaviate.classes.query as wq
from weaviate.util import generate_uuid5
from datetime import datetime, timezone
from sentence_transformers import SentenceTransformer
  
model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')

def generate_vector(text):
    embeddings = model.encode(text)
    return list(embeddings)

def db_fill(client):

    data_url = "https://raw.githubusercontent.com/weaviate-tutorials/edu-datasets/main/movies_data_1990_2024.json"
    resp = requests.get(data_url)
    df = pd.DataFrame(resp.json())

    emb_dfs = list()
    src_texts = list()

    for i, row in enumerate(df.itertuples(index=False)):

        src_text = "Title" + row.title + "; Overview: " + row.overview
        src_texts.append(src_text)

        if (len(src_texts) == 50) or (i + 1 == len(df)): 
            print(i)
            output = generate_vector(src_texts)
            emb_df = pd.DataFrame(output)
            emb_dfs.append(emb_df)
            src_texts = list()

    emb_df = pd.concat(emb_dfs)

    movies = client.collections.get("Movie")

    with movies.batch.dynamic() as batch:
        for i, movie in enumerate(df.itertuples(index=False)):
            release_date = datetime.strptime(movie.release_date, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )

            movie_obj = {
                "title": movie.title,
                "overview": movie.overview,
                "release_date": release_date,
            }

            vector = emb_df.iloc[i].to_list()

            batch.add_object(
                properties=movie_obj,
                uuid=generate_uuid5(movie.id),
                vector=vector
            )

    if len(movies.batch.failed_objects) > 0:
        print(f"Failed to import {len(movies.batch.failed_objects)} objects")


def query(client, text):
    movies = client.collections.get("Movie")
    response = movies.query.near_vector(
        near_vector=generate_vector(text),
        limit=3,
        return_metadata=wq.MetadataQuery(distance=True),
    )

    for o in response.objects:
        print(o.properties["title"], o.properties["release_date"].year)
        print(f"Distance to query: {o.metadata.distance:.3f}\n")

if __name__=="__main__": 
    client = connect()
    # db_fill(client)
    query(client,"time travel")
    client.close()
