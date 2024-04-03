import os, json, glob, requests
import numpy
import pandas as pd
import weaviate.classes.query as wq
from weaviate.util import generate_uuid5
from datetime import datetime, timezone
from transformers import AutoFeatureExtractor,ResNetModel
from PIL import Image

processor = AutoFeatureExtractor.from_pretrained('Ramos-Ramos/dino-resnet-50')
model = ResNetModel.from_pretrained('Ramos-Ramos/dino-resnet-50')

def generate_vector(data):

    inputs = processor(images=data, return_tensors="pt")
    outputs = model(**inputs)
    return outputs.pooler_output.reshape(-1,)

def db_fill(client):

    txt=''
    with open('/content/name of the animals.txt','r') as f:
        txt = f.read().split('\n')

    images = []
    for i in txt:
        files = glob.glob('/content/animals/animals/'+i + '/*.jpg')
    images.extend(files)
    images.extend(['cat1.jpeg','cat2.jpeg','cat3.jpeg','dog1.jpeg','dog2.jpeg'])

    print(len(images))

    animals = client.collections.delete("Animals")
    animals = client.collections.create("Animals")
    animals = client.collections.get("Animals")

    with animals.batch.dynamic() as batch:
        for i, animal in enumerate(images):
            animal_obj = {
                "path": animal,
            }

            batch.add_object(
                properties=animal_obj,
                uuid=generate_uuid5(i),
                vector=generate_vector(Image.open(animal)).tolist() 
            )

    if len(animals.batch.failed_objects) > 0:
        print(f"Failed to import {len(animals.batch.failed_objects)} objects")


def query(client, image):
    animals = client.collections.get("Animals")
    response = animals.query.near_vector(
        near_vector=generate_vector(image).tolist(), 
        limit=3,
        return_metadata=wq.MetadataQuery(distance=True),
    )

    for obj in response.objects:
        print(obj.properties)
        print(f"Distance to query: {obj.metadata.distance:.3f}\n") 

if __name__=="__main__": 
    client = connect()
    # db_fill(client)
    query(client,Image.open('dog1.jpeg'))
    client.close()
