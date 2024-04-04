import hnswlib
import numpy as np
from transformers import AutoFeatureExtractor, ResNetModel
from PIL import Image
from pymongo import MongoClient
import os, glob

# Connect to MongoDB
def connect_mongo():
    client = MongoClient('localhost', 27017)
    client.drop_database('image_database')  # Reset database
    db = client['image_database']
    return db.image_paths

# Initialize HNSW index
def init_hnsw(max_elements, dim=2048):  # Adjust dimensions according to your model
    p = hnswlib.Index(space='l2', dim=dim)
    p.init_index(max_elements=max_elements, ef_construction=200, M=16)
    return p

# Model and processor initialization
processor = AutoFeatureExtractor.from_pretrained('Ramos-Ramos/dino-resnet-50')
model = ResNetModel.from_pretrained('Ramos-Ramos/dino-resnet-50')

def generate_vector(data):
    inputs = processor(images=data, return_tensors="pt")
    outputs = model(**inputs)
    return outputs.pooler_output.detach().numpy().reshape(-1,)

mongo_collection = connect_mongo()
hnsw_index = None  # Will be initialized in db_fill

def db_fill():
    global hnsw_index

    txt=''
    with open( "animals.txt" ,'r') as f:
        txt = f.read().split('\n')

    image_paths = []
    for i in txt:
        files = glob.glob('/assets/archive/animals/animals/'+i + '/*.jpg')
        image_path.extend(files)

    vectors = []
    for i, image_path in enumerate(image_paths):
        image = Image.open(image_path)
        vector = generate_vector(image)

        if i == 0:
            dim = vector.shape[0]
            hnsw_index = init_hnsw(max_elements=len(image_paths), dim=dim)
        
        vectors.append(vector)
        mongo_collection.insert_one({"_id": i, "path": image_path})

    hnsw_index.add_items(np.array(vectors), np.arange(len(image_paths)))

    print(f"Inserted {len(image_paths)} images into MongoDB and HNSWlib index.")

def query_image(input_image):
    input_vector = generate_vector(input_image)

    labels, distances = hnsw_index.knn_query(input_vector, k=1)
    nearest_image_id = int(labels[0][0])
    
    result = mongo_collection.find_one({"_id": nearest_image_id})
    if result:
        return result['path']
    else:
        return "No matching image found."

if __name__ == "__main__":
    db_fill()
    sample_image = Image.open('./test/dog1.jpeg')
    print(query_image(sample_image))
