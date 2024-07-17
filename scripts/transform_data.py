from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests


def connect_mongo(uri):
    client = MongoClient(uri, server_api=ServerApi('1'))

    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
        return uri
    except Exception as e:
        print(e)
    
def create_connect_db(client, db_name):
    db = client[db_name]
    return db

def create_connect_collection(db, col_name):
    collection = db[col_name]
    return collection

def extract_api_data(url):
    return requests.get(url).json()

def insert_data(col, data):
    docs = col.insert_many(data)
    return len(docs.inserted_ids)

if __name__ == "__main__":
    client = connect_mongo("mongodb+srv://henriquedev42:P51bDGXVVdaqPJ1L@cluster-pipeline.shmvhsi.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-Pipeline")
    db = create_connect_db(client,"db_produtos_desafio")
    col = create_connect_collection(db, "produtos")

    data = extract_api_data("https://labdados.com/produtos")
    n_docs = insert_data(col, data)
    print(f"Quantidade de documentos inseridos{n_docs}")

    client.close()