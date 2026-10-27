from pprint import pprint 
from DbConnector import DbConnector
from typing import Any

DB_COLLECTION_NAMES = ['movies', 'ratings']

Document = dict[str, Any]

class DatabaseManager:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)    
        print('Created collection: ', collection)

    def insert_documents(self, collection_name: str, docs: Document | list[Document]):
        collection = self.db[collection_name]

        if isinstance(docs, dict):
            collection.insert_one(docs)
        elif isinstance(docs, list) and docs:
            collection.insert_many(docs, ordered=False)

    def fetch_documents(self, collection_name, limit=10):
        collection = self.db[collection_name]
        documents = collection.find({}).limit(limit)
        for doc in documents: 
            pprint(doc)
        

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

        
    def show_coll(self):
        collections = self.db.list_collection_names()
        print(collections)
         


def main():
    program = None
    try:
        program = DatabaseManager()
        create_collections(program, DB_COLLECTION_NAMES)

        program.show_coll()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

def create_collections(program: DatabaseManager, collection_names: list[str]):
    existing_collections = program.db.list_collection_names()
    for collection_name in collection_names:
        if collection_name in existing_collections:
            print(f"Collection '{collection_name}' already exists.")
        else:
            program.create_coll(collection_name)

if __name__ == '__main__':
    main()
