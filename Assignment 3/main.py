import math
from itertools import islice
from pathlib import Path
from pprint import pprint
from typing import Any, Iterable, Iterator

import pandas as pd
from pymongo.errors import BulkWriteError, DuplicateKeyError

from DbConnector import DbConnector

DB_COLLECTION_NAMES = ['movies', 'ratings']

DATA_DIR = Path(__file__).parent / 'data'
CLEAN_DIR = DATA_DIR / 'clean'   # created by Clean.ipynb
BATCH_SIZE = 5000

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

        # Conflicting documents (e.g. duplicate _id) are skipped; the cleaned dataset will resolve them
        if isinstance(docs, dict):
            try:
                collection.insert_one(docs)
            except DuplicateKeyError:
                print(f"Skipped duplicate document in '{collection_name}': _id={docs.get('_id')}")
        elif isinstance(docs, list) and docs:
            try:
                collection.insert_many(docs, ordered=False)
            except BulkWriteError as e:
                print(f"Skipped {len(e.details['writeErrors'])} conflicting documents in '{collection_name}'")

    def has_data(self, collection_name) -> bool:
        return self.db[collection_name].find_one() is not None

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

        if program.has_data('movies'):
            print("Collection 'movies' already has data, skipping insert.")
        else:
            insert_in_batches(program, 'movies', build_movie_docs(CLEAN_DIR))

        if program.has_data('ratings'):
            print("Collection 'ratings' already has data, skipping insert.")
        else:
            # Read genres back from the db so this works whether movies were inserted now or in an earlier run
            genres_by_movie = {doc['_id']: [g['name'] for g in doc.get('genres') or []]
                               for doc in program.db.movies.find({}, {'genres.name': 1})}
            insert_in_batches(program, 'ratings', build_rating_docs(CLEAN_DIR, genres_by_movie))

        # Created after the bulk insert; no-ops if the indexes already exist
        program.db.movies.create_index([('overview', 'text'), ('tagline', 'text')])
        program.db.ratings.create_index('user_id')

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

def insert_in_batches(program: DatabaseManager, collection_name: str, docs: Iterable[Document]):
    docs = iter(docs)
    inserted = 0
    while batch := list(islice(docs, BATCH_SIZE)):
        program.insert_documents(collection_name, batch)
        inserted += len(batch)
        print(f"Processed {inserted} documents for '{collection_name}'")

def is_missing(value) -> bool:
    return (value is None or value is pd.NaT or value is pd.NA
            or (isinstance(value, float) and math.isnan(value)))

def drop_missing(doc: Document) -> Document:
    # A missing value is represented by leaving the field out of the document
    return {key: value for key, value in doc.items() if not is_missing(value)}

def build_movie_docs(clean_dir: Path) -> Iterator[Document]:
    movies   = pd.read_pickle(clean_dir / 'movies.pkl')
    credits  = pd.read_pickle(clean_dir / 'credits.pkl')
    keywords = pd.read_pickle(clean_dir / 'keywords.pkl')

    # Credits and keywords are embedded in each movie
    movies = (movies.merge(credits, on='id', how='left')
                    .merge(keywords, on='id', how='left')
                    .rename(columns={'id': '_id'}))

    for record in movies.to_dict('records'):
        doc = drop_missing(record)
        if 'release_date' in doc:
            doc['release_date'] = doc['release_date'].to_pydatetime()
        yield doc

def build_rating_docs(clean_dir: Path, genres_by_movie: dict[int, list[str]]) -> Iterator[Document]:
    ratings = pd.read_pickle(clean_dir / 'ratings.pkl')

    # movie_id is already the movie's _id (mapped through links in Clean)
    for user_id, movie_id, rating, timestamp in zip(ratings['userId'], ratings['movie_id'],
                                                     ratings['rating'], ratings['timestamp']):
        movie_id = int(movie_id)
        yield {
            'user_id': int(user_id),
            'movie_id': movie_id,
            'rating': float(rating),
            'timestamp': timestamp.to_pydatetime(),
            'genres': genres_by_movie.get(movie_id, []),
        }

if __name__ == '__main__':
    main()
