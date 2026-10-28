import ast
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
# Switch to 'ratings.csv' / 'links.csv' for the full dataset
RATINGS_FILE = 'ratings_small.csv'
LINKS_FILE = 'links_small.csv'
BATCH_SIZE = 5000

MOVIE_NESTED_COLUMNS = ['belongs_to_collection', 'genres', 'production_companies',
                        'production_countries', 'spoken_languages']
MOVIE_NUMERIC_COLUMNS = ['budget', 'popularity', 'revenue', 'runtime', 'vote_average', 'vote_count']
MOVIE_BOOL_COLUMNS = ['adult', 'video']

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
            insert_in_batches(program, 'movies', build_movie_docs(DATA_DIR))

        if program.has_data('ratings'):
            print("Collection 'ratings' already has data, skipping insert.")
        else:
            # Read genres back from the db so this works whether movies were inserted now or in an earlier run
            genres_by_movie = {doc['_id']: [g['name'] for g in doc.get('genres') or []]
                               for doc in program.db.movies.find({}, {'genres.name': 1})}
            insert_in_batches(program, 'ratings', build_rating_docs(DATA_DIR, genres_by_movie))

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

def parse_nested(value):
    # Nested CSV columns are Python literals (single quotes), not JSON
    if not isinstance(value, str):
        return None
    try:
        return ast.literal_eval(value)
    except (ValueError, SyntaxError):
        return None

def is_missing(value) -> bool:
    return (value is None or value is pd.NaT or value is pd.NA
            or (isinstance(value, float) and math.isnan(value)))

def drop_missing(doc: Document) -> Document:
    # A missing value is represented by leaving the field out of the document
    return {key: value for key, value in doc.items() if not is_missing(value)}

def read_nested_csv(path: Path, columns: list[str]) -> pd.DataFrame:
    df = pd.read_csv(path)
    for column in columns:
        df[column] = df[column].map(parse_nested)
    return df

def build_movie_docs(data_dir: Path) -> Iterator[Document]:
    movies = pd.read_csv(data_dir / 'movies_metadata.csv', low_memory=False)

    # Some broken rows have non-numeric ids; they would otherwise get a string _id
    movies['id'] = pd.to_numeric(movies['id'], errors='coerce')
    movies = movies.dropna(subset=['id'])
    movies['id'] = movies['id'].astype(int)

    for column in MOVIE_NUMERIC_COLUMNS:
        movies[column] = pd.to_numeric(movies[column], errors='coerce')
    for column in MOVIE_NESTED_COLUMNS:
        movies[column] = movies[column].map(parse_nested)
    for column in MOVIE_BOOL_COLUMNS:
        movies[column] = movies[column].map({'True': True, 'False': False})
    movies['release_date'] = pd.to_datetime(movies['release_date'], errors='coerce')

    credits = read_nested_csv(data_dir / 'credits.csv', ['cast', 'crew'])
    keywords = read_nested_csv(data_dir / 'keywords.csv', ['keywords'])
    links = pd.read_csv(data_dir / 'links.csv').dropna(subset=['tmdbId'])
    links = links.rename(columns={'tmdbId': 'id', 'movieId': 'movielens_id'})[['id', 'movielens_id']]
    links['id'] = links['id'].astype(int)

    movies = (movies.merge(credits, on='id', how='left')
                    .merge(keywords, on='id', how='left')
                    .merge(links, on='id', how='left'))
    movies['movielens_id'] = movies['movielens_id'].astype('Int64')
    movies = movies.rename(columns={'id': '_id'})

    for record in movies.to_dict('records'):
        doc = drop_missing(record)
        if 'release_date' in doc:
            doc['release_date'] = doc['release_date'].to_pydatetime()
        yield doc

def build_rating_docs(data_dir: Path, genres_by_movie: dict[int, list[str]]) -> Iterator[Document]:
    links = pd.read_csv(data_dir / LINKS_FILE).dropna(subset=['tmdbId'])
    # ratings.csv uses MovieLens ids; map them to TMDB ids so movie_id matches movies._id
    ml_to_tmdb = dict(zip(links['movieId'], links['tmdbId'].astype(int)))

    for chunk in pd.read_csv(data_dir / RATINGS_FILE, chunksize=200_000):
        chunk['movie_id'] = chunk['movieId'].map(ml_to_tmdb)
        chunk = chunk.dropna(subset=['movie_id'])
        timestamps = pd.to_datetime(chunk['timestamp'], unit='s')

        for user_id, movie_id, rating, timestamp in zip(chunk['userId'], chunk['movie_id'],
                                                         chunk['rating'], timestamps):
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
