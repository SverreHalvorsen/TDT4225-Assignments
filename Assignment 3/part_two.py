from collections import defaultdict
from itertools import combinations
from pprint import pprint

from DatabaseManagement import DatabaseManager


def task_one(program: DatabaseManager):
    print("Task 1: Top 10 Directors with 5+ movies by median revenue\n")

    return list(program.db.movies.aggregate([
        {"$unwind": "$crew"},
        {"$match": {"crew.job": "Director"}},
        {"$group": {
            "_id": "$crew.name",
            "movie_count": {"$sum": 1},
            "median_revenue": {"$median": {"input": "$revenue", "method": "approximate"}},
            "mean_vote_average": {"$avg": "$vote_average"}
        }},
        {"$match": {"movie_count": {"$gte": 5}}},
        {"$sort": {"median_revenue": -1}},
        {"$limit": 10}
    ]))

def task_two(program: DatabaseManager):
    print("Task 2: Actor pairs with 3+ movies together by co-appearances\n")

    pair_stats = defaultdict(lambda: [0, 0.0, 0])

    frequent_actors = {d["_id"] for d in program.db.movies.aggregate([
        {"$project": {"names": {"$setUnion": ["$cast.name", []]}}},
        {"$unwind": "$names"},
        {"$group": {"_id": "$names", "movie_count": {"$sum": 1}}},
        {"$match": {"movie_count": {"$gte": 3}}}
    ])}

    movies = program.db.movies.find(
        {"cast.1": {"$exists": True}},
        {"cast": 1, "vote_average": 1, "_id": 0}
    )

    for movie in movies:
        names = sorted({c["name"] for c in movie["cast"] if c.get("name")} & frequent_actors)
        vote = movie.get("vote_average")

        for pair in combinations(names, 2):
            stats = pair_stats[pair]
            stats[0] += 1
            if vote is not None:
                stats[1] += vote
                stats[2] += 1

    result = [
        {
            "actor_1": a,
            "actor_2": b,
            "co_appearances": count,
            "avg_vote_average": round(vote_sum / vote_count, 2) if vote_count > 0 else None,
        }
        for (a, b), (count, vote_sum, vote_count) in pair_stats.items()
        if count >= 3
    ]
    result.sort(key=lambda x: (-x["co_appearances"], -(x["avg_vote_average"] or 0)))

    print(f"{len(result):,} pairs with 3+ movies together, showing the top 10\n")
    return result[:10]

def task_three(program: DatabaseManager):
    print("Task 3: Top 10 actors with 10+ credited movies by widest genre breadth\n")

    return list(program.db.movies.aggregate([
        {"$project": {"cast": 1, "genres.name": 1}},
        {"$unwind": "$cast"},
        {"$unwind": "$genres"},
        {"$group": {
            "_id": "$cast.name",
            "movie_ids": {"$addToSet": "$_id"},
            "genre_breadth": {"$addToSet": "$genres.name"}
        }},
        {"$project": {
            "_id": 0,
            "actor": "$_id",
            "movie_count": {"$size": "$movie_ids"},
            "genre_breadth": {"$size": "$genre_breadth"},
            "example_genres": {"$slice": ["$genre_breadth", 5]}
        }},
        {"$match": {"movie_count": {"$gte": 10}}},
        {"$sort": {"genre_breadth": -1, "movie_count": -1}},
        {"$limit": 10}
    ]))

def task_four(program: DatabaseManager):
    print("Task 4: Top 10 collections with 3+ movies with largest total revenue\n")

    return list(program.db.movies.aggregate([
        {"$match": {"belongs_to_collection": {"$ne": None}}},
        {"$group": {
            "_id": "$belongs_to_collection.name",
            "movie_count": {"$sum": 1},
            "total_revenue": {"$sum": "$revenue"},
            "median_vote_average": {"$median": {"input": "$vote_average", "method": "approximate"}},
            "earliest_release_date": {"$min": "$release_date"},
            "latest_release_date": {"$max": "$release_date"}
        }},
        {"$match": {"movie_count": {"$gte": 3}}},
        {"$sort": {"total_revenue": -1}},
        {"$limit": 10}
    ]))

def task_five(program: DatabaseManager):
    print("Task 5: Median runtime and count of movies per decade and primary genre\n")

    return list(program.db.movies.aggregate([
        {"$match": {"release_date": {"$ne": None}, "genres.0": {"$exists": True}, "runtime": {"$ne": None}}},
        {"$project": {
            "decade": {"$multiply": [{"$floor": {"$divide": [{"$year": "$release_date"}, 10]}}, 10]},
            "primary_genre": {"$arrayElemAt": ["$genres.name", 0]},
            "runtime": 1
        }},
        {"$group": {
            "_id": {
                "decade": "$decade",
                "primary_genre": "$primary_genre"
            },
            "median_runtime": {"$median": {"input": "$runtime", "method": "approximate"}},
            "movie_count": {"$sum": 1}
        }},
        {"$sort": {"_id.decade": 1, "median_runtime": -1}}

    ]))

def task_six(program: DatabaseManager):
    print("Task 6: Average female proportion of top 5 billed cast per decade\n")

    return list(program.db.movies.aggregate([
        {"$match": {"release_date": {"$ne": None}, "cast.0": {"$exists": True}}},
        {"$project": {
            "decade": {"$multiply": [{"$floor": {"$divide": [{"$year": "$release_date"}, 10]}}, 10]},
            "top5": {"$slice": [{"$sortArray": {"input": "$cast", "sortBy": {"order": 1}}}, 5]}
        }},
        {"$project": {
            "decade": 1,
            "known": {"$filter": {"input": "$top5", "cond": {"$in": ["$$this.gender", [1, 2]]}}}
        }},
        {"$match": {"known.0": {"$exists": True}}},
        {"$project": {
            "decade": 1,
            "female_proportion": {"$divide": [
                {"$size": {"$filter": {"input": "$known", "cond": {"$eq": ["$$this.gender", 1]}}}},
                {"$size": "$known"}
            ]}
        }},
        {"$group": {
            "_id": "$decade",
            "avg_female_proportion": {"$avg": "$female_proportion"},
            "movie_count": {"$sum": 1}
        }},
        {"$sort": {"avg_female_proportion": -1}}
    ]))

def task_seven(program: DatabaseManager):
    print("Task 7: Top 20 'noir' / 'neo-noir' movies with 50+ votes by vote_average\n")

    return list(program.db.movies.aggregate([
        {"$match": {"$text": {"$search": "noir"}, "vote_count": {"$gte": 50}}},
        {"$project": {
            "_id": 0,
            "title": 1,
            "year": {"$year": "$release_date"},
            "vote_average": 1,
            "vote_count": 1
        }},
        {"$sort": {"vote_average": -1, "vote_count": -1}},
        {"$limit": 20}
    ]))

def task_eight(program: DatabaseManager):
    print("Task 8: Top 20 director-actor pairs with 3+ movies (100+ votes) by mean vote_average\n")

    return list(program.db.movies.aggregate([
        {"$match": {"vote_count": {"$gte": 100}}},
        {"$project": {
            "directors": {"$setUnion": [{"$map": {
                "input": {"$filter": {"input": "$crew", "cond": {"$eq": ["$$this.job", "Director"]}}},
                "in": "$$this.name"
            }}, []]},
            "actors": {"$setUnion": ["$cast.name", []]},
            "vote_average": 1,
            "revenue": 1
        }},
        {"$unwind": "$directors"},
        {"$unwind": "$actors"},
        {"$match": {"$expr": {"$ne": ["$directors", "$actors"]}}},
        {"$group": {
            "_id": {"director": "$directors", "actor": "$actors"},
            "film_count": {"$sum": 1},
            "mean_vote_average": {"$avg": "$vote_average"},
            "mean_revenue": {"$avg": "$revenue"}
        }},
        {"$match": {"film_count": {"$gte": 3}}},
        {"$sort": {"mean_vote_average": -1, "film_count": -1}},
        {"$limit": 20}
    ]))

def task_nine(program: DatabaseManager):
    print("Task 9: Top 10 non-English original languages among US productions\n")

    return list(program.db.movies.aggregate([
        {"$match": {
            "original_language": {"$exists": True, "$ne": "en"},
            "production_countries.iso_3166_1": "US"
        }},
        {"$group": {
            "_id": "$original_language",
            "count": {"$sum": 1},
            "example_title": {"$first": "$title"}
        }},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]))

def task_ten(program: DatabaseManager):
    print("Task 10: Top 10 most genre-diverse and highest-variance users (20+ ratings)\n")

    result = list(program.db.ratings.aggregate([
        {"$group": {
            "_id": "$user_id",
            "ratings_count": {"$sum": 1},
            "std": {"$stdDevPop": "$rating"},
            "genre_lists": {"$addToSet": "$genres"}
        }},
        {"$match": {"ratings_count": {"$gte": 20}}},
        {"$project": {
            "ratings_count": 1,
            "variance": {"$pow": ["$std", 2]},
            "distinct_genres": {"$size": {"$reduce": {
                "input": "$genre_lists",
                "initialValue": [],
                "in": {"$setUnion": ["$$value", "$$this"]}
            }}}
        }},
        {"$facet": {
            "most_genre_diverse": [
                {"$sort": {"distinct_genres": -1, "ratings_count": -1}},
                {"$limit": 10}
            ],
            "highest_variance": [
                {"$sort": {"variance": -1}},
                {"$limit": 10}
            ]
        }}
    ], allowDiskUse=True))

    return result[0]

tasks = [task_one, task_two, task_three, task_four, task_five, task_six, task_seven, task_eight, task_nine, task_ten]


def run_tasks(program: DatabaseManager):
    for task in tasks:
        pprint(task(program))
        print("-" * 50)

def main():
    program = DatabaseManager()
    try:
        run_tasks(program)
    finally:
        program.connection.close_connection()

if __name__ == "__main__":
    main()

