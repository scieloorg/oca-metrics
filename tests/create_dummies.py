import bson
import orjson
import gzip
import os

# Create dummy SciELO BSON
docs = [
    {"_id": "1", "doi": "10.1234/1", "collection": "scl", "pid_v2": "S1", "publication_year": 2024},
    {"_id": "2", "doi": "10.1234/2", "collection": "scl", "pid_v2": "S2", "publication_year": 2024},
    {"_id": "3", "doi": "10.1234/3", "collection": "scl", "pid_v2": "S3", "publication_year": 2024},
]

with open("tests/temp_data/input.bson", "wb") as f:
    for doc in docs:
        f.write(bson.encode(doc))

# Create dummy OpenAlex GZ
lines = [
    {"id": "W1", "type": "article", "publication_year": 2024, "doi": "10.1234/1"},
    {"id": "W2", "type": "book", "publication_year": 2023, "doi": "10.1234/2"},
    {"id": "W3", "type": "article", "publication_year": 2024, "doi": "10.1234/3"},
]

with gzip.open("tests/temp_data/input_openalex/updated_date=2024-01-01/part_0.gz", "wb") as f:
    for line in lines:
        f.write(orjson.dumps(line) + b"\n")