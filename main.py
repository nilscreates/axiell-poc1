from fastapi import FastAPI, Query
from pydantic import BaseModel
from typing import Optional
#from elasticsearch import Elasticsearch
import os

app = FastAPI()

# Read environment variables
ELASTIC_URL = os.getenv("ELASTIC_URL")
ELASTIC_USERNAME = os.getenv("ELASTIC_USERNAME")
ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD")
ELASTIC_INDEX = os.getenv("ELASTIC_INDEX")

# Initialize Elasticsearch client
es = Elasticsearch(
    ELASTIC_URL,
    basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD),
    verify_certs=False
)

class SearchRequest(BaseModel):
    semantic_query: str
    keywords: Optional[str] = None
    format_filter: Optional[str] = None

@app.post("/search_catalogue")
def search_catalogue(request: SearchRequest):
    must_clauses = []

    # Semantic query (ELSER)
    if request.semantic_query:
        must_clauses.append({
            "semantic": {
                "field": "merged_descriptives",
                "query": request.semantic_query
            }
        })

    # Keyword match block
    if request.keywords:
        must_clauses.append({
            "multi_match": {
                "query": request.keywords,
                "fields": [
                    "title",
                    "expression_title",
                    "subjects",
                    "genres",
                    "creator_name",
                    "statement_of_responsibility",
                    "literary_form",
                    "isbns"
                ]
            }
        })

    # Filter block (format)
    filters = []
    if request.format_filter:
        filters.append({
            "term": {
                "manifestation_type.keyword": request.format_filter
            }
        })

    # Final query
    query_body = {
        "query": {
            "bool": {
                "must": must_clauses,
                "filter": filters
            }
        }
    }

    response = es.search(index=ELASTIC_INDEX, body=query_body)
    hits = response.get("hits", {}).get("hits", [])
    results = [hit["_source"] for hit in hits]

    return {"results": results}
