from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
import os
import requests

app = FastAPI()

# Read credentials from Railway environment
ELASTIC_URL      = os.getenv("ELASTIC_URL")
ELASTIC_USERNAME = os.getenv("ELASTIC_USERNAME")
ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD")
ELASTIC_INDEX    = os.getenv("ELASTIC_INDEX")

class SearchRequest(BaseModel):
    semantic_query: str
    keywords: Optional[str]      = None
    format_filter: Optional[str] = None

@app.post("/search_catalogue")
def search_catalogue(request: SearchRequest):
    must_clauses = []

    # Semantic (ELSER) clause
    must_clauses.append({
        "semantic": {
            "field": "merged_descriptives",
            "query": request.semantic_query
        }
    })

    # Lexical keyword match
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

    # Optional format filter
    filters = []
    if request.format_filter:
        filters.append({
            "term": {
                "manifestation_type.keyword": request.format_filter
            }
        })

    body = {
        "query": {
            "bool": {
                "must":   must_clauses,
                "filter": filters
            }
        }
    }

    # Call Elasticsearch via HTTP
    resp = requests.post(
        f"{ELASTIC_URL}/{ELASTIC_INDEX}/_search",
        auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD),
        json=body,
        verify=False
    )
    resp.raise_for_status()

    hits = resp.json().get("hits", {}).get("hits", [])
    results = [
        {
            "title": hit["_source"].get("title") or hit["_source"].get("expression_title"),
            "summary": hit["_source"].get("description") or ""
        }
        for hit in hits
    ]

    return {"results": results}
