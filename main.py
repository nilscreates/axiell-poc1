
from fastapi import FastAPI, Request
import requests
import os

app = FastAPI()

ELASTIC_URL = os.getenv("ELASTIC_URL")
ELASTIC_INDEX = os.getenv("ELASTIC_INDEX")
ELASTIC_USERNAME = os.getenv("ELASTIC_USERNAME")
ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD")

@app.post("/search_catalogue")
async def search_catalogue(request: Request):
    payload = await request.json()

    semantic_query = payload.get("semantic_query")
    keywords = payload.get("keywords")
    language = payload.get("language_filter")
    pub_date_from = payload.get("pub_date_from")
    pub_date_to = payload.get("pub_date_to")
    format_filter = payload.get("format_filter")

    query = {
        "size": 3,
        "query": {
            "bool": {
                "must": [
                    {
                        "text_expansion": {
                            "merged_descriptives.tokens": {
                                "model_id": ".elser_model_2_linux-x86_64",
                                "model_text": semantic_query
                            }
                        }
                    }
                ],
                "should": [
                    {
                        "multi_match": {
                            "query": keywords,
                            "fields": [
                                "title^3",
                                "expression_title^2",
                                "creator_name^2",
                                "subjects^1.5",
                                "genres^1.2",
                                "statement_of_responsibility"
                            ],
                            "fuzziness": "AUTO"
                        }
                    }
                ],
                "filter": []
            }
        }
    }

    if language:
        query["query"]["bool"]["filter"].append({"term": {"language": language}})
    if pub_date_from or pub_date_to:
        range_filter = {"range": {"publication_date": {}}}
        if pub_date_from:
            range_filter["range"]["publication_date"]["gte"] = pub_date_from
        if pub_date_to:
            range_filter["range"]["publication_date"]["lte"] = pub_date_to
        query["query"]["bool"]["filter"].append(range_filter)
    if format_filter:
        query["query"]["bool"]["filter"].append({"term": {"manifestation_type": format_filter}})

    response = requests.post(
        f"{ELASTIC_URL}/{ELASTIC_INDEX}/_search",
        auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD),
        json=query
    )

    hits = response.json().get("hits", {}).get("hits", [])
    results = [
        {
            "title": hit["_source"].get("title") or hit["_source"].get("expression_title"),
            "summary": hit["_source"].get("description") or "No summary available."
        }
        for hit in hits
    ]

    return {"results": results}
