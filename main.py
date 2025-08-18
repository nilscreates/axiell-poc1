import os, time, re
from typing import Optional, List, Dict, Any
from datetime import datetime

import httpx
from fastapi import FastAPI, Query
from elasticsearch import Elasticsearch
from tenacity import retry, stop_after_attempt, wait_exponential

ES_URL  = os.environ.get("ELASTIC_URL")
ES_USER = os.environ.get("ELASTIC_USERNAME")
ES_PASS = os.environ.get("ELASTIC_PASSWORD")
ES_WORKS_INDEX   = os.environ.get("ELASTIC_INDEX", "quria-fields-subset-full004")
ES_AUTHORS_INDEX = os.environ.get("ES_AUTHORS_INDEX", "authors")
ES_ELSER_PIPELINE= os.environ.get("ES_ELSER_PIPELINE", "author_profile_elser")
OL_DELAY_MS = int(os.environ.get("OPENLIBRARY_DELAY_MS", "200"))

es = Elasticsearch(ES_URL, basic_auth=(ES_USER, ES_PASS), request_timeout=60)

app = FastAPI(title="Author Enrichment Service")

# --- Utils -------------------------------------------------------------

def to_year(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    m = re.search(r"\d{4}", str(s))
    return int(m.group(0)) if m else None

def _score_candidate(d: Dict[str, Any], target_name: str, target_birth: Optional[int]) -> int:
    lname = (target_name or "").strip().lower()
    exact = 1000 if (d.get("name") or "").strip().lower() == lname else 0
    wc = int(d.get("work_count") or 0)
    birth_bonus = 0
    if target_birth:
        by = to_year(d.get("birth_date"))
        if by:
            birth_bonus = 100 - abs(by - target_birth)
    return exact + birth_bonus + wc

def pick_best_author(docs: List[Dict[str, Any]], target_name: str, target_birth: Optional[int]) -> Optional[Dict[str, Any]]:
    if not docs:
        return None
    return sorted(docs, key=lambda x: _score_candidate(x, target_name, target_birth), reverse=True)[0]

def extract_bio(detail: Dict[str, Any]) -> Optional[str]:
    v = detail.get("bio") or detail.get("description")
    if not v:
        return None
    if isinstance(v, str):
        return v
    if isinstance(v, dict) and "value" in v and isinstance(v["value"], str):
        return v["value"]
    return None

# --- OpenLibrary -------------------------------------------------------

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.5, max=4))
async def ol_search_author(client: httpx.AsyncClient, name: str) -> Dict[str, Any]:
    r = await client.get("https://openlibrary.org/search/authors.json", params={"q": name}, timeout=20)
    r.raise_for_status()
    return r.json()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.5, max=4))
async def ol_get_author(client: httpx.AsyncClient, olid: str) -> Dict[str, Any]:
    r = await client.get(f"https://openlibrary.org/authors/{olid}.json", timeout=20)
    r.raise_for_status()
    return r.json()

# --- Elasticsearch helpers --------------------------------------------

def index_author_doc(olid: str, payload: Dict[str, Any]) -> None:
    es.index(index=ES_AUTHORS_INDEX, id=olid, document=payload, pipeline=ES_ELSER_PIPELINE, refresh="false")

def upsert_single_author(name: str, birth_year: Optional[int] = None) -> Dict[str, Any]:
    """Blocking helper for one author (used by the HTTP handler)."""
    import asyncio
    async def _run():
        async with httpx.AsyncClient() as client:
            data = await ol_search_author(client, name)
            docs = data.get("docs", [])
            best = pick_best_author(docs, name, birth_year)
            if not best:
                return {"status": "no_match", "name": name}

            olid = (best.get("key") or "").split("/")[-1]  # handles "/authors/OLxxxA" or "OLxxxA"
            detail = await ol_get_author(client, olid)
            bio = extract_bio(detail) or f"{name}. Author."
            subjects = best.get("top_subjects") or []
            variants = best.get("alternate_names") or []

            doc = {
                "author_id": olid,
                "name": best.get("name") or name,
                "birth": to_year(best.get("birth_date")) or birth_year,
                "variants": variants,
                "bio_text": bio,
                "top_subjects": subjects,
                "source": {
                    "openlibrary_id": olid,
                    "openlibrary_url": f"https://openlibrary.org/authors/{olid}",
                    "match_confidence": int(best.get("work_count") or 0),
                    "last_fetched": datetime.utcnow().isoformat()
                }
            }
            index_author_doc(olid, doc)
            await asyncio.sleep(OL_DELAY_MS / 1000.0)
            return {"status": "ok", "olid": olid, "name": doc["name"], "subjects": subjects[:8]}
    return asyncio.run(_run())

def composite_page(after_key: Optional[Dict[str, Any]] = None, size: int = 1000) -> Dict[str, Any]:
    body = {
        "size": 0,
        "aggs": {
            "authors": {
                "composite": {
                    "size": size,
                    "sources": [
                        {"name": {"terms": {"field": "creator_name.keyword"}}},
                        {"birth": {"terms": {"field": "creator_birth.keyword"}}}
                    ]
                }
            }
        }
    }
    if after_key:
        body["aggs"]["authors"]["composite"]["after"] = after_key
    return es.search(index=ES_WORKS_INDEX, body=body)

# --- HTTP endpoints ----------------------------------------------------

@app.get("/health")
def health():
    ok = es.ping()
    return {"ok": ok}

@app.post("/enrich/author")
def enrich_author(name: str = Query(..., description="Exact display name"),
                  birth: Optional[int] = Query(None, description="Birth year if known")):
    """Enrich a single author by name (+/- birth year) from OpenLibrary into ES."""
    res = upsert_single_author(name, birth)
    return res

@app.post("/enrich/batch")
def enrich_batch(limit: int = Query(500, ge=1, le=20000),
                 start_after_name: Optional[str] = None,
                 start_after_birth: Optional[str] = None):
    """Batch-enrich distinct authors from works using a composite aggregation cursor."""
    count = 0
    after_key = None
    if start_after_name is not None and start_after_birth is not None:
        after_key = {"name": start_after_name, "birth": start_after_birth}

    client = httpx.Client(timeout=20)
    try:
        while count < limit:
            # page composite agg
            body = {
                "size": 0,
                "aggs": {
                    "authors": {
                        "composite": {
                            "size": min(1000, limit - count),
                            "sources": [
                                {"name": {"terms": {"field": "creator_name.keyword"}}},
                                {"birth": {"terms": {"field": "creator_birth.keyword"}}}
                            ]
                        }
                    }
                }
            }
            if after_key:
                body["aggs"]["authors"]["composite"]["after"] = after_key
            resp = es.search(index=ES_WORKS_INDEX, body=body)
            buckets = resp["aggregations"]["authors"]["buckets"]
            if not buckets:
                break

            for b in buckets:
                if count >= limit:
                    break
                key = b["key"]
                name = key.get("name")
                birth_year = to_year(key.get("birth"))
                # fetch + index (sync wrapper calling async OL)
                try:
                    result = upsert_single_author(name, birth_year)
                except Exception as e:
                    result = {"status": "error", "name": name, "error": str(e)}
                count += 1

            after_key = resp["aggregations"]["authors"].get("after_key")
            if not after_key:
                break
    finally:
        client.close()

    return {"processed": count, "next_after_key": after_key}
