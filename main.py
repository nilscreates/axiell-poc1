# main.py
import os, re, asyncio
from typing import Optional, List, Dict, Any
from datetime import datetime

import httpx
from fastapi import FastAPI, Query
from elasticsearch import Elasticsearch
from tenacity import retry, stop_after_attempt, wait_exponential

app = FastAPI(title="Author Enrichment Service")

# ==== Config (Railway envs) ===================================================
ELASTIC_URL        = os.environ.get("ELASTIC_URL") or "http://localhost:9200"
ELASTIC_API_KEY    = os.environ.get("ELASTIC_API_KEY")  # REQUIRED for this setup
ELASTIC_VERIFY     = os.environ.get("ELASTIC_VERIFY_CERTS", "true").lower() != "false"

# Index/pipeline names
ES_WORKS_INDEX     = os.environ.get("ELASTIC_INDEX", "quria-fields-subset-full004")
ES_AUTHORS_INDEX   = os.environ.get("ES_AUTHORS_INDEX", "authors")
ES_ELSER_PIPELINE  = os.environ.get("ES_ELSER_PIPELINE", "author_profile_elser")

# OpenLibrary polite delay (seconds) between requests
OL_DELAY           = float(os.environ.get("OL_DELAY", "0.5"))

# ==== Elasticsearch client (API key only) =====================================
es = Elasticsearch(
    ELASTIC_URL,
    api_key=ELASTIC_API_KEY,
    request_timeout=60,
    verify_certs=ELASTIC_VERIFY,
)

# ==== Utils ===================================================================
def to_year(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    m = re.search(r"\d{4}", str(s))
    return int(m.group(0)) if m else None

def score_candidate(d: Dict[str, Any], target_name: str, target_birth: Optional[int]) -> int:
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
    return sorted(docs, key=lambda x: score_candidate(x, target_name, target_birth), reverse=True)[0]

def extract_bio(detail: Dict[str, Any]) -> Optional[str]:
    v = detail.get("bio") or detail.get("description")
    if not v:
        return None
    if isinstance(v, str):
        return v
    if isinstance(v, dict) and isinstance(v.get("value"), str):
        return v["value"]
    return None

def index_author_doc(olid: str, payload: Dict[str, Any]) -> None:
    es.index(index=ES_AUTHORS_INDEX, id=olid, document=payload, pipeline=ES_ELSER_PIPELINE, refresh="false")

# ==== OpenLibrary (async) =====================================================
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

async def enrich_one(name: str, birth_year: Optional[int]) -> Dict[str, Any]:
    async with httpx.AsyncClient() as client:
        data = await ol_search_author(client, name)
        docs = data.get("docs", [])
        best = pick_best_author(docs, name, birth_year)
        if not best:
            return {"status": "no_match", "name": name}

        olid = (best.get("key") or "").split("/")[-1]  # "/authors/OL21594A" -> "OL21594A"
        detail = await ol_get_author(client, olid)
        bio = extract_bio(detail) or f"{name}. Author."
        subjects = best.get("top_subjects") or []
        variants = best.get("alternate_names") or []

        doc = {
            "author_id": olid,
            "name": best.get("name") or name,
            "birth": to_year(best.get("birth_date")) or birth_year,
            "variants": variants,
            "top_subjects": subjects,
            "bio_text": bio,
            "source": {
                "openlibrary_id": olid,
                "openlibrary_url": f"https://openlibrary.org/authors/{olid}",
                "match_confidence": int(best.get("work_count") or 0),
                "last_fetched": datetime.utcnow().isoformat()
            }
        }
        index_author_doc(olid, doc)
        # polite delay to avoid rate limiting
        await asyncio.sleep(OL_DELAY)
        return {"status": "ok", "olid": olid, "name": doc["name"], "subjects": subjects[:8]}

# ==== HTTP endpoints ==========================================================
@app.get("/health")
def health():
    try:
        return {"ok": bool(es.ping())}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}

@app.get("/diag")
def diag():
    # TEMPORARY: remove when stable
    info = {
        "url_set": bool(ELASTIC_URL),
        "has_api_key": bool(ELASTIC_API_KEY),
        "verify_certs": ELASTIC_VERIFY,
        "works_index": ES_WORKS_INDEX,
        "authors_index": ES_AUTHORS_INDEX,
        "pipeline": ES_ELSER_PIPELINE,
        "ol_delay": OL_DELAY,
    }
    try:
        es.info()
        info["es_info_ok"] = True
    except Exception as e:
        info["es_info_ok"] = False
        info["reason"] = str(e)[:300]
    return info

@app.get("/")
def root():
    return {"ok": True, "service": "Author Enrichment Service"}

@app.post("/enrich/author")
async def enrich_author(
    name: str = Query(..., description="Exact display name from works"),
    birth: Optional[int] = Query(None, description="Birth year if known")
):
    return await enrich_one(name, birth)

@app.post("/enrich/batch")
async def enrich_batch(
    limit: int = Query(200, ge=1, le=20000),
    start_after_name: Optional[str] = None,
    start_after_birth: Optional[str] = None
):
    """
    Backfill distinct authors from works using a composite aggregation cursor.
    Returns how many processed and a next_after_key you can pass back to continue.
    """
    processed = 0
    after_key = {"name": start_after_name, "birth": start_after_birth} if (start_after_name is not None and start_after_birth is not None) else None

    while processed < limit:
        body = {
            "size": 0,
            "aggs": {
                "authors": {
                    "composite": {
                        "size": min(1000, limit - processed),
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
            after_key = None
            break

        for b in buckets:
            if processed >= limit:
                break
            key = b["key"]
            name = key.get("name")
            birth_year = to_year(key.get("birth"))
            try:
                await enrich_one(name, birth_year)
            except Exception as e:
                print(f"[enrich_batch] error for '{name}': {e}")
            processed += 1

        after_key = resp["aggregations"]["authors"].get("after_key")
        if not after_key:
            break

    return {"processed": processed, "next_after_key": after_key}

# ==== Entrypoint (Railway uses $PORT) =========================================
# Start command for Railway:
# uvicorn main:app --host 0.0.0.0 --port $PORT
