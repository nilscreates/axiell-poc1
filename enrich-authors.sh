#!/bin/bash
set -e

BASE_URL="https://axiell-poc1-production.up.railway.app"
LIMIT=400
STATE_FILE=".enrich_state"

# Load last cursor if it exists
if [ -f "$STATE_FILE" ]; then
  START_AFTER_NAME=$(cat $STATE_FILE | jq -r '.name')
  START_AFTER_BIRTH=$(cat $STATE_FILE | jq -r '.birth')
else
  START_AFTER_NAME=""
  START_AFTER_BIRTH=""
fi

while true; do
  if [ -z "$START_AFTER_NAME" ]; then
    RESP=$(curl -s -X POST "$BASE_URL/enrich/batch?limit=$LIMIT")
  else
    RESP=$(curl -s -X POST "$BASE_URL/enrich/batch?limit=$LIMIT&start_after_name=$START_AFTER_NAME&start_after_birth=$START_AFTER_BIRTH")
  fi

  echo "$RESP" | jq .

  # Pull cursor for next loop
  NEXT_NAME=$(echo "$RESP" | jq -r '.next_after_key.name // empty')
  NEXT_BIRTH=$(echo "$RESP" | jq -r '.next_after_key.birth // empty')

  if [ -z "$NEXT_NAME" ]; then
    echo "✅ Done — no more authors."
    rm -f "$STATE_FILE"
    break
  fi

  # Save cursor for resume
  echo "{\"name\":\"$NEXT_NAME\", \"birth\":\"$NEXT_BIRTH\"}" > $STATE_FILE

  # URL encode for next call
  START_AFTER_NAME=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$NEXT_NAME")
  START_AFTER_BIRTH="$NEXT_BIRTH"
done
