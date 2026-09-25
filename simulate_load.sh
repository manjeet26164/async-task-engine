#!/bin/bash

TARGET_URL="${1:-http://localhost:8080/api/v1/jobs/submit}"
API_KEY="${2:-taskflow-secret-key-2026}"
TOTAL_REQUESTS=200

echo "TaskFlow Load Simulator ($TOTAL_REQUESTS requests)"
echo "Target: $TARGET_URL"

START_TIME=$(date +%s)
TASK_TYPES=("IMAGE_PROCESSING" "SEND_EMAIL" "PAYMENT_GATEWAY" "DATA_SYNC" "DOCUMENT_OCR")

SUCCESS_COUNT=0
CONFLICT_COUNT=0
ERROR_COUNT=0

TEMP_DIR=$(mktemp -d)

echo "Dispatching $TOTAL_REQUESTS concurrent requests..."

for i in $(seq 1 $TOTAL_REQUESTS); do
  TYPE_INDEX=$((i % ${#TASK_TYPES[@]}))
  TASK_TYPE="${TASK_TYPES[$TYPE_INDEX]}"
  FAIL_FLAG="false"
  if [ $((i % 15)) -eq 0 ]; then
    FAIL_FLAG="true" # Trigger simulated failure on ~7% of jobs to populate DLQ
  fi

  IDEMP_KEY="load-test-$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid 2>/dev/null || echo "$i-$RANDOM")"

  (
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$TARGET_URL" \
      -H "Content-Type: application/json" \
      -H "Idempotency-Key: $IDEMP_KEY" \
      -H "X-API-KEY: $API_KEY" \
      -d "{\"taskType\": \"$TASK_TYPE\", \"payload\": \"{\\\"taskIndex\\\": $i, \\\"fail\\\": $FAIL_FLAG}\"}")
    echo "$HTTP_CODE" >> "$TEMP_DIR/results.txt"
  ) &
done

wait

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

if [ -f "$TEMP_DIR/results.txt" ]; then
  SUCCESS_COUNT=$(grep -c "202" "$TEMP_DIR/results.txt" || true)
  CONFLICT_COUNT=$(grep -c "409" "$TEMP_DIR/results.txt" || true)
  ERROR_COUNT=$(grep -v -E "202|409" "$TEMP_DIR/results.txt" | wc -l || true)
fi

rm -rf "$TEMP_DIR"

echo -e "\nLoad Test Results:"
echo "Total requests: $TOTAL_REQUESTS"
echo "Accepted (202): $SUCCESS_COUNT"
echo "Conflict (409): $CONFLICT_COUNT"
echo "Errors:         $ERROR_COUNT"
echo "Duration:       ${ELAPSED}s"
echo "Dashboard:      http://localhost:8080/index.html"
