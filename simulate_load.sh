#!/bin/bash

TARGET_URL="${1:-http://localhost:8080/api/v1/jobs/submit}"
API_KEY="${2:-taskflow-secret-key-2026}"
TOTAL_REQUESTS=200

echo -e "\033[0;36m========================================================\033[0m"
echo -e "\033[1;33m 🚀 TaskFlow Concurrency Load Simulator ($TOTAL_REQUESTS Tasks)\033[0m"
echo -e "\033[0;37m Target URL: $TARGET_URL\033[0m"
echo -e "\033[0;36m========================================================\033[0m"

START_TIME=$(date +%s)
TASK_TYPES=("IMAGE_PROCESSING" "SEND_EMAIL" "PAYMENT_GATEWAY" "DATA_SYNC" "DOCUMENT_OCR")

SUCCESS_COUNT=0
CONFLICT_COUNT=0
ERROR_COUNT=0

TEMP_DIR=$(mktemp -d)

echo -e "\033[0;32mDispatching $TOTAL_REQUESTS concurrent requests...\033[0m"

for i in $(seq 1 $TOTAL_REQUESTS); do
  TYPE_INDEX=$((i % ${#TASK_TYPES[@]}))
  TASK_TYPE="${TASK_TYPES[$TYPE_INDEX]}"
  FAIL_FLAG="false"
  if [ $((i % 15)) -eq 0 ]; then
    FAIL_FLAG="true"
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

echo -e "\n\033[0;36m================ Load Test Summary ================\033[0m"
echo -e " Total Requests Dispatched: $TOTAL_REQUESTS"
echo -e " \033[0;32mHTTP 202 Accepted:        $SUCCESS_COUNT\033[0m"
echo -e " \033[1;33mHTTP 409 Conflict:        $CONFLICT_COUNT\033[0m"
echo -e " \033[0;31mOther / Errors:           $ERROR_COUNT\033[0m"
echo -e " Elapsed Time:             ${ELAPSED}s"
echo -e "\033[0;36m====================================================\033[0m"
echo -e "\033[0;32m✨ Open http://localhost:8080/index.html to view real-time queue consumption!\033[0m"
