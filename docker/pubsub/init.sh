#!/bin/sh
# Creates topics and subscriptions on the Pub/Sub emulator. Idempotent: 409 on re-run is fine.
set -eu
HOST=${PUBSUB_HOST:-localhost:8085}
P=projects/${PROJECT:-poc-local}

put() {
  body=${2:-'{}'}
  curl -s -o /dev/null -w "%{http_code} $1\n" -X PUT "http://$HOST/v1/$1" -H 'content-type: application/json' -d "$body"
}

for t in order-events payment-events order-events-dlq payment-events-dlq; do put "$P/topics/$t"; done

sub() {
  put "$P/subscriptions/$1" "{\"topic\":\"$P/topics/$2\",\"ackDeadlineSeconds\":15,\"enableMessageOrdering\":true,
    \"deadLetterPolicy\":{\"deadLetterTopic\":\"$P/topics/$2-dlq\",\"maxDeliveryAttempts\":5},
    \"retryPolicy\":{\"minimumBackoff\":\"10s\",\"maximumBackoff\":\"600s\"}}"
}
sub payment-service.order-events order-events
sub order-service.payment-events payment-events
