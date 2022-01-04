docker build --tag prometheus-deephaven/grpc-api .
docker build --tag local-flask/prometheus-webhook-alerts flask-app
docker-compose up
