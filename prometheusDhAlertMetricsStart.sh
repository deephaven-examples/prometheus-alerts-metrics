docker build --tag prometheus-deephaven/server .
docker build --tag flask/prometheus-webhook-alerts flask-app
docker-compose up $1
