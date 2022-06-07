docker compose exec broker \
  kafka-topics --create \
    --topic extraction \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1
