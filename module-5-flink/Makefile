.PHONY sessionization_job:
# Ensure env vars KAFKA_URL, KAFKA_TOPIC, KAFKA_GROUP, KAFKA_WEB_TRAFFIC_KEY, KAFKA_WEB_TRAFFIC_SECRET,
# POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD are set
	 docker compose exec jobmanager ./bin/flink run -py /opt/src/job/sessionization_job.py --pyFiles /opt/src -d

