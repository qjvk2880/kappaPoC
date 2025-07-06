.PHONY: create-topic install-requirements produce all

create-topic:
	@echo "🔧 Creating Kafka topic..."
	docker exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
		--create \
		--topic real-estate-topic \
		--bootstrap-server localhost:9092 \
		--partitions 1 \
		--replication-factor 1
	@echo "✅ Kafka topic created."

install-requirements:
	@echo "🔧 Installing Python requirements..."
	source .venv/bin/activate && pip install -r requirements.txt
	@echo "✅ Requirements installed."

produce:
	@echo "🔧 Running producer.py..."
	.venv/bin/python producer.py
	@echo "✅ Producer finished."

create-table:
	@echo "🔧 Creating property table in PostgreSQL..."
	docker exec -i postgres psql -U user -d real_estate -c "\
	CREATE TABLE IF NOT EXISTS property (\
	  id SERIAL PRIMARY KEY,\
	  zip_code VARCHAR(10),\
	  road_address TEXT,\
	  lot_address TEXT\
	);"
	@echo "✅ Table created."

consumer:
	@echo "🔧 Running Spark Structured Streaming consumer..."
	docker exec -it spark-master \
	  env SPARK_LOCAL_DIRS=/tmp SPARK_USER_HOME=/tmp SPARK_SUBMIT_OPTS="-Divy.home=/opt/bitnami/spark/.ivy2 -Duser.name=appuser" \
	  spark-submit \
	    --master spark://spark-master:7077 \
	    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
	    --jars /opt/spark/jars/postgresql-42.2.27.jar \
	    /app/spark_consumer.py
	@echo "✅ Spark consumer finished."

all: create-topic install-requirements produce