# Deltatre.TechTest
Tech test for deltatre

---Components---
1.confluent-kafka cluster 
- spinned up using quick start docker image

2.kafka producer - ck-producer/MessageGenerator.py
Python script to generate random user messages and produce on topic 'user-events-1'

3.Pyspark Consumer
https://hub.docker.com/r/jupyter/pyspark-notebook/ - A convenient docker image with spark and python setup.
Setup spark streaming consumer using KafkaUtils.CreateDirectStream to consume data from kafka topic 'user-events-1'

4.confluent-kafka producer 
Aggregates the streaming data using spark's RDDs and Dataframe transformations to derive the firstseen and lastseen details of each user. 
Used confluent-kafka producer api to produce it to destination topic - 'user-summary-aggregation'

Note:
Ignore consumer folder. Just implemented for testing Confluent Kafka consumer API







