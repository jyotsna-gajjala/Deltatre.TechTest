import json
from confluent_kafka import Consumer, Producer,admin,TopicPartition,KafkaError
import confluent_kafka


def commit_completed(err, partitions):
    if err:
        print("commit error", str(err))
    else:
        print("Committed partition offsets: " + str(partitions))

def my_assign (consumer, partitions):
        for p in partitions:
            #p.offset = confluent_kafka.OFFSET_BEGINNING
            p.offset = 1
            #p.offset = 1662060
        print(f'assigned---- {format(partitions)}')
        consumer.assign(partitions)

def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        #outMsg = msg.value().decode("utf-8")
        print(f"Delivered to user-summary-1 ")
    return None

def main():
    print('Initiating consumer-----')
    brokers = "10.0.75.1:9092"
    consumer_topic = "user-events-1"

    c = Consumer(
        {
            "bootstrap.servers": brokers,
            "auto.offset.reset": 'earliest',
            "group.id": "consumer1",
            "enable.auto.commit": True,
            #"auto.commit.interval.ms": 3000,
            #"max.poll.interval.ms": 100000,
            "on_commit": commit_completed,
        }
    )    

    #c.subscribe([consumer_topic], on_assign=my_assign)
    c.subscribe([consumer_topic])
    print('Starting to consume from --',consumer_topic)
    print('Initiating producer-----')
    produce_topic = "user-summary-1"
    producer = Producer({"bootstrap.servers": brokers})
    
    while True:

        try:
            msg = c.poll(timeout=1000)
            if msg is None:
                continue
            if msg.error():

                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    print('error----')
                    raise KafkaException(msg.error())
            else:

                kafkaMsg = msg.value().decode("utf-8")
                print('consumed msg-----',kafkaMsg)
                print('producing summary msg now---')
                producer.produce(
                    produce_topic, kafkaMsg, callback=delivery_report,
                )
                producer.poll(0)
                producer.flush()

        except KeyboardInterrupt:
            print("Interrupted by keyboard, exiting",)
        
        # finally:
        #     c.close()
    



if __name__ == "__main__":
    main()