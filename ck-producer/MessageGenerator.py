
import json
import random
from random import randint
import string
import uuid
from randomtimestamp import randomtimestamp
import datetime
from datetime import datetime
import time
from confluent_kafka import Consumer, Producer,admin,TopicPartition,KafkaError
import confluent_kafka


def produce_user_events():
    
    ##Generate randowm user events for producing on Kafka topic -- user_events
    random_key = str(randint(1,10))
    randomstr = ''.join([random.choice(string.ascii_letters + string.digits) for n in range(4)])
    random_time = int(datetime.timestamp(randomtimestamp(2019,False)))
    user_dict = {}
    metadata = {}
    metadata['messageid'] = str(uuid.uuid4())
    metadata['sent_at'] = random_time
    metadata['timestamp'] = random_time
    metadata['received_at'] = random_time+100
    metadata['apikey'] = 'apikey'+ random_key
    metadata['spaceid'] = 'spaceid' + random_key
    metadata['version'] =  'version' + random_key
    event_data = {}
    event_data['movieid'] = 'MIM'+ randomstr
    user_dict['userid']=  'user'+str(randint(1, 100))
    user_dict['type']= 'event'
    user_dict['metadata']= metadata
    user_dict['event']= 'played movie'
    user_dict['event_data']= event_data
    
    #print(user_dict)
    return json.dumps(user_dict)


###Producer delivery report
def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        #outMsg = msg.value().decode("utf-8")
        print(f"Delivered to user-events-1 ")
    return None

def main():

    print('Initiating producer-----')
    brokers = "10.0.75.1:9092"
    produce_topic = "user-events-1"
    producer = Producer({"bootstrap.servers": brokers})
    while True:
    
        data = produce_user_events()
        producer.produce(
                            produce_topic, data, callback=delivery_report,
                        )
        producer.poll(0)
        producer.flush()
        time.sleep(5)

if __name__ == "__main__":
    main()
