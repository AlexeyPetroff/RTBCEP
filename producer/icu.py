#!/usr/bin/python

import json
import sys
import uuid
import os
from time import sleep, time

import random
from kafka import KafkaProducer


campaign_ids = []
ad_ids = {}
pub_ids = []
dev_ids = []
path = os.path.dirname(os.path.abspath(__file__))


def read_countries_from_file():
    with open(path+'/countries.txt') as f:
        content =  f.readlines()
        return [x.strip('\n') for x in content]


def read_models_from_file():
    with open(path+'/models.txt') as f:
        content =  f.readlines()
        return [x.strip('\n') for x in content]


countries = read_countries_from_file()
models = read_models_from_file()


def fill_random_campaign_ids():
    campaign_ids.append(str(uuid.uuid4()))


def fill_random_ad_ids():
    for campaign_id in campaign_ids:
        for _ in range(3):
            ad_ids.setdefault(campaign_id, []).append(str(uuid.uuid4()))


def fill_random_pub_ids():
    for _ in range(1000):
        pub_ids.append(str(uuid.uuid4()))


def fill_random_dev_ids():
    for _ in range(10000):
        dev_ids.append(str(uuid.uuid4()))


def random_country(dev_id):
    if random.random() < 0.988:
        h = hash(dev_id)
        random.seed(h)
    country = random.choice(countries)
    random.seed()
    return country


def random_model():
    return random.choice(models)


def random_age():
    return random.randint(10, 50)


def random_event():
    events = []
    camp_id = random.choice(campaign_ids)
    ad_id = random.choice(ad_ids[camp_id])
    timestamp = time()
    dev_id = random.choice(dev_ids)
    country = random_country(dev_id)
    os = random.choice(['Android', 'OS'])

    model = random.choice(models)
    age = random_age()
    gender = random.choice(['M', 'F'])
    pub_id = random.choice(pub_ids)
    impression = {'capaign_id': camp_id, 'ad_id': ad_id, 'timestamp': timestamp, 'country': country, 'os': os,
                  'dev_id': dev_id, 'model': model, 'age': age, 'gender': gender, 'pub_id': pub_id,
                  'type': 'impression'}
    events.append(impression)
    if random.random() < 0.01:
        if random.random() < 0.98:
            sleep(1)
        click = dict(impression, **{'timestamp': time(), 'type': 'click'})
        events.append(click)
        if random.random() < 0.25:
            conversion = dict(impression, **{'timestamp': time(), 'type': 'conversion'})
            events.append(conversion)
    return events


server = "localhost:9092"


def main():
    # the topic
    topic = sys.argv[1]

    # create a Kafka producer
    producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'), bootstrap_servers=server)
    print("*** Stream on " + server + ", topic : " + topic + "***")
    fill_random_campaign_ids()
    fill_random_ad_ids()
    fill_random_pub_ids()
    fill_random_dev_ids()
    try:
        while True:
            for _ in range(1, 2000):
                # get random events
                events = random_event()
                for e in events:
                    producer.send(topic, e, key=e['ad_id'])
                    print("Sending event: %s" %(json.dumps(e).encode('utf-8')))
            sleep(7)

    except KeyboardInterrupt:
        pass

    print("\nSTOPPED!")
    producer.flush()


if __name__ == "__main__":
    main()
