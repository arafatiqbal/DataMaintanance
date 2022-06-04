#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Consumer
import json
import ccloud_lib
from google.cloud import storage
import zlib
import os

client = storage.Client.from_service_account_json('creds.json')
bucket = client.get_bucket("datamaintenance")

if __name__ == '__main__':
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)


    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'python_example_group_1'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = Consumer(consumer_conf)

    consumer.subscribe([topic])
    all_data = []


    total_count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                all_data.append(data)
                count = data['count']
                total_count += count
                print("Consumed record with key {} and value {}, and updated total count to {}".format(record_key, record_value, total_count))
    except KeyboardInterrupt:
        pass
    finally:
        all_dataJson = json.dumps(all_data)
        compress(all_dataJson, level=-1)
        with open('all_data.json', 'a',encoding='utf-8') as f:
            json.dump(all_dataJson, f, ensure_ascii=False, indent=4)

        blob = bucket.blob("data")
        blob.upload_from_filename("./all_data.json")
        consumer.close()
