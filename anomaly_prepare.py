import pulsar
import os
import pickle5 as pickle
import json
import itertools
import math
import random

from pprint import pprint

if not os.path.exists(os.path.join(os.getcwd(), 'data')):
    raise OSError("Must first download data, see README.md")
data_dir = os.path.join(os.getcwd(), 'data')

encode = lambda x: x.encode('ascii')
decode = lambda x: x.decode('utf-8')

def read_pickle(path):
  with open(path, 'rb') as f:
      data = pickle.load(f)
  return data

def pulsar_publish(payload):
  PULSAR_HOST = '163.221.68.242'
  PULSAR_PORT = 6650

  pulsar_address = f"pulsar://{PULSAR_HOST}:{PULSAR_PORT}"
  client = pulsar.Client(service_url=pulsar_address,
                         log_conf_file_path="empty")

  debug = client.create_producer(topic=f'non-persistent://public/default/debug', 
                                 producer_name='client')
  # payload = {'hello': 'world!', 
  #            'test': [1, 2, 3]}
  payload = encode(json.dumps(payload))
  debug.send(payload, disable_replication=False)
  print(f"Published message: {payload}")

def select_sensors(clusters, percent_attacked=0.30):
    all_sensors = list(itertools.chain.from_iterable(clusters))
#     print(all_sensors)
    return sensors

def decide_attacks(clusters, percent_attacked=0.30):
  all_sensors = list(itertools.chain.from_iterable(clusters))
  num_attacked = math.floor(len(all_sensors) * percent_attacked)
  random.seed(100)
  sensors = random.sample(all_sensors, num_attacked)
  print(all_sensors, '\n', sensors)

  _dict = {}

  for i, c in enumerate(clusters):
      _dict[str(i).zfill(4)] = {}
      _dict[str(i).zfill(4)]['sensors'] = c
      _dict[str(i).zfill(4)]['attacked'] = list(set(c) & set(sensors))
      _dict[str(i).zfill(4)]['ATTACK_START'] = "2018-03-5 10:00:00-05:00"
      _dict[str(i).zfill(4)]['ATTACK_END'] = "2018-03-5 14:00:00-05:00"
      _dict[str(i).zfill(4)]['ATTACK_MODE'] = "deductive"

  return _dict

if __name__ == '__main__':

  clusters = read_pickle(data_dir + '/clusters.pkl')

  # _dict = {}
  # for i, c in enumerate(clusters):
  #     _dict[str(i).zfill(4)] = c

  _dict = decide_attacks(clusters)
  _dict['task_type'] = 'GENERATE_MEANS'
  _dict['granularity'] = 10
  pulsar_publish(_dict)

  pprint(_dict)
  _dict['task_type'] = 'GENERATE_ATTACK_DATA'
  _dict['granularity'] = 10

  pprint(_dict)
  pulsar_publish(_dict)