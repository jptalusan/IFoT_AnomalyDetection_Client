import zmq
import json
import threading
import time
import random

encode = lambda x: x.encode('ascii')
decode = lambda x: x.decode('utf-8')
current_milli_time = lambda: int(round(time.time() * 1000))

def TEST_TASK_QUERY(client, loop=1):
  payload = json.dumps({
                        "task_type": "TEST_TASK_QUERY",
                        "worker": '0000',
                        })
  for i in range(loop):
    client.send_multipart([b'receive_query', encode(payload)], flags=zmq.DONTWAIT)
    print("TEST_TASK_QUERY:SENT")
  time.sleep(0.3)

def TEST_TASK_PIPELINE_START(client, loop=1, ack=False):
  payload = json.dumps({
                        "task_type": "TEST_TASK_PIPELINE_START",
                        "pipeline": [0, 1, 2]
                        })
  for i in range(loop):
    client.send_multipart([b'receive_query', encode(payload)], flags=zmq.DONTWAIT)
    print(f"TEST_TASK_PIPELINE_START:SENT {i}")
    if ack:
          msg = client.recv_multipart()
          print(msg)

  time.sleep(0.3)

def TEST_TASK_SEQUENTIAL_PIPELINE_START(client, loop=1, ack=False, randomized=False):
  pipeline = [0, 1, 2]
  if randomized:
        random.shuffle(pipeline)
  payload = json.dumps({
                        "task_type": "TEST_TASK_SEQUENTIAL_PIPELINE_START",
                        "pipeline": pipeline
                        })
  for i in range(loop):
    client.send_multipart([b'receive_query', encode(payload)], flags=zmq.DONTWAIT)
    print(f"TEST_TASK_SEQUENTIAL_PIPELINE_START:SENT {i}")
    if ack:
          msg = client.recv_multipart()
          print(msg)
  time.sleep(0.3)

# I can also include a duration parameter here...
def ANOMALY_DETECT_SEND_MEANS(client):
  payload = json.dumps({
                        "task_type": "ANOMALY_DETECT_SEND_MEANS"
                        })
  client.send_multipart([b'receive_query', encode(payload)], flags=zmq.DONTWAIT)
  print("ANOMALY_DETECT_SEND_MEANS:SENT")
  time.sleep(0.3)

def ANOMALY_DETECT_TOGGLE_SENDING(client):
  payload = json.dumps({
                        "task_type": "ANOMALY_DETECT_TOGGLE_SENDING"
                        })
  client.send_multipart([b'receive_query', encode(payload)], flags=zmq.DONTWAIT)
  print("ANOMALY_DETECT_TOGGLE_SENDING:SENT")
  time.sleep(0.3)

def ANOMALY_DETECT_TOGGLE_ATTACK(client):
  payload = json.dumps({
                        "task_type": "ANOMALY_DETECT_TOGGLE_ATTACK"
                        })
  client.send_multipart([b'receive_query', encode(payload)], flags=zmq.DONTWAIT)
  print("ANOMALY_DETECT_TOGGLE_ATTACK:SENT")
  time.sleep(0.3)

def listener():
  context = zmq.Context()
  client = context.socket(zmq.ROUTER)
  # client.identity = encode('Client-0001')
  host = '*'
  port = 5999
  client.bind(f'tcp://{host}:{port}')
  while True:
    msg = client.recv_multipart()
    if len(msg) >= 2:
    # for worker task query so far.
      payload = json.loads(decode(msg[2]))
      time_sent = float(payload['time_received'])
      time_received = current_milli_time()
      time_elapsed = (time_received - time_sent) / 1000.0
      print(f"{payload['task_type']} {payload['result']} in {time_elapsed} s")
      client.send_multipart([msg[0], b'ACK', encode(payload['t_id'])])

if __name__ == '__main__':
  threading.Thread(target=listener, args = ()).start()
  IDENTITY    = ''
  TIMEOUT     = 5 # seconds

  # Testing broker
  BROKER_HOST = 'localhost'
  BROKER_PORT = 9000

  context = zmq.Context()
  broker = context.socket(zmq.DEALER)
  broker.set_hwm(0)
  broker.identity = encode('163.221.68.230:5999')
  broker.connect(f'tcp://{BROKER_HOST}:{BROKER_PORT}')

  # Test cases
  # TEST_TASK_QUERY(broker, loop=10)
  # TEST_TASK_SEQUENTIAL_PIPELINE_START(broker, loop=10, ack=True)
  TEST_TASK_PIPELINE_START(broker, loop=10, ack=False)

  # Testing worker directly
  WORKER_HOST = 'localhost'
  WORKER_PORT = 6000 # RSU-0000

  context = zmq.Context()
  worker = context.socket(zmq.DEALER)
  worker.set_hwm(0)
  worker.identity = encode('163.221.68.230:5999')
  worker.connect(f'tcp://{WORKER_HOST}:{WORKER_PORT}')

  # Test cases
  # TEST_TASK_QUERY(worker)
  # TEST_TASK_PIPELINE_START(worker)
  # TEST_TASK_SEQUENTIAL_PIPELINE_START(worker)
  # TEST_TASK_PIPELINE_START(worker, loop=10)

  # # Prepare data to be attacked
  # WORKER_HOST = 'localhost'
  # for i in range(3):
  #   WORKER_PORT = 6000 + i # RSU-0000
  #   context = zmq.Context()
  #   worker = context.socket(zmq.DEALER)
  #   worker.identity = encode('163.221.68.230:5999')
  #   worker.connect(f'tcp://{WORKER_HOST}:{WORKER_PORT}')
  #   ANOMALY_DETECT_TOGGLE_SENDING(worker)
  #   ANOMALY_DETECT_SEND_MEANS(worker)
  #   ANOMALY_DETECT_TOGGLE_ATTACK(worker)
  # # time.sleep(120)
  # # ANOMALY_DETECT_TOGGLE_SENDING(worker)

  # WORKER_HOST = '163.221.68.248'
  # for i in range(3):
  #   WORKER_PORT = 6000 + i # RSU-0000
  #   context = zmq.Context()
  #   worker = context.socket(zmq.DEALER)
  #   worker.identity = encode('163.221.68.230:5999')
  #   worker.connect(f'tcp://{WORKER_HOST}:{WORKER_PORT}')
  #   ANOMALY_DETECT_TOGGLE_SENDING(worker)
  #   ANOMALY_DETECT_SEND_MEANS(worker)
  #   ANOMALY_DETECT_TOGGLE_ATTACK(worker)