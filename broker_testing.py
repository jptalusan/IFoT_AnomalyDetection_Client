import zmq
import json
import threading
import time
import pulsar

encode = lambda x: x.encode('ascii')
decode = lambda x: x.decode('utf-8')

def TEST_TASK_QUERY(client):
  payload = json.dumps({
                        "task_type": "TEST_TASK_QUERY",
                        "worker": '0000',
                        })
  client.send_multipart([b'receive_query', encode(payload)], flags=zmq.DONTWAIT)
  print("TEST_TASK_QUERY:SENT")
  time.sleep(0.3)

def TEST_TASK_PIPELINE_START(client, loop=1):
  payload = json.dumps({
                        "task_type": "TEST_TASK_PIPELINE_START",
                        "pipeline": [0, 1, 2]
                        })
  for i in range(loop):
    client.send_multipart([b'receive_query', encode(payload)], flags=zmq.DONTWAIT)
    print(f"TEST_TASK_PIPELINE_START:SENT {i}")
  time.sleep(0.3)

def TEST_TASK_SEQUENTIAL_PIPELINE_START(client):
  payload = json.dumps({
                        "task_type": "TEST_TASK_SEQUENTIAL_PIPELINE_START",
                        "pipeline": [0, 1, 2]
                        })
  client.send_multipart([b'receive_query', encode(payload)], flags=zmq.DONTWAIT)
  print("TEST_TASK_SEQUENTIAL_PIPELINE_START:SENT")
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
    for m in msg:
      if m == b'receive_query' or m == b'rsu-0000':
        continue
      print(decode(m))
      print()

if __name__ == '__main__':
  threading.Thread(target=listener, args = ()).start()
  IDENTITY    = ''
  TIMEOUT     = 5 # seconds

  # Testing broker
  BROKER_HOST = 'localhost'
  BROKER_PORT = 9000

  context = zmq.Context()
  broker = context.socket(zmq.DEALER)
  broker.identity = encode('163.221.68.230:5999')
  broker.connect(f'tcp://{BROKER_HOST}:{BROKER_PORT}')

  # Test cases
  # TEST_TASK_QUERY(broker)
  # TEST_TASK_PIPELINE_START(broker)
  # TEST_TASK_SEQUENTIAL_PIPELINE_START(broker)

  # TEST_TASK_PIPELINE_START(broker, loop=10)

  # Testing worker directly
  WORKER_HOST = 'localhost'
  WORKER_PORT = 6000 # RSU-0000

  context = zmq.Context()
  worker = context.socket(zmq.DEALER)
  worker.identity = encode('163.221.68.230:5999')
  worker.connect(f'tcp://{WORKER_HOST}:{WORKER_PORT}')

  # Test cases
  # TEST_TASK_QUERY(worker)
  # TEST_TASK_PIPELINE_START(worker)
  # TEST_TASK_SEQUENTIAL_PIPELINE_START(worker)
  # TEST_TASK_PIPELINE_START(worker, loop=10)

  # Prepare data to be attacked
  WORKER_HOST = 'localhost'
  for i in range(3):
    WORKER_PORT = 6000 + i # RSU-0000
    context = zmq.Context()
    worker = context.socket(zmq.DEALER)
    worker.identity = encode('163.221.68.230:5999')
    worker.connect(f'tcp://{WORKER_HOST}:{WORKER_PORT}')
    ANOMALY_DETECT_TOGGLE_SENDING(worker)
    ANOMALY_DETECT_SEND_MEANS(worker)
    ANOMALY_DETECT_TOGGLE_ATTACK(worker)
  # time.sleep(120)
  # ANOMALY_DETECT_TOGGLE_SENDING(worker)