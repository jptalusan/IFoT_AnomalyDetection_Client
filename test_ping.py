import zmq
import json

encode = lambda x: x.encode('ascii')
decode = lambda x: x.decode('utf-8')

if __name__ == '__main__':
  # DEALER TEST waits for response

  context = zmq.Context()
  client = context.socket(zmq.DEALER)
  client.identity = encode('Client-0000')

  host = 'localhost'

  # add a loop to send a command to different RSUs
  port = 9000
  client.connect(f'tcp://{host}:{port}')
  payload = json.dumps({"task_type": "TEST_TASK_QUERY",
                        "test":"hello",
                        "start_time": "2018-03-27 00:00:00-05:00",
                        "end_time": "2018-03-27 23:59:59-05:00",
                        "worker": "0000"
                        })


  for i in range(1, 20):
    client.send_multipart([b'receive_query', encode(payload)], flags=zmq.DONTWAIT)
    print("Sent a test query")

    message = client.recv_multipart()
    [print(decode(m)) for m in message]

  # REQUEST TEST
  # ms = 500
  # context = zmq.Context()
  # sock = context.socket(zmq.REQ)
  # sock.identity = encode('Client-0000')
  # sock.setsockopt(zmq.SNDTIMEO, ms)
  # sock.setsockopt(zmq.RCVTIMEO, ms)
  # sock.setsockopt(zmq.REQ_RELAXED, 1)
  # sock.setsockopt(zmq.REQ_CORRELATE, 1)
  
  # sock.setsockopt(zmq.LINGER, ms) # Discard pending buffered socket messages on close().
  # sock.setsockopt(zmq.CONNECT_TIMEOUT, ms)
  # sock.connect('tcp://localhost:9000')
  
  # for i in range(1, 2):
  #     print("Sending a ping...")

  #     payload = json.dumps({
  #                     "data": str(i),
  #                     })

  #     sock.send_multipart([b'receive_query', encode(payload)])
  #     print("Sent a ping...")
  #     try:
  #       rep = sock.recv_json()  # This blocks until we get something
  #       print('Ping got reply:', rep)
  #     except zmq.error.Again as e:
  #       print("Boom")
  #       print(e)
  #       # break
  # sock.close()