# for testing the anomaly detection IFoT build.  

This thing is meant to be a unit test for it.  
# Main Components
## Broker_Testing.py

* Meant to be a unit test for broker capabilities:
    * This includes the test functions.
    * Anomaly detection functions.
* You must run `anomaly_prepare.py` first in order to setup the dataset for the experiment.
    * However, as of `05-20-2021` this dataset is not up-to-date and must be modified first.
    * The functionaility remains the same though.
* I've forgotten what exactly the functions do but i will keep a running list of them here.

## Test functionalities (These are all in the broker_testin.py)
### For brokers
* `TEST_TASK_QUERY`
* `TEST_TASK_PIPELINE_START`
* `TEST_TASK_SEQUENTIAL_PIPELINE_START`
* `TEST_TASK_PIPELINE_START`

### For workers
* `TEST_TASK_QUERY`
* `TEST_TASK_PIPELINE_START`
* `TEST_TASK_SEQUENTIAL_PIPELINE_START`
* `TEST_TASK_PIPELINE_START`

### Anomaly Detection
* `ANOMALY_DETECT_TOGGLE_SENDING`
* `ANOMALY_DETECT_SEND_MEANS`
* `ANOMALY_DETECT_TOGGLE_ATTACK`

# Other files

## Anomaly detection preparation
* For preparing "attacked" dataset just for testing.

## Cluster attack test
* `test_cluster_attack.py`: I think this is not used anymore
* `test_ping.py`: This is a basic test file for simple single ZMQ messaging.
* `test_base.py`: I think this is not used anymore
I won't commit these until I know that I still use them.