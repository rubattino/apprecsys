#!/bin/bash
export YAHOO_DATA=/home/mert/yahoo/
screen -dmS test bash -c 'PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --ip=* --port=8080" /home/mert/spark-1.4.1-bin-hadoop2.6/./bin/pyspark --driver-memory 2g; exec bash'
