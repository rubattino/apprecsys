#!/bin/bash
export YAHOO_DATA=/media/mert/Data/yahoo/
screen -dmS test bash -c 'PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --ip=* --port=8080" /home/mert/code/spark-1.6.0-bin-hadoop2.6/./bin/pyspark --driver-memory 2g; exec bash'
