#!/bin/bash

SERVER_COUNT=`ps awx | grep -c "python3[ ]post_processor_tcpd.py"`
SERVER_PATH="/home/ubuntu/tc_test/python_server"

#echo $SERVER_COUNT
if  [ $SERVER_COUNT -eq 0 ] ; then
    echo "Python Server Not Running!"
	cd ${SERVER_PATH}
    bash run_server.sh &
else
    echo "TCP Server already Running!"
fi
