#!/bin/bash

PYTHON_ENV_DIR=".env"

if [[ -d "$PYTHON_ENV_DIR" ]]
then
	echo "$PYTHON_ENV_DIR exists on your filesystem."
else
	python3 -m venv .env
	echo "Create $PYTHON_ENV_DIR"
fi

# activate virtual env
. .env/bin/activate

# installing requirements
pip install -r requirements.txt

# run server
python3 post_processor_tcpd.py
