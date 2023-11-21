#!/bin/bash

GENESIS_USER=
GENESIS_PASSWORD=

source ../.venv/bin/activate
cd ..

echo "Run pipeline 1/2: KBA"
python project/kba_pipeline.py

echo "Run pipeline 2/2: Destatis Genesis"
if [ -z ${GENESIS_USER+x} ];
then echo "Please enter a Genesis-Destatis user and password (can be created for free) in this script to access their data collection";
else python project/destatis_pipeline.py $GENESIS_USER $GENESIS_PASSWORD;
fi

