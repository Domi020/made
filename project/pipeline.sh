#!/bin/bash

# IMPORTANT: Set these two variables for your own Destatis-Genesis account, if you want to run the Destatis pipeline
GENESIS_USER=
GENESIS_PASSWORD=

cd "$(dirname "$0")"
cd ..

echo "Run pipeline 1/2: KBA"
python project/kba_pipeline.py
echo "pipeline 1 finished"

echo "Run pipeline 2/2: Destatis Genesis"
if [ -z ${GENESIS_USER} ];
then 
    echo "Pipeline 2 not executed: Please enter a Genesis-Destatis user and password (can be created for free) in this script to access their data collection";
    echo "pipeline 2 canceled";
else 
    python project/destatis_pipeline.py $GENESIS_USER $GENESIS_PASSWORD;
    echo "pipeline 2 finished";
fi

