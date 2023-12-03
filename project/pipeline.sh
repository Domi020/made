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
    echo "WARNING: No Genesis-Destatis user account was entered. Please enter a account in the pipeline.sh if you want to download the Destatis data or alternatively user a pre-downloaded csv of dataset '46241-0011'"
    python project/destatis_pipeline.py;
    echo "pipeline 2 finished";
else 
    python project/destatis_pipeline.py $GENESIS_USER $GENESIS_PASSWORD;
    echo "pipeline 2 finished";
fi

