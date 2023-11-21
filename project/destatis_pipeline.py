import genesis_puller
import sys

if len(sys.argv) != 3:
    print("Usage: python destatis_pipeline.py <genesis_username> <genesis_password>")
    exit(1)
genesis_puller.get_csv_from_genesis(sys.argv[1], sys.argv[2])