import genesis_puller
import sys
import pandas as pd
import sqlalchemy as sqla

if len(sys.argv) != 3:
    print("Usage: python destatis_pipeline.py <genesis_username> <genesis_password>")
    exit(1)

sqlite_file_engine = sqla.create_engine('sqlite:///data/kba.sqlite')
genesis_puller.get_csv_from_genesis(sys.argv[1], sys.argv[2])
csv_dataframe = pd.read_csv(filepath_or_buffer='data/accidents.csv', sep=';', header=0)
csv_dataframe.to_sql(name='accidents', con=sqlite_file_engine, 
                    if_exists='replace', # so that the script can be run multiple times without error/duplicate data
                    index=False) # don't create extra index column
