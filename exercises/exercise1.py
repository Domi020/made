import pandas as pd
import sqlalchemy as sqla

### CONSTANTS, URLS, FILE REFERENCES

# address/file reference to the local sqlite db and table name
sqlite_reference = 'sqlite:///airports.sqlite'
table_name = 'airports'

# url to the csv which has to be imported
data_url = 'https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv'

# column data type mapping
type_dict = {
    "column_1": sqla.INTEGER,
    "column_2": sqla.TEXT,
    "column_3": sqla.TEXT,
    "column_4": sqla.TEXT,
    "column_5": sqla.VARCHAR(3),
    "column_6": sqla.VARCHAR(4),
    "column_7": sqla.FLOAT,
    "column_8": sqla.FLOAT,
    "column_9": sqla.INTEGER,
    "column_10": sqla.FLOAT,
    "column_11": sqla.CHAR(1),
    "column_12": sqla.TEXT,
    "geo_punkt": sqla.TEXT,
}



### MAIN PIPELINE

# create SQLAlchemy engine pointing to the sqlite file
sqlite_file_engine = sqla.create_engine(sqlite_reference)

# download and read csv to dataframe (includes header => header=0)
csv_dataframe = pd.read_csv(filepath_or_buffer=data_url, sep=';', header=0)

# use dataframe method to directly import it to the sqlite engine
csv_dataframe.to_sql(name=table_name, con=sqlite_file_engine, 
                    if_exists='replace', # so that the script can be run multiple times without error/duplicate data
                    dtype=type_dict,
                    index=False) # don't create extra index column




