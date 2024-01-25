import pandas as pd
import sqlalchemy as sqla
import urllib.request
import zipfile

### CONSTANTS, URLS, FILE REFERENCES

# address/file reference to the local sqlite db and table name
sqlite_reference = 'sqlite:///gtfs.sqlite'
table_name = 'stops'

# url to the zip which has to be imported
data_url = 'https://gtfs.rhoenenergie-bus.de/GTFS.zip'

# column data type mapping for sqlite
type_dict = {
    "stop_id": sqla.INTEGER,
    "stop_name": sqla.TEXT,
    "stop_lat": sqla.FLOAT,
    "stop_lon": sqla.FLOAT,
    "zone_id": sqla.INTEGER
}


### FUNCTIONS

def retrieve_data(data_url: str) -> str:
    """Download zip file and extract given file
    :return path to extract file
    """
    urllib.request.urlretrieve(data_url, "GTFS.zip")
    with zipfile.ZipFile("GTFS.zip", 'r') as zip_directory:
        return zip_directory.extract("stops.txt")
    
def transform_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Transform given dataframe according to the requirements"""
    df = dataframe[["stop_id", "stop_name", "stop_lat", "stop_lon", "zone_id"]] # only those columns are needed
    df = df[df["zone_id"] == 2001] # only data in zone 2001
    return df

def filter_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Filter given dataframe according to the requirements"""
    df = dataframe[(dataframe["stop_lat"] >= -90) & (dataframe["stop_lat"] <= 90)] # -90 <= stop_lat, stop_lon <= 90
    df = df[(df["stop_lon"] >= -90) & (df["stop_lon"] <= 90)]
    return df

def load_dataframe_to_db(dataframe: pd.DataFrame) -> None:
    """Load dataframe to sqlite file"""
    # create SQLAlchemy engine pointing to the sqlite file
    sqlite_file_engine = sqla.create_engine(sqlite_reference)

    # use dataframe method to directly import it to the sqlite engine
    dataframe.to_sql(name=table_name, con=sqlite_file_engine, 
                        if_exists='replace', # so that the script can be run multiple times without error/duplicate data
                        dtype=type_dict,
                        index=False) # don't create extra index column


### MAIN PIPELINE

# download and read csv to dataframe (includes header => header=0)
file_path = retrieve_data(data_url)

# read csv to pandas dataframe
csv_dataframe = pd.read_csv(filepath_or_buffer=file_path, sep=',', quotechar='"', header=0)

# transform and filter the dataframe as needed
df = transform_dataframe(csv_dataframe)
df = filter_dataframe(df)

# load to sqlite file
load_dataframe_to_db(df)