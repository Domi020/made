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

def retrieve_data(data_url: str, local_file_name: str, file_path_in_zip: str) -> str:
    """Download zip file and extract given file
    :param data_url: the url to the online resource
    :param local_file_name: the local file name under which the online zip should be saved to
    :param file_path_in_zip: the name of the to be extracted file in the zip
    :return path to the extracted file
    """
    urllib.request.urlretrieve(data_url, local_file_name)
    with zipfile.ZipFile(local_file_name, 'r') as zip_directory:
        return zip_directory.extract(file_path_in_zip)
    
def transform_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Transform and filter the given dataframe according to the requirements. The following steps are done:
    - throw away unnecessary columns
    - choose only data from zone 2001
    :param dataframe: the to be transformed dataframe
    :return the transformed dataframe
    """
    df = dataframe[["stop_id", "stop_name", "stop_lat", "stop_lon", "zone_id"]]
    df = df[df["zone_id"] == 2001]
    return df

def validate_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Validate given dataframe according to the requirements. The following steps are done:
    - Only rows are kept where stop_lat/stop_lon are between -90 and 90
    :param dataframe: the to be validated dataframe
    :return the validated dataframe
    """
    df = dataframe[(dataframe["stop_lat"] >= -90) & (dataframe["stop_lat"] <= 90)]
    df = df[(df["stop_lon"] >= -90) & (df["stop_lon"] <= 90)]
    return df

def load_dataframe_to_db(dataframe: pd.DataFrame, type_dict: dict, sqlite_file: str, table_name: str) -> None:
    """Load dataframe to a sqlite file/table
    :param dataframe: the to be saved data
    :param type_dict: a python dict of {column_name : column_type} to map the correct types to the columns
    :param sqlite_file: the path to the sqlite file (is created if it does not exist)
    :param table_name: the table name under which the data should be saved
    """
    # create SQLAlchemy engine pointing to the sqlite file
    sqlite_file_engine = sqla.create_engine(sqlite_file)

    # use dataframe method to directly import it to the sqlite engine
    dataframe.to_sql(name=table_name, con=sqlite_file_engine, 
                        if_exists='replace', # so that the script can be run multiple times without error/duplicate data
                        dtype=type_dict,
                        index=False) # don't create extra index column


### MAIN PIPELINE

# download and unzip GTFS data
file_path = retrieve_data(data_url, "GTFS.zip", "stops.txt")

# read csv to pandas dataframe
csv_dataframe = pd.read_csv(filepath_or_buffer=file_path, sep=',', quotechar='"', header=0)

# transform and validate the dataframe as needed
df = transform_dataframe(csv_dataframe)
df = validate_dataframe(df)

# load to sqlite file
load_dataframe_to_db(df, type_dict, sqlite_reference, table_name)