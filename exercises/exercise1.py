import pandas as pd
import sqlalchemy as sqla

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

# Create SQLAlchemy engine pointing to the sqlite file
sqlite_file_engine = sqla.create_engine('sqlite:///airports.sqlite')

csv_dataframe = pd.read_csv(filepath_or_buffer='https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv',
            sep=';', header=0)

csv_dataframe.to_sql(name='airports', con=sqlite_file_engine, 
                    if_exists='replace', 
                    dtype=type_dict,
                    index=False)




