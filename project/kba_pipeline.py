import urllib.request
import openpyxl
import pandas as pd
import numpy as np
import sqlalchemy as sqla

def get_file_from_internet(online_file, local_file):
    urllib.request.urlretrieve(online_file, local_file)

def load_data_from_excel(file_name):
    wb = openpyxl.load_workbook(filename=file_name)
    table = wb['FE1.2']
    return table['B41':'M53']

def transform_to_df(data):
    df = pd.DataFrame(data)
    return df.applymap(lambda x: x.value)

def transform_dataframe(df, year):
    # Rename columns
    df = df.rename(columns={
        0: 'Alter',
        1: 'A1',
        2: 'A2',
        3: 'A',
        4: 'B',
        5: 'B96, BE',
        6: 'C1, C1E',
        7: 'C, CE',
        8: 'D1, D1E',
        9: 'D, DE',
        10: 'Zusammen',
        11: 'Fahrerlaubnisse bzw. Führerscheine'
    })

    # Add year
    df['Jahr'] = year

    # Change datatypes (all numbers are integer)
    df = df.astype({
        'Alter': str,
        'Jahr': 'Int64',
        'A1': 'Int64',
        'A2': 'Int64',
        'A': 'Int64',
        'B': 'Int64',
        'B96, BE': 'Int64',
        'C1, C1E': 'Int64',
        'C, CE': 'Int64',
        'D1, D1E': 'Int64',
        'D, DE': 'Int64',
        'Zusammen': 'Int64',
        'Fahrerlaubnisse bzw. Führerscheine': 'Int64'
    })

    return df

def clean_dataframe(df):
    # Replace - and . with NaN
    df = df.replace('-', np.nan)
    df = df.replace('.', np.nan)
    return df
    
def append_to_sqlite_table(df, sqlite_engine, append):
    df.to_sql(name='Fahrerlaubnisse', con=sqlite_engine, 
                    if_exists=append, # so that the script can be run multiple times without error/duplicate data
                    index=False) # don't create extra index column)


def run_kba_pipeline(data, sqlite_engine):
    for i in range(len(data)):
        get_file_from_internet(data[i][0], data[i][1])
        tab_data = load_data_from_excel(data[i][1])
        df = transform_to_df(tab_data)
        df = clean_dataframe(df)
        df = transform_dataframe(df, data[i][3])
        if i == 0:
            # Only remove existing table at the first import
            append_to_sqlite_table(df, sqlite_engine, 'replace')
        else:
            append_to_sqlite_table(df, sqlite_engine, 'append')




data = [
    ('https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Kraftfahrer/FE1/fe1_2023.xlsx?__blob=publicationFile&v=5', 'data/kba_probe_2023.xlsx', 'kba_2023', 2023),
    ('https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Kraftfahrer/FE1/fe1_2022.xlsx?__blob=publicationFile&v=4', 'data/kba_probe_2022.xlsx', 'kba_2022', 2022),
    ('https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Kraftfahrer/FE1/fe1_2021.xlsx?__blob=publicationFile&v=2', 'data/kba_probe_2021.xlsx', 'kba_2021', 2021),
    ('https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Kraftfahrer/FE1/fe1_2020.xlsx?__blob=publicationFile&v=2', 'data/kba_probe_2020.xlsx', 'kba_2020', 2020),
    ('https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Kraftfahrer/FE1/fe1_2019_xlsx.xlsx?__blob=publicationFile&v=2', 'data/kba_probe_2019.xlsx', 'kba_2019', 2019),
    ('https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Kraftfahrer/FE1/fe1_2018_xlsx.xlsx?__blob=publicationFile&v=2', 'data/kba_probe_2018.xlsx', 'kba_2018', 2018),
    ('https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Kraftfahrer/FE1/fe1_2017_xlsx.xlsx?__blob=publicationFile&v=2', 'data/kba_probe_2017.xlsx', 'kba_2017', 2017),
]


sqlite_file_engine = sqla.create_engine('sqlite:///data/kba.sqlite')

if __name__ == "__main__":
    run_kba_pipeline(data, sqlite_file_engine)
