import urllib.request
import openpyxl
import pandas as pd
import sqlalchemy as sqla

def get_file_from_internet(online_file, local_file):
    urllib.request.urlretrieve(online_file, local_file)

def load_data_from_excel(file_name):
    wb = openpyxl.load_workbook(filename=file_name)
    table = wb['FE1.2']
    return table['C41':'M53']

def transform_to_df(data):
    df = pd.DataFrame(data)
    return df.applymap(lambda x: x.value)

def save_to_sqlite(df, table_name):
    sqlite_file_engine = sqla.create_engine('sqlite:///data/kba.sqlite')
    df.to_sql(name=table_name, con=sqlite_file_engine, 
                    if_exists='replace', # so that the script can be run multiple times without error/duplicate data
                    index=False) # don't create extra index column


data = [
    ('https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Kraftfahrer/FE1/fe1_2023.xlsx?__blob=publicationFile&v=5', 'data/kba_probe_2023.xlsx', 'kba_2023'),
    ('https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Kraftfahrer/FE1/fe1_2022.xlsx?__blob=publicationFile&v=4', 'data/kba_probe_2022.xlsx', 'kba_2022'),
    ('https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Kraftfahrer/FE1/fe1_2021.xlsx?__blob=publicationFile&v=2', 'data/kba_probe_2021.xlsx', 'kba_2021'),
    ('https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Kraftfahrer/FE1/fe1_2020.xlsx?__blob=publicationFile&v=2', 'data/kba_probe_2020.xlsx', 'kba_2020'),
    ('https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Kraftfahrer/FE1/fe1_2019_xlsx.xlsx?__blob=publicationFile&v=2', 'data/kba_probe_2019.xlsx', 'kba_2019'),
    ('https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Kraftfahrer/FE1/fe1_2018_xlsx.xlsx?__blob=publicationFile&v=2', 'data/kba_probe_2018.xlsx', 'kba_2018'),
    ('https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Kraftfahrer/FE1/fe1_2017_xlsx.xlsx?__blob=publicationFile&v=2', 'data/kba_probe_2017.xlsx', 'kba_2017'),
]

for i in range(len(data)):
    get_file_from_internet(data[i][0], data[i][1])
    tab_data = load_data_from_excel(data[i][1])
    df = transform_to_df(tab_data)
    save_to_sqlite(df, data[i][2])


