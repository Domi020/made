import genesis_puller
import sys
import pandas as pd
import sqlalchemy as sqla
import numpy as np
import os.path


def remove_first_lines():
    with open('data/accidents.csv') as old, open('data/accidents_improved.csv', 'w') as new:
        lines = old.readlines()
        new.writelines(lines[7:-1])

def remove_unnecessary_columns(df):
    # Removes/slices unnecessary dimensions out of the data cube: 
    # - gender (does not matter for research)
    # - age (only 15-21 "beginners")
    # - only "Hauptverursacher"
    for i in range(4, len(df.columns)):
        if (df[i][0] != 'Insgesamt' or (df[i][1] != '15 bis unter 18 Jahre' and df[i][1] != '18 bis unter 21 Jahre' and df[i][1] != '21 bis unter 25 Jahre' and df[i][1] != '25 bis unter 35 Jahre' and df[i][1] != '35 bis unter 45 Jahre' and df[i][1] != '45 bis unter 55 Jahre')
         or df[i][2] != 'Hauptverursacher des Unfalls'):
                df = df.drop(columns=i)
    return df

def remove_unnecessary_lines(df):
    # Removes unnecessary lines from the data cube:
    # - only "Personenkraftwagen", "Kraftomnibusse", "Güterkraftfahrzeuge" and "Landwirtschaftliche Zugmaschinen" as only those can definitely be driven 17 and over
    # - all kinds of accidents => Insgesamt
    # - all places (innerorts, außerorts...) => Insgesamt
    for i in range(4, len(df.index)):
        if (df.loc[[i]][1].iloc[0] != 'Personenkraftwagen' and df.loc[[i]][1].iloc[0] != 'Güterkraftfahrzeug'
        and df.loc[[i]][1].iloc[0] != 'Landwirtschaftliche Zugmaschine') or df.loc[[i]][2].iloc[0] != 'Insgesamt' or df.loc[[i]][3].iloc[0] != 'Insgesamt':
             df = df.drop(index=i)
    return df
 
def retransform_dataframe(df):
    # Use age as column names
    df = df.rename(columns={0:'year', 1: 'vehicle', 79: '15-17', 81: '18-20', 83: '21-24', 85: '25-34', 87: '35-44', 89: '45-54'})

    # drop "Insgesamt" columns
    df = df.drop(columns=2)
    df = df.drop(columns=3)

    # drop header rows
    df = df.drop(index=0)
    df = df.drop(index=1)
    df = df.drop(index=2)
    df = df.drop(index=3)

    # reset index so it goes 0, 1, 2... again after the deletions
    df = df.reset_index(drop=True)

    # replace - and . with NaN
    df = df.replace('-', np.nan)
    df = df.replace('.', np.nan)

    return df

def change_datatypes(df):
    return df.astype({
       'year': 'Int64',
       'vehicle': str,
       '15-17': 'Int64',
       '18-20': 'Int64',
       '21-24': 'Int64', 
       '25-34': 'Int64',
       '35-44': 'Int64',
       '45-54': 'Int64'
    })

def transform_destatis_dataset(df):
    df = remove_unnecessary_columns(df)
    df = remove_unnecessary_lines(df)
    df = retransform_dataframe(df)
    return change_datatypes(df)
     
def run_destatis_pipeline(sqlite_engine, genesis_user=None, genesis_password=None):
    if genesis_user is not None:
        genesis_puller.get_csv_from_genesis(genesis_user, genesis_password)
    else:
        if not os.path.isfile('data/accidents.csv'):
            print("ERROR: No Destatis Genesis account entered and local accidents.csv does not exist! Aborting pipeline")
            return False
        print("Found local accidents.csv! Continuing...")
    remove_first_lines()
    csv_dataframe = pd.read_csv(filepath_or_buffer='data/accidents_improved.csv', sep=';', header=None)
    df = transform_destatis_dataset(csv_dataframe)
    df.to_sql(name='accidents', con=sqlite_engine, 
                    if_exists='replace', # so that the script can be run multiple times without error/duplicate data
                    index=False) # don't create extra index column
    return True



# MAIN PART

if __name__ == "__main__":
    sqlite_file_engine = sqla.create_engine('sqlite:///data/kba.sqlite')

    if len(sys.argv) != 3:
        print("WARNING: Destatis Genesis account data not entered!")
        print("Usage (for direct python call): python destatis_pipeline.py <genesis_username> <genesis_password>")
        print("Try to transform a local accidents.csv...")
        run_destatis_pipeline(sqlite_file_engine)
    else:
        run_destatis_pipeline(sqlite_file_engine, sys.argv[1], sys.argv[2])
    

    





