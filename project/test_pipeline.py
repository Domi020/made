import sqlalchemy as sqla
from sqlalchemy import inspect
import kba_pipeline
import os
import pandas as pd


class TestKbaPipeline:

    # Complete pipeline run for kba pipeline
    def test_complete_kba_pipeline(self):
        # arrange
        data = [('https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Kraftfahrer/FE1/fe1_2023.xlsx?__blob=publicationFile&v=5', 'data/kba_probe_2023.xlsx', 'kba_2023', 2023)]
        sqlite_file = sqla.create_engine('sqlite:///data/kba_test.sqlite')

        # test
        kba_pipeline.run_kba_pipeline(data, sqlite_file)

        # assert
        assert os.path.isfile('data/kba_test.sqlite')

        kba_inspector = inspect(sqlite_file)
        tables = kba_inspector.get_table_names()
        assert 'Fahrerlaubnisse' in tables

        df = pd.read_sql(sql='Fahrerlaubnisse', con=sqlite_file)
        assert not df.values.__contains__('.')
        assert not df.values.__contains__('-')

        con = sqlite_file.connect()
        md = sqla.MetaData()
        table = sqla.Table('Fahrerlaubnisse', md, autoload=True, autoload_with=con)
        col_names = [col.name for col in list(table.c)]
        col_types = [col.type.python_type for col in list(table.c)]
        assert col_names == ['Alter', 'A1', 'A2', 'A', 'B', 'B96, BE', 'C1, C1E', 'C, CE', 'D1, D1E', 'D, DE', 'Zusammen', 'Fahrerlaubnisse bzw. Führerscheine', 'Jahr']
        assert col_types == [str, int, int, int, int, int, int, int, int, int, int, int, int]


    @classmethod
    def teardown_class(cls):
        try:
            os.remove('data/kba_test.sqlite')
        except OSError:
            pass