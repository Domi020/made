import sqlalchemy as sqla
from sqlalchemy import inspect
import kba_pipeline
import os
import pandas as pd
import pandas.testing


class TestKbaPipeline:

    @classmethod
    def setup_class(cls):
        cls.single_data = [('https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Kraftfahrer/FE1/fe1_2023.xlsx?__blob=publicationFile&v=5', 'data/kba_probe_2023.xlsx', 'kba_2023', 2023)]
        cls.complete_data = [
            ('https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Kraftfahrer/FE1/fe1_2023.xlsx?__blob=publicationFile&v=5', 'data/kba_probe_2023.xlsx', 'kba_2023', 2023),
            ('https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Kraftfahrer/FE1/fe1_2022.xlsx?__blob=publicationFile&v=4', 'data/kba_probe_2022.xlsx', 'kba_2022', 2022),
            ('https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Kraftfahrer/FE1/fe1_2021.xlsx?__blob=publicationFile&v=2', 'data/kba_probe_2021.xlsx', 'kba_2021', 2021),
            ('https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Kraftfahrer/FE1/fe1_2020.xlsx?__blob=publicationFile&v=2', 'data/kba_probe_2020.xlsx', 'kba_2020', 2020),
            ('https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Kraftfahrer/FE1/fe1_2019_xlsx.xlsx?__blob=publicationFile&v=2', 'data/kba_probe_2019.xlsx', 'kba_2019', 2019),
            ('https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Kraftfahrer/FE1/fe1_2018_xlsx.xlsx?__blob=publicationFile&v=2', 'data/kba_probe_2018.xlsx', 'kba_2018', 2018),
            ('https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Kraftfahrer/FE1/fe1_2017_xlsx.xlsx?__blob=publicationFile&v=2', 'data/kba_probe_2017.xlsx', 'kba_2017', 2017),
            ]       
        cls.sqlite_file = sqla.create_engine('sqlite:///data/kba_test.sqlite')

    # Complete pipeline run for kba pipeline
    def test_kba_pipeline_single_file(self):
        # arrange
        data = self.single_data

        # test
        kba_pipeline.run_kba_pipeline(data, self.sqlite_file)

        # assert
        assert os.path.isfile('data/kba_test.sqlite')

        kba_inspector = inspect(self.sqlite_file)
        tables = kba_inspector.get_table_names()
        assert 'Fahrerlaubnisse' in tables

        con = self.sqlite_file.connect()
        row_count_cursor = con.execute('SELECT COUNT(*) FROM Fahrerlaubnisse;')
        assert next(row_count_cursor)[0] == 13
        # with Session(self.sqlite_file) as session:
        #     assert session.query('Fahrerlaubnisse').count() == 13

        md = sqla.MetaData()
        table = sqla.Table('Fahrerlaubnisse', md, autoload=True, autoload_with=con)
        col_names = [col.name for col in list(table.c)]
        col_types = [col.type.python_type for col in list(table.c)]
        assert col_names == ['Alter', 'A1', 'A2', 'A', 'B', 'B96, BE', 'C1, C1E', 'C, CE', 'D1, D1E', 'D, DE', 'Zusammen', 'Fahrerlaubnisse bzw. Führerscheine', 'Jahr']
        assert col_types == [str, int, int, int, int, int, int, int, int, int, int, int, int]
    
    def test_kba_pipeline_complete(self):
        # arrange
        data = self.complete_data

        # test
        kba_pipeline.run_kba_pipeline(data, self.sqlite_file)

        # assert
        assert os.path.isfile('data/kba_test.sqlite')

        kba_inspector = inspect(self.sqlite_file)
        tables = kba_inspector.get_table_names()
        assert 'Fahrerlaubnisse' in tables

        con = self.sqlite_file.connect()
        row_count_cursor = con.execute('SELECT COUNT(*) FROM Fahrerlaubnisse;')
        assert next(row_count_cursor)[0] == (7 * 13)
        


    def test_clean_dataframe(self):
        # arrange
        data = pd.read_pickle('project/test_data/df_to_clean.pkl')

        # test
        result = kba_pipeline.clean_dataframe(data)

        # assert
        assert not result.values.__contains__('.')
        assert not result.values.__contains__('-')


    def test_transform_dataframe(self):
        # arrange
        data = pd.read_pickle('project/test_data/df_to_transform.pkl')

        # test
        result = kba_pipeline.transform_dataframe(data, 2023)

        # assert
        expected = pd.read_pickle('project/test_data/df_transformed.pkl')
        pandas.testing.assert_frame_equal(result, expected)


    @classmethod
    def teardown_class(cls):
        try:
            os.remove('data/kba_test.sqlite')
        except OSError:
            pass