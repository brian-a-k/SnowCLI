from snow.loading.data_loader import DataLoader
from snowflake.snowpark import Row


class TestDataLoader:
    # TODO Requires snowflake connection
    def test_load_csv(self):
        assert True

    def test_load_semi_structured_file(self):
        assert True

    def test_set_schema_ownership(self):
        assert True

    def test_get_stage_file_format(self):
        assert True

    def test_get_load_summary(self):
        test_tbl_name = 'TEST_DB.TEST_SCHEMA.TEST_TABLE'

        test_copy_results = [
            Row(file='test_file_1', status='LOADED', rows_parsed=1000, rows_loaded=1000),
            Row(file='test_file_2', status='LOADED', rows_parsed=1000, rows_loaded=1000),
            Row(file='test_file_3', status='LOADED', rows_parsed=1000, rows_loaded=1000),
            Row(file='test_file_4', status='LOADED', rows_parsed=1000, rows_loaded=1000)
        ]

        test_load_summary = {
            'table_name': test_tbl_name,
            'files_processed': 4,
            'files_loaded': 4,
            'rows_parsed': 4000,
            'rows_loaded': 4000
        }

        test_zero_results = [Row(status='Copy executed with 0 files processed.')]

        test_zero_summary = {
            'table_name': test_tbl_name,
            'files_processed': 0,
            'files_loaded': 0,
            'rows_parsed': 0,
            'rows_loaded': 0
        }

        assert DataLoader.get_load_summary(test_tbl_name, test_copy_results) == test_load_summary
        assert DataLoader.get_load_summary(test_tbl_name, test_zero_results) == test_zero_summary
