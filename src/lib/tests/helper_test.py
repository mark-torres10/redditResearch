from tempfile import NamedTemporaryFile

import pandas as pd

from lib.helper import write_dict_list_to_csv

class TestWriteDictListToCsv:
    """Tests the `write_dict_list_to_csv function`."""
    @staticmethod
    def create_test_data():
        return [
            {'name': 'Alice', 'age': 25, 'location': 'New York'},
            {'name': 'Bob', 'age': 30, 'location': 'San Francisco'},
            {'name': 'Charlie', 'age': 35, 'location': 'Boston'}
        ]

    def test_output_file(self):
        """Tests that the output file is created."""
        data = self.create_test_data()

        with NamedTemporaryFile(mode='w', delete=False) as f:
            filename = f.name

            write_dict_list_to_csv(data, filename)

            df = pd.read_csv(filename)
            assert len(df) == len(data)

    def test_output_content(self):
        """Tests that the output content is correct."""
        data = self.create_test_data()

        with NamedTemporaryFile(mode='w', delete=False) as f:
            filename = f.name

            write_dict_list_to_csv(data, filename)

            df = pd.read_csv(filename)
            for i, row in df.iterrows():
                for key in data[i]:
                    assert row[key] == data[i][key]