from tempfile import NamedTemporaryFile
from typing import Dict, List
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from lib.helper import write_dict_list_to_csv


class TestWriteDictListToCsv:
    """Tests the `write_dict_list_to_csv function`."""

    @staticmethod
    def create_test_data() -> List[Dict]:
        return [
            {"name": "Alice", "age": 25, "location": "New York"},
            {"name": "Bob", "age": 30, "location": "San Francisco"},
            {"name": "Charlie", "age": 35, "location": "Boston"},
        ]

    def test_output_file(self) -> None:
        """Tests that the output file is created."""
        data = self.create_test_data()

        with NamedTemporaryFile(mode="w", delete=False) as f:
            filename = f.name

            write_dict_list_to_csv(data, filename)

            df = pd.read_csv(filename)
            assert len(df) == len(data)

    def test_output_content(self) -> None:
        """Tests that the output content is correct."""
        data = self.create_test_data()

        with NamedTemporaryFile(mode="w", delete=False) as f:
            filename = f.name

            write_dict_list_to_csv(data, filename)

            df = pd.read_csv(filename)
            for i, row in df.iterrows():
                for key in data[i]:
                    assert row[key] == data[i][key]


class TestHelperFunctions(unittest.TestCase):
    def test_convert_utc_timestamp_to_datetime_string(self):
        self.assertEqual(
            convert_utc_timestamp_to_datetime_string(1679147878.0),
            "Sunday, March 19, 2023, at 8:11:18 PM"
        )
        self.assertEqual(
            convert_utc_timestamp_to_datetime_string(1640995200.0),
            "Saturday, January 01, 2022, at 12:00:00 AM"
        )
        self.assertEqual(
            convert_utc_timestamp_to_datetime_string(1662057600.0),
            "Saturday, September 03, 2022, at 12:00:00 AM"
        )


class TestHelper(unittest.TestCase):
    @patch("lib.helper.api")
    def test_get_author_name_from_author_id(self, mock_api):
        # Test case where author exists
        mock_redditor = MagicMock()
        mock_redditor.name = "test_author"
        mock_api.redditor.return_value = mock_redditor

        author_id = "test_author_id"
        author_name = get_author_name_from_author_id(author_id)

        mock_api.redditor.assert_called_once_with(author_id)
        self.assertEqual(author_name, "test_author")

        # Test case where author does not exist
        mock_api.redditor.side_effect = Exception("Author not found")

        author_name = get_author_name_from_author_id(author_id)

        mock_api.redditor.assert_called_with(author_id)
        self.assertEqual(author_name, "")


if __name__ == '__main__':
    unittest.main()
