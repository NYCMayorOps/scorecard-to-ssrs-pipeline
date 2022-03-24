from unittest.mock import patch, Mock
import unittest
import pandas as pd
import percent_clean_scores_section import scorecard_sections



print("############################################################")



class TestClass(unittest.TestCase):
    def test_percent_clean_scores_section(var):
        df = pd.read_csv('fulcrum_data.csv')
        actual = scorecard_sections(df, 2021, 11, 'section', True)
        expected = pd.read_csv('dd_section_11_2021.csv')
        expected['MONTH'] = expected['MONTH']
        pd.testing.assert_frame_equal(expected, actual)
        print("test percent clean scores section passed")

if __name__ == "__main__":
    unittest.main()