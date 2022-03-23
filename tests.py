from unittest.mock import patch, Mock
import unittest
import pandas as pd
from percent_clean_scores_section import scout_v2_fulcrum_export_cpr



print("############################################################")



class TestClass(unittest.TestCase):
    def test_percent_clean_scores_section(var):
        df = pd.read_csv('fulcrum_data.csv')
        actual = scout_v2_fulcrum_export_cpr(df, 2021, 11)
        expected = pd.read_csv('dd_section_11_2021.csv')
        expected['MONTH'] = expected['MONTH']
        pd.testing.assert_frame_equal(expected, actual)
        print("test percent clean scores section passed")

if __name__ == "__main__":
    unittest.main()