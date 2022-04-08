from unittest.mock import patch, Mock
import unittest
import pandas as pd
from percent_clean_scores_section import scorecard_sections
from percent_clean_scores_district import scorecard_districts
from percent_clean_scores_boro import scorecard_boro

print("############################################################")



class TestClass(unittest.TestCase):
    def test_percent_clean_scores_section(var):
        df = pd.read_csv('fd_mock_all.csv')
        actual = scorecard_sections(df, 2021, 11, True)
        actual.to_csv('answer_section.csv')
        expected = pd.read_csv('dd_section_11_2021_no_lm.csv')
        pd.testing.assert_frame_equal(expected, actual)
        print("test percent clean scores section passed")
    
    def test_percenct_clean_scores_district(var):
        df = pd.read_csv('fd_mock_all.csv')
        actual = scorecard_districts(df, 2021, 11 )
        actual.to_csv('answer_district.csv')
        expected = pd.read_csv('dd_district_2021_11.csv')
        expected['Month'] = expected['Month'].astype('str')
        pd.testing.assert_frame_equal(expected, actual)
        print("test percent clean scores district passed.")
        print("")
    
    '''
    def test_percent_clean_scores_boro(var):
        df = pd.read_csv('fd_mock_all.csv')
        actual = scorecard_boro(df, 2021, 11)
        actual.to_csv('answer_boro.csv')
        expected = pd.read_csv('dd_boro_2021_11.csv')
        pd.testing.assert_frame_equal(expected, actual)
        print("percent clean scores borough passed.")
    '''
if __name__ == "__main__":
    unittest.main()