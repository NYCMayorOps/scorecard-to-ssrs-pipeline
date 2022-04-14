from unittest.mock import patch, Mock
import unittest
import pandas as pd
from percent_clean_scores_section import scorecard_sections
from percent_clean_scores_district import scorecard_districts
from percent_clean_scores_boro import scorecard_boros
from percent_clean_scores_bid import scorecard_bids
from connector import Connector

connector = Connector
print("############################################################")



class TestClass(unittest.TestCase):
    def test_percent_clean_scores_section(var):
        df = connector.fd_mock
        actual = scorecard_sections(df, 2021, 11, True)
        #actual.to_csv('answer_section.csv')
        expected = pd.read_csv('dd_section_11_2021_no_lm.csv')
        pd.testing.assert_frame_equal(expected, actual)
        print("test percent clean scores section passed")
    
    def test_percenct_clean_scores_district(var):
        df = connector.fd_mock
        actual = scorecard_districts(df, 2021, 11 )
        #actual.to_csv('answer_district.csv')
        expected = pd.read_csv('dd_district_2021_11.csv')
        expected['Month'] = expected['Month'].astype('str')
        pd.testing.assert_frame_equal(expected, actual)
        print("test percent clean scores district passed.")
        print("")
    
    '''
    def test_percent_clean_scores_boro(var):
        df = connector.fd_mock
        actual = scorecard_boro(df, 2021, 11)
        actual.to_csv('answer_boro.csv')
        expected = pd.read_csv('dd_boro_2021_11.csv')
        pd.testing.assert_frame_equal(expected, actual)
        print("percent clean scores borough passed.")
    '''
    def test_percent_clean_scores_bid(var):
        df = connector.fd_bids_mock
        actual = scorecard_bids(df, 2022, 1)
        actual = actual.sort_values('bid_name')
        actual = actual.reset_index(drop=True)
        expected = pd.read_csv('dd_bid_2022Q1.csv').sort_values('bid_name')
        expected = expected.reset_index(drop=True)
        pd.testing.assert_frame_equal(expected, actual)

if __name__ == "__main__":
    unittest.main()