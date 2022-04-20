from unittest.mock import patch, Mock
import unittest
import pandas as pd
from percent_clean_scores_section import scorecard_sections
from percent_clean_scores_district import scorecard_districts
from percent_clean_scores_boro import scorecard_boros
from percent_clean_scores_bid import scorecard_bids
from percent_clean_scores_bid_citywide import scorecard_bids_citywide
from connector import Connector


print("############################################################")



class TestClass(unittest.TestCase):

    connector = Connector()
    
    def test_connection(self):
        assert(len(self.connector.fd) > 2)
        assert(len(self.connector.linear_miles) > 2)
        assert(len(self.connector.bid_linear_miles) > 2)
        assert(len(self.connector.district) > 2)
       
    def test_percent_clean_scores_section(self):
        df = self.connector.fd_mock
        if df.empty:
            raise Exception("connector returned empty dataframe")
        actual = scorecard_sections(df, 2021, 11, self.connector, True)
        #actual.to_csv('answer_section.csv')
        expected = pd.read_csv('dd_section_11_2021_no_lm.csv')
        pd.testing.assert_frame_equal(expected, actual)
        print("test percent clean scores section passed")
    
    
    def test_percenct_clean_scores_district(self):
        df = self.connector.fd_mock
        if df.empty:
            raise Exception("connector returned empty dataframe")
        actual = scorecard_districts(df, 2021, 11, self.connector )
        #actual.to_csv('answer_district.csv')
        expected = pd.read_csv('dd_district_2021_11.csv')
        expected['Month'] = expected['Month'].astype('str')
        pd.testing.assert_frame_equal(expected, actual)
        print("test percent clean scores district passed.")
  
    
    '''
    def test_percent_clean_scores_boro(self):
        df = self.connector.fd_mock
        actual = scorecard_boro(df, 2021, 11, self.connector)
        actual.to_csv('answer_boro.csv')
        expected = pd.read_csv('dd_boro_2021_11.csv')
        pd.testing.assert_frame_equal(expected, actual)
        print("percent clean scores borough passed.")
    '''
    
    def test_percent_clean_scores_bid(self):
        df = self.connector.fd_bids_mock
        if df.empty:
            raise Exception("connector returned empty dataframe")
        actual = scorecard_bids(df, 2022, 1, self.connector)
        actual = actual.sort_values('bid_name')
        actual = actual.reset_index(drop=True)
        expected = pd.read_csv('dd_bid_2022Q1.csv').sort_values('bid_name')
        expected = expected.reset_index(drop=True)
        pd.testing.assert_frame_equal(expected, actual)
        print("test_percent_clean_scores_bid passed")
    
    def test_percent_clean_scores_bid_citywide(self):
        df = self.connector.fd_bids_mock
        actual = scorecard_bids_citywide(df, 2022, 1, self.connector)
        expected = pd.read_csv('dd_bid_citywide_2022Q1.csv')
        pd.testing.assert_frame_equal(expected, actual)
        print("test_percent_clean_scores_bid_citywide passed")
    
if __name__ == "__main__":
    unittest.main()