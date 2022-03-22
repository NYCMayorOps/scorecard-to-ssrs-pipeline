from unittest.mock import patch, Mock
import pandas as pd
from percent_clean_scores_section import scout_v2_fulcrum_export_cpr

def test_percent_clean_scores_section():
    df = pd.read_csv('fulcrum_data.csv')
    print(df['section_no'])
    actual = scout_v2_fulcrum_export_cpr(df, 2021, 11)
    expected = pd.read_csv('dd_section_11_2021.csv')
    expected['MONTH'] = expected['MONTH']
    pd.testing.assert_frame_equal(actual, expected)

test_percent_clean_scores_section()