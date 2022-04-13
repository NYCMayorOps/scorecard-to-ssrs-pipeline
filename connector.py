import pandas as pd

class Connector:
    fd = pd.read_csv('fd_mock_all.csv')
    
    fd_mock = pd.read_csv('fd_mock_all.csv')

    linear_miles= pd.read_csv('linear_miles.csv')
    
    bids=pd.read_csv('bids.csv')
    
    bid_section_crosswalk = pd.read_csv('bid_section_crosswalk.csv')

    bids_distinct = pd.read_csv('bids_distinct.csv')
    
    district = pd.read_csv('district.csv')


