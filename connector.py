import pandas as pd
import sqlalchemy as sa

class Connector:
    fd = pd.read_csv('fd_mock_all.csv')    
    fd_mock = pd.read_csv('fd_mock_all.csv')
    fd_bids_mock = pd.read_csv('fd_bids.csv')
    linear_miles= pd.read_csv('linear_miles.csv')
    district = pd.read_csv('district.csv')
    bid_linear_miles = pd.read_csv('bid_linear_miles.csv')


