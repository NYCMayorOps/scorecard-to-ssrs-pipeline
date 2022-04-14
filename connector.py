import pandas as pd
import sqlalchemy as sa

class Connector:
    def __init__():
        self.fd = pd.read_csv('fd_mock_all.csv')    
        self.fd_mock = pd.read_csv('fd_mock_all.csv')
        self.fd_bids_mock = pd.read_csv('fd_bids.csv')
        self.linear_miles= pd.read_csv('linear_miles.csv')
        self.district = pd.read_csv('district.csv')
        self.bid_linear_miles = pd.read_csv('bid_linear_miles.csv')
