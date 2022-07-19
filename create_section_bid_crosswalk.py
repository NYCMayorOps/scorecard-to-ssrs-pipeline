import pandas as pd
#this will be offline.
#from .connector import Connector
from connector import Connector
connector = Connector()
lm = connector.linear_miles
bid = connector.bids
bid['SECTION'] = bid['BidIdentifier'].apply(lambda x: x.split('_')[-2])
bid.drop('BidIdentifier', axis=1, inplace=True)
df = pd.merge(bid, lm, how='right', on='SECTION')
df.reset_index(drop=True, inplace=True)
df.drop_duplicates(inplace=True)
df.reset_index(drop=True, inplace=True)
df.to_csv('bid_section_crosswalk.csv')
df.drop(['LINEAR_MILES', 'SECTION'], axis=1, inplace=True)
df.drop_duplicates(inplace=True)
df.reset_index(drop=True, inplace=True)
df.to_csv('bids_distinct.csv')
