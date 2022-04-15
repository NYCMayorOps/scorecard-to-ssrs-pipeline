import pandas as pd
from percent_clean_scores_bid import scorecard_bids
import numpy as np

def scorecard_bids_citywide(fd, yyyy, quarter):
    df = scorecard_bids(fd, yyyy, quarter)
    df['bid_id'] = 'Citywide'
    this_agg = df.groupby(['bid_id', 'quarter']).agg(sw_acceptable_miles=('streets_acceptable_miles', np.sum),
                                                     st_acceptable_miles=('sidewalks_acceptable_miles', np.sum),
                                                     linear_miles=('linear_miles', np.sum),
                                                    )
    this_agg.reset_index(inplace=True)                                                    
    answer = pd.DataFrame()
    answer['quarter'] = this_agg['quarter']
    answer['percent_acceptably_clean_streets'] = round((this_agg.st_acceptable_miles / this_agg.linear_miles), 3) * 100
    answer['percent_acceptably_clean_sidewalks'] =  round((this_agg.sw_acceptable_miles / this_agg.linear_miles), 3) * 100
    return answer