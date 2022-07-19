import pandas as pd
from percent_clean_scores_bid import scorecard_bids
import numpy as np
from precision import Precision



def scorecard_bids_citywide(fd, yyyy, quarter, connector):
    my_round = Precision.my_round
    df = scorecard_bids(fd, yyyy, quarter, connector)
    df['bid_id'] = 'Citywide'
    this_agg = df.groupby(['bid_id', 'quarter']).agg(st_acceptable_miles=('streets_acceptable_miles', np.sum),
                                                     sw_acceptable_miles=('sidewalks_acceptable_miles', np.sum),
                                                     linear_miles=('linear_miles', np.sum),
                                                    )
    this_agg.reset_index(inplace=True)                                                   
    answer = pd.DataFrame()
    answer['quarter'] = this_agg['quarter']
    answer['percent_acceptably_clean_streets'] = my_round((this_agg.st_acceptable_miles * 100)/ this_agg.linear_miles)
    answer['percent_acceptably_clean_sidewalks'] =  my_round((this_agg.sw_acceptable_miles * 100)/ this_agg.linear_miles)
    return answer