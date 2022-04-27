import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import percent_clean_scores_section as pcss
import numpy as np
import logging

#bids only recorded after 11-2021
#


def scorecard_bids(fd, yyyy, quarter, connector):
    #print(f"fulcrum data info:")
    #print(fd.info())
    #find this month
    mm = np.nan
    if quarter == 1:
        mm = 3
    elif quarter == 2:
        mm = 6
    elif quarter == 3:
        mm = 9
    elif quarter == 4:
        mm = 12
    else:
        raise Exception("not a valid quarter")
    #from sections
    fd = pcss.load_fulcrum_data(fd, yyyy, mm, is_one_month = False)
    this_agg = aggregate(fd)
    a = merge_linear_miles(this_agg, connector)
    a = pcss.rating_calculation(a)
    return cleanup(a, yyyy, quarter)
#for multimonth
nullif = lambda x: x if x > 0 else np.nan

def aggregate(fd):
    #create quarter
    fd['quarter'] = None
    fd_copy = fd.copy()
    month_to_quarter = {
        1 : 'Q1',
        2 : 'Q1',
        3 : 'Q1',
        4 : 'Q2',
        5 : 'Q2',
        6 : 'Q2',
        7 : 'Q3',
        8 : 'Q3',
        9 : 'Q3',
        10 : 'Q4',
        11 : 'Q4',
        12 : 'Q4'
    }
    for index, row in fd_copy.iterrows():
        fd.at[index, 'quarter'] = str(int(row['currentyear'])) + month_to_quarter[row['currentmonth']]
    #print(f"bid identifier: {set(fd['bid_identifier'])}")
    fd['bid_id'] = fd['bid_identifier'].apply(lambda x: x.split('_')[-1] if x is not None else '')
    #print(f"bid id: {set(fd['bid_id'])}")
    groupby_list = ['bid_id', 'quarter']
    this_agg = fd.groupby(groupby_list).agg(st_rate_avg=('st_mean', np.mean),
                                                                st_count=('st_rated', np.sum),
                                                                st_count_accept=('st_acceptable', np.sum),
                                                                st_count_filthy=('st_filthy', np.sum),
                                                                st_count_rated=('st_rated', np.sum),
                                                                sw_rate_avg=('sw_mean', np.mean),
                                                                sw_count=('sw_rated', np.sum),
                                                                sw_count_accept=('sw_acceptable', np.sum),
                                                                sw_count_filthy=('sw_filthy', np.sum),
                                                                sw_count_rated=('sw_rated', np.sum),                                               
                                                                )
    this_agg.reset_index(inplace=True)
    this_agg['st_count'] = this_agg['st_count'].apply(nullif)
    this_agg['st_count_rated'] = this_agg['st_count_rated'].apply(nullif)
    this_agg['sw_count'] = this_agg['sw_count'].apply(nullif)
    this_agg['sw_count_rated'] = this_agg['sw_count_rated'].apply(nullif)
    this_agg.to_csv('this_agg_bid.csv')
    return this_agg

def merge_linear_miles(this_agg, connector):  
    if len(this_agg.index) == 0:
        logging.warn("where is this_agg?")
        #return this_agg
    #add linear miles
    lm = connector.bid_linear_miles
    assert(lm.empty == False)
    this_agg = this_agg.merge(lm, how='right', on='bid_id' )
    #you have to get rid of linear miles on null or zero street counts because linear miles are aggregated for the calculation.
    #we use linear miles on a bid basis, not the section basis.
    this_agg_copy = this_agg.copy()
    for index, row in this_agg_copy.iterrows():
        if nullif(row['st_count_rated']) is np.nan:
            this_agg.at[index, 'linear_miles'] = np.nan
    this_agg.to_csv('bid_agg.csv')
    return this_agg

def cleanup(a, yyyy, quarter):
    df = pd.DataFrame()
    df['bid_name'] = a.bid_human_name
    df['quarter'] = str(yyyy) + 'Q' + str(quarter)
    df['street_rating_avg'] = a.street_rating_average.astype(float).round(3) 
    df['streets_cnt'] = a.st_count
    df['streets_acceptable_cnt'] = a.st_count_accept
    df['streets_acceptable_miles'] = a.streets_acceptable_miles.astype(float).round(3)
    df['streets_filthy_cnt'] = a.streets_filthy_cnt
    df['streets_filthy_miles'] = a.streets_filthy_miles.astype(float).round(3)
    df['sidewalk_rating_avg'] = a.sidewalk_rating_avg.astype(float).round(3)
    df['sidewalks_cnt'] = a.sw_count
    df['sidewalks_acceptable_cnt'] = a.sidewalks_acceptable_cnt 
    df['sidewalks_acceptable_miles'] = a.sidewalks_acceptable_miles.astype(float).round(3)
    df['sidewalks_filthy_cnt'] = a.sidewalks_filthy_cnt
    df['sidewalks_filthy_miles'] = a.sidewalks_filthy_miles.astype(float).round(3)
    df['linear_miles'] = a.linear_miles.astype(float).round(3)
    df['percent_acceptably_clean_streets'] = (a.streets_acceptable_miles / a.linear_miles).astype(float).round(3)
    df['percent_acceptably_clean_sidewalks'] = (a.sidewalks_acceptable_miles / a.linear_miles).astype(float).round(3)
    return df                                        