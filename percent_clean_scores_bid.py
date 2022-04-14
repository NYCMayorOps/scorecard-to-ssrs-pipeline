import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import percent_clean_scores_section as pcss
import numpy as np
from connector import Connector

connector = Connector()

def scorecard_bids(fd, yyyy, quarter):
    #print(f"fulcrum data info:")
    #print(fd.info())
    #find this month
    mm = None
    print("tm")
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
    a = merge_linear_miles(this_agg)
    a = pcss.rating_calculation(a)
    return cleanup(a, yyyy, quarter)
#for multimonth
nullif = lambda x: x if x > 0 else None

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
        fd.at[index, 'quarter'] = str(row['currentyear']) + month_to_quarter[row['currentmonth']]
    fd['bid_id'] = fd['bid_identifier'].apply(lambda x: x.split('_')[-1])
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
    return this_agg

def merge_linear_miles(this_agg):  
    if len(this_agg.index) == 0:
        return this_agg
    #add linear miles
    lm = connector.bid_linear_miles
    this_agg = this_agg.merge(lm, how='right', on='bid_id' )
    #you have to get rid of linear miles on null or zero street counts because linear miles are aggregated for the calculation.
    #section is the lowest level of aggregation with linear miles. It is the correct level to filter linear miles.
    this_agg_copy = this_agg.copy()
    for index, row in this_agg_copy.iterrows():
        if nullif(row['st_count_rated']) is None:
            this_agg.at[index, 'linear_miles'] = None
    this_agg.to_csv('bid_agg.csv')
    return this_agg

'''
,bid_id,
quarter,
st_rate_avg,
st_count,
st_count_accept,
st_count_filthy,
st_count_rated,
sw_rate_avg,
sw_count,
sw_count_accept,
sw_count_filthy,
sw_count_rated,
bid_human_name,
linear_miles
'''
'''
def final_format(a):
    null_answer = pd.DataFrame(columns=['bid_name',
                                        #'quarter',
                                        #'street_rating_avg',
                                        #'streets_cnt',
                                        #'streets_acceptable_cnt',
                                        'streets_acceptable_miles',
                                        #'streets_filthy_cnt',
                                        'streets_filthy_miles',
                                        #'sidewalk_rating_avg',
                                        #'sidewalks_cnt',
                                        #'sidewalks_acceptable_cnt',
                                        'sidewalks_acceptable_miles',
                                        #'sidewalks_filthy_cnt',
                                        'sidewalks_filthy_miles',
                                        'linear_miles',
                                        'percent_acceptably_clean_streets',
                                        'percent_acceptably_clean_sidewalks'
                                        ]
                                )

    if len(a.index) == 0:
        return null_answer
    df = pd.DataFrame()
    df['BOROUGH'] = a.Borough
    df['DISTRICT'] = a.District
    df['SECTION'] = a.section_no
    #the month will be determined by query not realitiy
    df['MONTH'] = a['month']
    df['STREET_RATING_AVG'] = a.st_rate_avg.round(3)
    df['STREETS_CNT'] = a.st_count
    df['STREETS_ACCEPTABLE_CNT'] = a.streets_acceptable_cnt
    df['STREETS_ACCEPTABLE_MILES'] = a.streets_acceptable_miles
    df['STREETS_FILTHY_MILES'] = a.streets_filthy_miles
    df['STREETS_FILTHY_CNT'] = a.streets_filthy_cnt
    df['SIDEWALKS_RATING_AVG'] = a.sw_rate_avg.round(3)
    df['SIDEWALKS_CNT'] = a.sw_count
    df['SIDEWALKS_ACCEPTABLE_CNT'] = a.sidewalks_acceptable_cnt
    df['SIDEWALKS_ACCEPTABLE_MILES'] = a.sidewalks_acceptable_miles
    df['SIDEWALKS_FILTHY_CNT'] = a.sidewalks_filthy_cnt
    df['SIDEWALKS_FILTHY_MILES'] = a.sidewalks_filthy_miles
    df['LINEAR_MILES'] = a.linear_miles
    #SECTION,MONTH,STREET_RATING_AVG,STREETS_CNT,STREETS_ACCEPTABLE_CNT,STREETS_ACCEPTABLE_MILES,STREETS_FILTHY_MILES,STREETS_FILTHY_CNT,SIDEWALK_RATING_AVG,SIDEWALKS_CNT,SIDEWALKS_ACCEPTABLE_CNT,SIDEWALKS_ACCEPTABLE_MILES,SIDEWALKS_FILTHY_CNT,SIDEWALKS_FILTHY_MILES,LINEAR_MILES,BULK_STREET_RATING_AVG,BULK_SIDEWALK_RATING_AVG,BULK_STREETS_CNT,BULK_SIDEWALKS_CNT
    #df.to_csv("answer.csv")
    return df
'''    
def cleanup(a, yyyy, quarter):
    df = pd.DataFrame()
    df['bid_name'] = a.bid_human_name
    df['quarter'] = str(yyyy) + 'Q' + str(quarter)
    df['street_rating_avg'] = a.street_rating_average 
    df['streets_cnt'] = a.st_count
    df['streets_acceptable_cnt'] = a.st_count_accept
    df['streets_acceptable_miles'] = a.streets_acceptable_miles
    df['streets_filthy_cnt'] = a.streets_filthy_cnt
    df['streets_filthy_miles'] = a.streets_filthy_miles
    df['sidewalk_rating_avg'] = a.sidewalk_rating_avg
    df['sidewalks_cnt'] = a.sw_count
    df['sidewalks_acceptable_cnt'] = a.sidewalks_acceptable_cnt 
    df['sidewalks_acceptable_miles'] = a.sidewalks_acceptable_miles
    df['sidewalks_filthy_cnt'] = a.sidewalks_filthy_cnt
    df['sidewalks_filthy_miles'] = a.sidewalks_filthy_miles
    df['linear_miles'] = a.linear_miles
    df['percent_acceptably_clean_streets'] = round(a.streets_acceptable_miles / a.linear_miles, 3)
    df['percent_acceptably_clean_sidewalks'] = round(a.sidewalks_acceptable_miles / a.linear_miles, 3)
    return df                                        