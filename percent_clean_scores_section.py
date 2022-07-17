
from calendar import month
from itertools import count
import this
import pandas as pd
import numpy as np

from pandas import testing
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import logging
from precision import Precision

precision = Precision().precision
#pd.options.mode.chained_assignment = 'warn'

#formerly know as scout_v2_fulcrum_export_cpr in SQL
def scorecard_sections(fd, yyyy, mm, connector, is_one_month=True,):
    #print(f"fulcrum data info:")
    #print(fd.info())
    fd = load_fulcrum_data(fd, yyyy, mm, is_one_month, connector)
    this_agg = aggregate(fd)
    a = merge_linear_miles(this_agg, connector)
    a = rating_calculation(a)
    a = merge_district(a, connector)
    answer = final_format(a)
    return answer
#for multimonth


def mean_calc(one, two, three, four):
    zero_if_null = lambda x: 0 if pd.isna(x) else x
    count_me = sum([1 if pd.isna(x) == False else 0 for x in [one, two, three, four]])
    if count_me == 0:
        return np.nan
    return ((zero_if_null(one) + zero_if_null(two) + zero_if_null(three) + zero_if_null(four)) / count_me )


    
def load_fulcrum_data(fd, yyyy, mm, is_one_month, connector, end_year=None, end_month=None):
    #fd for fulcrum data, same as sql
    #print(fd.info())
    #### before we do anything, lets make sure the _updated_date is the same as the created_date
    
    #have to truncate the day and time from my_date for equality comparison with other datetimes (yyyy-mm).
    fd['my_date'] = fd['my_date2'].apply(lambda x: datetime.strptime(datetime.strftime(x, '%Y-%m'), '%Y-%m'))
    this_month = f"{yyyy}-{mm}"
    this_month_dt = datetime.strptime(this_month, '%Y-%m')
    next_month = this_month_dt + relativedelta(months=1)
    this_date = next_month - relativedelta(days=1)
    #print(f"this date: {this_date}")
    start_date = datetime.strptime(f'{yyyy}-{mm}', '%Y-%m')
    if (end_year and end_month):
        end_date = datetime.strptime(f'{end_year}-{end_month}', '%Y-%m')

    #select by this month result is 100 rows in test.
    if (is_one_month):
        #aren't these and statements the same? Sometimes the edit date is not the current month.
        fd = fd.loc[((fd['my_date'] == start_date) & (fd['currentmonth'] == mm) & (fd['currentyear'] == yyyy))]
    #is_one_month is false. Looking for three months.
    elif (end_year == None and end_month == None):
        #if not one month, than this month and the previous two months,making three months.
        #staring with the last day of the month, three months ago is actuall the first day of two months ago. 
        #so September 1st to November 31st
        three_months_ago_date = datetime.strptime(f'{yyyy}-{mm}', '%Y-%m') - relativedelta(months=2)
        #print(f"three months ago date: {three_months_ago_date}")
        fd = fd.loc[((fd['my_date'] >= three_months_ago_date) & (fd['my_date'] <= this_date))]
    elif (end_year is not None and end_month is not None):
        fd = fd.loc[((fd['my_date'] >= start_date) & (fd['my_date'] <= end_date))]
    else: 
        raise Exception("the parameters for load_fulcrum_data are wrong. did you specify end year and end month?")
    #print(f"current months: {set(fd['currentmonth'])}, current years: {set(fd['currentyear'])}")
    fd = fd.copy()
    fd['st_mean'] = None
    fd['sw_mean'] = None
    fd['st_acceptable'] = None
    fd['sw_acceptable'] = None
    fd['st_filthy'] = None
    fd['sw_filthy'] = None
    fd['st_rated'] = None
    fd['sw_rated'] = None
    
    null_if_five = (lambda x: None if x > 4.99999 else x)
    #fd[['street_1', 'street_2', 'street_3', 'street_4', 'sidewalk_1', 'sidewalk_2', 'sidewalk_3', 'sidewalk_4']] = fd[['street_1', 'street_2', 'street_3', 'street_4', 'sidewalk_1', 'sidewalk_2', 'sidewalk_3', 'sidewalk_4']].apply(null_if_0)
    fd['street_1'] = fd['street_1'].apply(null_if_five)
    fd['street_2'] = fd['street_2'].apply(null_if_five)
    fd['street_3'] = fd['street_3'].apply(null_if_five)
    fd['street_4'] = fd['street_4'].apply(null_if_five)
    fd['sidewalk_1'] = fd['sidewalk_1'].apply(null_if_five) 
    fd['sidewalk_2'] = fd['sidewalk_2'].apply(null_if_five) 
    fd['sidewalk_3'] = fd['sidewalk_3'].apply(null_if_five) 
    fd['sidewalk_4'] = fd['sidewalk_4'].apply(null_if_five)
    fd_copy = fd.copy()

    for index, row in fd_copy.iterrows():
        street_mean =  mean_calc(row['street_1'], row['street_2'], row['street_3'], row['street_4'])
        sidewalk_mean =  mean_calc(row['sidewalk_1'], row['sidewalk_2'], row['sidewalk_3'], row['sidewalk_4'])
        acceptable_lambda = lambda x : None if pd.isna(x) else (1 if x < 1.5 else 0)
        filthy_lambda = lambda x : None if pd.isna(x) else (1 if x >=1.75 else 0)
        st_acceptable = acceptable_lambda(street_mean)
        st_filthy = filthy_lambda(street_mean)
        sw_acceptable = acceptable_lambda(sidewalk_mean)
        sw_filthy = filthy_lambda(sidewalk_mean)
        none_if_na_else_1 = lambda x: None if pd.isna(x) else 1
        st_rated = none_if_na_else_1(street_mean)
        sw_rated = none_if_na_else_1(sidewalk_mean)



        #assign to original table
        fd.at[index, "st_mean"] = street_mean
        fd.at[index, "sw_mean"] = sidewalk_mean
        fd.at[index, "st_acceptable"] = st_acceptable
        fd.at[index, "sw_acceptable"] = sw_acceptable
        fd.at[index, "st_filthy"] = st_filthy
        fd.at[index, "sw_filthy"] = sw_filthy
        fd.at[index, "st_rated"] = st_rated
        fd.at[index, "sw_rated"] = sw_rated

        #https://stackoverflow.com/questions/32751229/pandas-sum-by-groupby-but-exclude-certain-columns
        #df.groupby(['Code', 'Country', 'Item_Code', 'Item', 'Ele_Code', 'Unit']).agg({'Y1961': np.sum, 'Y1962': np.sum, 'Y1963': np.sum})
    #print(fd.info())
    #df.agg(x=('A', max), y=('B', 'min'), z=('C', np.mean))
   
    #fd.to_csv('fd_pre_aggregate.csv')
    return fd

#nullif lambda function (global scope)
nullif = lambda x: x if x > 0 else np.nan

def aggregate(fd):   
    groupby_list = ['currentmonth', 'currentyear','section_no', 'district_no','borough']
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

def merge_linear_miles(this_agg, connector):  
    if len(this_agg.index) == 0:
        pass
        #return this_agg
    #add linear miles
    lm = connector.linear_miles
    lm['linear_miles'] = lm['LINEAR_MILES'].astype(float)
    lm['section_no'] = lm['SECTION']
    this_agg = this_agg.merge(lm, how='right', on='section_no' )
    #you have to get rid of linear miles on null or zero street counts because linear miles are aggregated for the calculation.
    #section is the lowest level of aggregation with linear miles. It is the correct level to filter linear miles.
    this_agg_copy = this_agg.copy()
    for index, row in this_agg_copy.iterrows():
        if nullif(row['st_count_rated']) is np.nan:
            this_agg.at[index, 'LINEAR_MILES'] = np.nan
            this_agg.at[index, 'linear_miles'] = np.nan
    #this_agg.to_csv('this_agg.csv')
    return this_agg

def rating_calculation(a):
    if len(a.index) == 0:
        logging.warn('where is input for rating calculation?')
    a['street_rating_average'] = a['st_rate_avg'].astype(float).round(precision)
    nullif = lambda x: x if x > 0 else np.nan 
    #in sql, you would check if the st_count_rated was null by dividing count rated by count rated.
    #if count_rated is None, none times accept = none. 
    #lambda nullif turns 0 to None.
    a.st_count_rated = a.st_count_rated.apply(nullif) 
    a.sw_count_rated = a.sw_count_rated.apply(nullif)
    a.linear_miles = a.linear_miles.astype(float)

    a['streets_acceptable_cnt'] = (((a.st_count_rated) / a.st_count_rated) * a.st_count_accept)
    a['streets_acceptable_miles'] = a.st_count_accept / (a.st_count_rated) *  a.linear_miles
    a['streets_filthy_miles'] = ((a.st_count_filthy / (a.st_count_rated)) *  a.linear_miles)
    a['streets_filthy_cnt'] = ((a.st_count_rated) / a.st_count_rated) * a.st_count_filthy
    a['sidewalk_rating_avg'] = ((a.sw_rate_avg))
    #a['sidewalks_cnt'] = a['sw_count_rated'].apply(none_if_na_else_1)
    a['sidewalks_acceptable_cnt'] = ((a.sw_count_rated) / a.sw_count_rated) * a.sw_count_accept #1 if rated is greater than 0
    a['sidewalks_acceptable_miles'] = ((a.sw_count_accept / (a.sw_count_rated)) *  a.linear_miles)
    a['sidewalks_filthy_miles'] = ((a.sw_count_filthy / (a.sw_count_rated)) *  a.linear_miles)
    a['sidewalks_filthy_cnt'] =  ((a.sw_count_rated) / a.sw_count_rated) * a.sw_count_filthy #one if sw_count > 0
    a['linear_miles'] = ((a.st_count_rated) / a.st_count_rated) * (a.linear_miles)
    #a.to_csv('rating_calculation.csv')
    return a

def merge_district(a, connector):
    if len(a.index) == 0:
        return a
    #need to left join district.
    d = connector.district
    d['district_no'] = d['District']
    a = a.merge(d, how='left', on='district_no' )
    #need to add month back after merge.
    month_zero = (a['currentyear'] * 100 + a['currentmonth']).fillna('0')
    month_zero_int = month_zero.astype(int)
    a['month'] = month_zero_int.map(lambda x: None if x == 0 else x)
    #a.to_csv('merge_districts.csv')
    return a

lambda_int = lambda x: int(x) if (type(x) == float or type(x) == int) and np.isnan(x) == False else np.nan



def final_format(a):
    precision = Precision().precision
    null_answer = pd.DataFrame(columns=['BOROUGH', 
                            'DISTRICT', 
                            'SECTION', 
                            'MONTH', 
                            'STREET_RATING_AVG', 
                            'STREETS_CNT', 
                            'STREETS_ACCEPTABLE_CNT',
                            'STREETS_ACCEPTABLE_MILES',
                            'STREETS_FILTHY_CNT',
                            'STREETS_FILTHY_MILES',
                            'SIDEWALKS_RATING_AVG',
                            'SIDEWALKS_CNT',
                            'SIDEWALKS_ACCEPTABLE_CNT',
                            'SIDEWALKS_ACCEPTABLE_MILES',
                            'SIDEWALKS_FILTHY_CNT',
                            'SIDEWALKS_FILTHY_MILES',
                            'LINEAR_MILES'])
                            
    if len(a.index) == 0:
        return null_answer
    df = pd.DataFrame()
    df['BOROUGH'] = a.Borough
    df['DISTRICT'] = a.District
    df['SECTION'] = a.section_no
    #the month will be determined by query not realitiy
    df['MONTH'] = a['month']
    df['STREET_RATING_AVG'] = a.st_rate_avg.astype(float).round(precision)
    df['STREETS_CNT'] = a.st_count.apply(lambda_int)
    df['STREETS_ACCEPTABLE_CNT'] = a.streets_acceptable_cnt.apply(lambda_int)
    df['STREETS_ACCEPTABLE_MILES'] = a.streets_acceptable_miles.astype(float).round(precision)
    df['STREETS_FILTHY_MILES'] = a.streets_filthy_miles.astype(float).round(precision)
    df['STREETS_FILTHY_CNT'] = a.streets_filthy_cnt.apply(lambda_int)
    df['SIDEWALKS_RATING_AVG'] = a.sw_rate_avg.astype(float).round(precision)
    df['SIDEWALKS_CNT'] = a.sw_count.apply(lambda_int)
    df['SIDEWALKS_ACCEPTABLE_CNT'] = a.sidewalks_acceptable_cnt.apply(lambda_int)
    df['SIDEWALKS_ACCEPTABLE_MILES'] = a.sidewalks_acceptable_miles.astype(float).round(precision)
    df['SIDEWALKS_FILTHY_CNT'] = a.sidewalks_filthy_cnt.apply(lambda_int)
    df['SIDEWALKS_FILTHY_MILES'] = a.sidewalks_filthy_miles.astype(float).round(precision)
    df['LINEAR_MILES'] = a.linear_miles.astype(float).round(precision)
    #SECTION,MONTH,STREET_RATING_AVG,STREETS_CNT,STREETS_ACCEPTABLE_CNT,STREETS_ACCEPTABLE_MILES,STREETS_FILTHY_MILES,STREETS_FILTHY_CNT,SIDEWALK_RATING_AVG,SIDEWALKS_CNT,SIDEWALKS_ACCEPTABLE_CNT,SIDEWALKS_ACCEPTABLE_MILES,SIDEWALKS_FILTHY_CNT,SIDEWALKS_FILTHY_MILES,LINEAR_MILES,BULK_STREET_RATING_AVG,BULK_SIDEWALK_RATING_AVG,BULK_STREETS_CNT,BULK_SIDEWALKS_CNT
    #df.to_csv("answer.csv")
    return df
    

