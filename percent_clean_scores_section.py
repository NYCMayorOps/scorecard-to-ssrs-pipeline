
from calendar import month
from itertools import count
import this
import pandas as pd
import numpy as np

from pandas import testing

pd.options.mode.chained_assignment = None
def scout_v2_fulcrum_export_cpr(df, yyyy, mm):

    fd = df #fd for fulcrum data, same as sql
    #print(fd.info())
    fd['my_date'] = pd.to_datetime(fd['_updated_at'], format='%Y-%m')
    pad_month = (lambda x : ('0' + str(x)) if len(str(int(x))) == 1 else str(x))
    #current month is an int #print(type(fd['currentmonth'][0]))
    print(set(fd['currentmonth']))
    print(set(fd['currentyear']))
    #select by this month result is 100 rows in test.
    fd = fd.loc[(fd['my_date'] == f'{yyyy}-{mm}') & (fd['currentmonth'] == mm) & (fd['currentyear'] == yyyy)]
    fd['st_mean'] = None
    fd['sw_mean'] = None
    fd['st_acceptable'] = None
    fd['sw_acceptable'] = None
    fd['st_filthy'] = None
    fd['sw_filthy'] = None
    fd['st_rated'] = None
    fd['sw_rated'] = None
    
    null_if_five = (lambda x: None if x == 5.0 else x)
    #fd[['street_1', 'street_2', 'street_3', 'street_4', 'sidewalk_1', 'sidewalk_2', 'sidewalk_3', 'sidewalk_4']] = fd[['street_1', 'street_2', 'street_3', 'street_4', 'sidewalk_1', 'sidewalk_2', 'sidewalk_3', 'sidewalk_4']].apply(null_if_0)
    fd['street_1'].apply(null_if_five)
    fd['street_2'].apply(null_if_five)
    fd['street_3'].apply(null_if_five)
    fd['street_4'].apply(null_if_five)
    fd['sidewalk_1'].apply(null_if_five) 
    fd['sidewalk_2'].apply(null_if_five) 
    fd['sidewalk_3'].apply(null_if_five) 
    fd['sidewalk_4'].apply(null_if_five)
    

    def mean_calc(one, two, three, four):
        denominator = None
        if (one or two or three or four) == False: 
            return None
        else: 
            #cannot sum none types
            count_me = sum([1 if x is not None else 0 for x in [one, two, three, four]])
        return ((one + two + three + four) / count_me )

    fd_copy = fd.copy()
    none_if_na_else_1 = lambda x: None if pd.isna(x) else 1

    for index, row in fd_copy.iterrows():
        street_mean =  mean_calc(row['street_1'], row['street_2'], row['street_3'], row['street_4'])
        sidewalk_mean =  mean_calc(row['street_1'], row['street_2'], row['street_3'], row['street_4'])
        acceptable_lambda = lambda x : 1 if x < 1.5 else 0
        filthy_lambda = lambda x : 1 if x >=1.75 else 0
        st_acceptable = acceptable_lambda(street_mean)
        st_filthy = filthy_lambda(street_mean)
        sw_acceptable = acceptable_lambda(sidewalk_mean)
        sw_filthy = filthy_lambda(sidewalk_mean)
        #rated_lambda = lambda x: 0 if x is None or x is np.nan else 1
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
   

    this_average = fd.groupby(['currentmonth', 'currentyear','section_no', 'district_no','borough']).agg(st_rate_avg=('st_mean', np.mean),
                                                                st_count=('st_acceptable', np.sum),
                                                                st_count_accept=('st_acceptable', np.sum),
                                                                st_count_filthy=('st_filthy', np.sum),
                                                                st_count_rated=('st_rated', np.sum),
                                                                sw_rate_avg=('sw_mean', np.mean),
                                                                sw_count=('sw_acceptable', np.sum),
                                                                sw_count_accept=('sw_acceptable', np.sum),
                                                                sw_count_filthy=('sw_filthy', np.sum),
                                                                sw_count_rated=('sw_rated', np.sum),                                               
                                                                )
    #fd.to_csv('output.csv')
    this_average.reset_index(inplace=True)
     #add linear miles
    lm = pd.read_csv('linear_miles.csv')
    lm['linear_miles'] = lm['LINEAR_MILES']
    lm['section_no'] = lm['SECTION']
    print(lm.info())
    print(this_average.info())
    this_average.to_csv('this_average.csv')
    this_average = this_average.merge(lm, how='right', on='section_no' )
  
    a = this_average
    #print(a.info())
    #a['month'] = a['currentyear'] * 100 + a['currentmonth']
    a['street_rating_average'] = round(a['st_rate_avg'], 3)
    a['streets_cnt'] = a['st_count_rated'].apply(none_if_na_else_1)
    nullif = lambda x: None if x is 0 else x
    #in sql, you would check if the st_count_rated was null by dividing count rated by count rated.
    #if count_rated is None, none times accept = none. 
    #lambda nullif turns 0 to None. 
    a['streets_acceptable_cnt'] = (nullif(a['st_count_rated']) / a['st_count_rated'] * a['st_count_accept'])
    a['streets_acceptable_miles'] = round(a.st_count_accept / a.st_count_rated *  a.linear_miles , 3)
    a['streets_filthy_miles'] = round(a.st_count_filthy / a.st_count_rated *  a.linear_miles , 3)
    a['streets_filthy_cnt'] = nullif(a['st_count_rated']) / a['st_count_rated'] * a['st_count_filthy']
    a['sidewalk_rating_avg'] = nullif(round(a.sw_rate_avg, 3))
    a['sidewalks_cnt'] = a['sw_count_rated'].apply(none_if_na_else_1)
    a['sidewalks_acceptable_cnt'] = (nullif(a['sw_count_rated']) / a['sw_count_rated'] * a['sw_count_accept'])
    a['sidewalks_acceptable_miles'] = round(a.sw_count_accept / a.sw_count_rated *  a.linear_miles , 3)
    a['sidewalks_filthy_miles'] = round(a.sw_count_filthy / a.sw_count_rated *  a.linear_miles , 3)
    a['sidewalks_filthy_cnt'] =  nullif(a['sw_count_rated']) / a['sw_count_rated'] * a['sw_count_filthy']
    a['linear_miles'] = round(a['linear_miles'])
    #need to left join district.
    d = pd.read_csv('district.csv')
    d['district_no'] = d['District']
    a = a.merge(d, how='left', on='district_no' )
    #need to add month back after merge.
    month_zero = (a['currentyear'] * 100 + a['currentmonth']).fillna('0')
    month_zero_int = month_zero.astype(int)
    #a['month_int'] = a['month_float'].map(lambda x: None if (x == 'None' or x is np.nan) else int(x))
    #a['month_int'] = a['month_int'].map(lambda x: None if x < 0.001 else x)   
    a['month'] = month_zero_int.map(lambda x: None if x == 0 else x)
    


    df = pd.DataFrame()
    #df['BOROUGH'] = a.Borough
    #df['District'] = a.District
    df['SECTION'] = a.section_no
    #the month will be determined by query not realitiy
    df['MONTH'] = a['month']
    df['STREET_RATING_AVG'] = a.st_rate_avg
    df['STREETS_CNT'] = a.streets_cnt
    df['STREETS_ACCEPTABLE_CNT'] = a.streets_acceptable_cnt
    df['STREETS_ACCEPTABLE_MILES'] = a.streets_acceptable_miles
    df['STREETS_FILTHY_MILES'] = a.streets_filthy_miles
    df['STREETS_FILTHY_CNT'] = a.streets_filthy_cnt
    df['SIDEWALK_RATING_AVG'] = a.sw_rate_avg
    df['SIDEWALKS_CNT'] = a.sidewalks_cnt
    df['SIDEWALKS_ACCEPTABLE_CNT'] = a.sidewalks_acceptable_cnt
    df['SIDEWALKS_ACCEPTABLE_MILES'] = a.sidewalks_acceptable_miles
    df['SIDEWALKS_FILTHY_CNT'] = a.sidewalks_filthy_cnt
    df['SIDEWALKS_FILTHY_MILES'] = a.sidewalks_filthy_miles
    df['LINEAR_MILES'] = a.linear_miles
    #SECTION,MONTH,STREET_RATING_AVG,STREETS_CNT,STREETS_ACCEPTABLE_CNT,STREETS_ACCEPTABLE_MILES,STREETS_FILTHY_MILES,STREETS_FILTHY_CNT,SIDEWALK_RATING_AVG,SIDEWALKS_CNT,SIDEWALKS_ACCEPTABLE_CNT,SIDEWALKS_ACCEPTABLE_MILES,SIDEWALKS_FILTHY_CNT,SIDEWALKS_FILTHY_MILES,LINEAR_MILES,BULK_STREET_RATING_AVG,BULK_SIDEWALK_RATING_AVG,BULK_STREETS_CNT,BULK_SIDEWALKS_CNT

    
    df.to_csv("answer.csv")
    return df
    
