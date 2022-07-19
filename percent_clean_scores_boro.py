from . import percent_clean_scores_section as pcss
import pandas as pd
import numpy as np
from .connector import Connector
from .precision import Precision

pad_month= lambda x: str(x) if (len(str(int(x))) == 2) else '0' + str(int(x))

def scorecard_boros(fd, yyyy, mm, connector):
    #find this month
    tm = pcss.scorecard_sections(fd, yyyy, mm, connector, True)
    #find one year ago
    oya = pcss.scorecard_sections(fd, int(yyyy - 1), mm, connector, True)
    #find last three months
    l3m = pcss.scorecard_sections(fd, yyyy, mm, connector, False)
    #find last three months one year ago
    oyal3m = pcss.scorecard_sections(fd, int(yyyy - 1), mm, connector, False)
    #group by district
    #print(oya.info())
    tmg = group_by_boro(tm)
    oyag = group_by_boro(oya)
    l3mg = group_by_boro_3_month(l3m)
    oyal3mg = group_by_boro_3_month(oyal3m)
    #calculate percent changes.
    big_combine = boro_df_combine(tmg, oyag, l3mg, oyal3mg)
    dclean =  boro_cleanup(big_combine, yyyy, mm)
    #not needed. All boros will always be present so no need to right join to a list of boroughs.
    #dclean =  pd.merge(dclean, district, how='right', on='District' )
    dclean['Month'] = dclean['Month'].apply(lambda x: str(yyyy) + pad_month(mm))
    #dclean['Borough'] = dclean['Borough_y']
    #dclean.drop(['Borough_x', 'Borough_y'], axis=1, inplace=True)
    #dclean.insert(0, 'Borough', dclean.pop('Borough'))

    dclean = dclean.reset_index(drop=True)
    #dclean.to_csv('answer_boro.csv')
    return dclean

def scorecard_citywide(fd, yyyy, mm, connector):
    #find this month
    tm = pcss.scorecard_sections(fd, yyyy, mm, connector, True)
    #find one year ago
    oya = pcss.scorecard_sections(fd, int(yyyy - 1), mm, connector, True)
    #find last three months
    l3m = pcss.scorecard_sections(fd, yyyy, mm, connector, False)
    #find last three months one year ago
    oyal3m = pcss.scorecard_sections(fd, int(yyyy - 1), mm, connector, False)
    #group by district
    #print(oya.info())
    tmg = group_by_citywide(tm)
    oyag = group_by_citywide(oya)
    l3mg = group_by_citywide_3_month(l3m)
    oyal3mg = group_by_citywide_3_month(oyal3m)
    #calculate percent changes.
    big_combine = boro_df_combine(tmg, oyag, l3mg, oyal3mg)
    dclean =  boro_cleanup(big_combine, yyyy, mm)
    #not needed. All boros will always be present so no need to right join to a list of boroughs.
    #dclean =  pd.merge(dclean, district, how='right', on='District' )
    dclean['Month'] = dclean['Month'].apply(lambda x: str(yyyy) + pad_month(mm)).astype(str)
    #dclean['Borough'] = dclean['Borough_y']
    #dclean.drop(['Borough_x', 'Borough_y'], axis=1, inplace=True)
    #dclean.insert(0, 'Borough', dclean.pop('Borough'))
 
    #drop the index brutally
    #del dclean[dclean.columns[0]]
    dclean =  dclean.reset_index(drop=True)
    #dclean.to_csv('answer_citywide.csv')
    return dclean

def group_by_boro(df):
    df = df.groupby(['BOROUGH']).agg( street_rating_avg=('STREET_RATING_AVG', np.mean),
                                           streets_cnt=('STREETS_CNT', np.sum),
                                           streets_acceptable_cnt=('STREETS_ACCEPTABLE_CNT', np.sum),
                                           streets_acceptable_miles=('STREETS_ACCEPTABLE_MILES', np.sum),
                                           streets_filthy_cnt=('STREETS_FILTHY_CNT', np.sum),
                                           streets_filthy_miles=('STREETS_FILTHY_MILES', np.sum),
                                           sidewalks_rating_avg=('SIDEWALKS_RATING_AVG', np.mean),
                                           sidewalks_cnt=('SIDEWALKS_CNT', np.sum),
                                           sidewalks_acceptable_cnt=('SIDEWALKS_ACCEPTABLE_CNT', np.sum),
                                           sidewalks_acceptable_miles=('SIDEWALKS_ACCEPTABLE_MILES', np.sum),
                                           sidewalks_filthy_cnt=('SIDEWALKS_FILTHY_CNT', np.sum),
                                           sidewalks_filthy_miles=('SIDEWALKS_FILTHY_MILES', np.sum),
                                           linear_miles=('LINEAR_MILES', np.sum)                                   
                                        )
    df['cnt'] = df['streets_cnt'].apply(nullif)
    df['sidewalks_cnt'] = df['sidewalks_cnt'].apply(nullif)
    df.reset_index(inplace=True)
    return df

def group_by_boro_3_month(df):
    df = df.groupby(['MONTH', 'BOROUGH']).agg( street_rating_avg=('STREET_RATING_AVG', np.mean),
                                           streets_cnt=('STREETS_CNT', np.sum),
                                           streets_acceptable_cnt=('STREETS_ACCEPTABLE_CNT', np.sum),
                                           streets_acceptable_miles=('STREETS_ACCEPTABLE_MILES', np.sum),
                                           streets_filthy_cnt=('STREETS_FILTHY_CNT', np.sum),
                                           streets_filthy_miles=('STREETS_FILTHY_MILES', np.sum),
                                           sidewalks_rating_avg=('SIDEWALKS_RATING_AVG', np.mean),
                                           sidewalks_cnt=('SIDEWALKS_CNT', np.sum),
                                           sidewalks_acceptable_cnt=('SIDEWALKS_ACCEPTABLE_CNT', np.sum),
                                           sidewalks_acceptable_miles=('SIDEWALKS_ACCEPTABLE_MILES', np.sum),
                                           sidewalks_filthy_cnt=('SIDEWALKS_FILTHY_CNT', np.sum),
                                           sidewalks_filthy_miles=('SIDEWALKS_FILTHY_MILES', np.sum),
                                           linear_miles=('LINEAR_MILES', np.sum)                                   
                                        )
    df['percent_clean_streets_3m_'] = (df.streets_acceptable_miles / df.linear_miles).astype(float)
    df['percent_clean_sidewalks_3m_'] = (df.sidewalks_acceptable_miles / df.linear_miles).astype(float)
    df = df.groupby(['BOROUGH']).agg( street_rating_avg=('street_rating_avg', np.mean),
                                           streets_cnt=('streets_cnt', np.sum),
                                           streets_acceptable_cnt=('streets_acceptable_cnt', np.sum),
                                           streets_acceptable_miles=('streets_acceptable_miles', np.sum),
                                           streets_filthy_cnt=('streets_filthy_cnt', np.sum),
                                           streets_filthy_miles=('streets_filthy_miles', np.sum),
                                           sidewalks_rating_avg=('sidewalks_rating_avg', np.mean),
                                           sidewalks_cnt=('sidewalks_cnt', np.sum),
                                           sidewalks_acceptable_cnt=('sidewalks_acceptable_cnt', np.sum),
                                           sidewalks_acceptable_miles=('sidewalks_acceptable_miles', np.sum),
                                           sidewalks_filthy_cnt=('sidewalks_filthy_cnt', np.sum),
                                           sidewalks_filthy_miles=('sidewalks_filthy_miles', np.sum),
                                           linear_miles=('linear_miles', np.sum),
                                           percent_clean_streets_3m_=('percent_clean_streets_3m_', np.mean),                                   
                                           percent_clean_sidewalks_3m_=('percent_clean_sidewalks_3m_', np.mean)
                                        )
    df['cnt'] = df['streets_cnt'].apply(nullif)
    df['sidewalks_cnt'] = df['sidewalks_cnt'].apply(nullif)
    df.reset_index(inplace=True)
    #df.to_csv("three_month_agg_boro.csv")
    return df



def group_by_citywide(df):
    df['BOROUGH'] = 'New York City'
    df = df.groupby(['BOROUGH']).agg( street_rating_avg=('STREET_RATING_AVG', np.mean),
                                           streets_cnt=('STREETS_CNT', np.sum),
                                           streets_acceptable_cnt=('STREETS_ACCEPTABLE_CNT', np.sum),
                                           streets_acceptable_miles=('STREETS_ACCEPTABLE_MILES', np.sum),
                                           streets_filthy_cnt=('STREETS_FILTHY_CNT', np.sum),
                                           streets_filthy_miles=('STREETS_FILTHY_MILES', np.sum),
                                           sidewalks_rating_avg=('SIDEWALKS_RATING_AVG', np.mean),
                                           sidewalks_cnt=('SIDEWALKS_CNT', np.sum),
                                           sidewalks_acceptable_cnt=('SIDEWALKS_ACCEPTABLE_CNT', np.sum),
                                           sidewalks_acceptable_miles=('SIDEWALKS_ACCEPTABLE_MILES', np.sum),
                                           sidewalks_filthy_cnt=('SIDEWALKS_FILTHY_CNT', np.sum),
                                           sidewalks_filthy_miles=('SIDEWALKS_FILTHY_MILES', np.sum),
                                           linear_miles=('LINEAR_MILES', np.sum)                                   
                                        )
    df['cnt'] = df['streets_cnt'].apply(nullif)
    df['sidewalks_cnt'] = df['sidewalks_cnt'].apply(nullif)
    df.reset_index(inplace=True)
    return df

def group_by_citywide_3_month(df):
    df['BOROUGH'] = 'New York City'
    df = df.groupby(['BOROUGH', 'MONTH']).agg( street_rating_avg=('STREET_RATING_AVG', np.mean),
                                           streets_cnt=('STREETS_CNT', np.sum),
                                           streets_acceptable_cnt=('STREETS_ACCEPTABLE_CNT', np.sum),
                                           streets_acceptable_miles=('STREETS_ACCEPTABLE_MILES', np.sum),
                                           streets_filthy_cnt=('STREETS_FILTHY_CNT', np.sum),
                                           streets_filthy_miles=('STREETS_FILTHY_MILES', np.sum),
                                           sidewalks_rating_avg=('SIDEWALKS_RATING_AVG', np.mean),
                                           sidewalks_cnt=('SIDEWALKS_CNT', np.sum),
                                           sidewalks_acceptable_cnt=('SIDEWALKS_ACCEPTABLE_CNT', np.sum),
                                           sidewalks_acceptable_miles=('SIDEWALKS_ACCEPTABLE_MILES', np.sum),
                                           sidewalks_filthy_cnt=('SIDEWALKS_FILTHY_CNT', np.sum),
                                           sidewalks_filthy_miles=('SIDEWALKS_FILTHY_MILES', np.sum),
                                           linear_miles=('LINEAR_MILES', np.sum)                                   
                                        )
    
    df['percent_clean_streets_3m_'] = (df.streets_acceptable_miles / df.linear_miles).astype(float)
    df['percent_clean_sidewalks_3m_'] = (df.sidewalks_acceptable_miles / df.linear_miles).astype(float)
    #print(df.head())
    #df.to_csv('citywide_agg_1.csv')
    df = df.groupby(['BOROUGH']).agg( street_rating_avg=('street_rating_avg', np.mean),
                                           streets_cnt=('streets_cnt', np.sum),
                                           streets_acceptable_cnt=('streets_acceptable_cnt', np.sum),
                                           streets_acceptable_miles=('streets_acceptable_miles', np.sum),
                                           streets_filthy_cnt=('streets_filthy_cnt', np.sum),
                                           streets_filthy_miles=('streets_filthy_miles', np.sum),
                                           sidewalks_rating_avg=('sidewalks_rating_avg', np.mean),
                                           sidewalks_cnt=('sidewalks_cnt', np.sum),
                                           sidewalks_acceptable_cnt=('sidewalks_acceptable_cnt', np.sum),
                                           sidewalks_acceptable_miles=('sidewalks_acceptable_miles', np.sum),
                                           sidewalks_filthy_cnt=('sidewalks_filthy_cnt', np.sum),
                                           sidewalks_filthy_miles=('sidewalks_filthy_miles', np.sum),
                                           linear_miles=('linear_miles', np.sum),
                                           percent_clean_streets_3m_=('percent_clean_streets_3m_', np.mean),                                   
                                           percent_clean_sidewalks_3m_=('percent_clean_sidewalks_3m_', np.mean)
                                        )
    #print(df.head())
    #df.to_csv('citywide_agg_2.csv')
    df['cnt'] = df['streets_cnt'].apply(nullif)
    df['sidewalks_cnt'] = df['sidewalks_cnt'].apply(nullif)
    df.reset_index(inplace=True)
    return df

def boro_df_combine(tmg, oyag, l3mg, oyal3mg):
    df = tmg
    df = pd.merge(df, oyag, how="left", on="BOROUGH", suffixes=['tmg', 'oyag'])
    df = pd.merge(df, l3mg, how="left", on="BOROUGH", suffixes=['oyag2', 'l3mg'])
    df = pd.merge(df, oyal3mg, how="left", on="BOROUGH", suffixes=['l3mg2', 'oyal3m'])
    return df

'''
 #   Column                            Non-Null Count  Dtype
---  ------                            --------------  -----
 0   BOROUGHtmg                        4 non-null      object
 1   DISTRICT                          4 non-null      object
 2   street_rating_avgtmg              3 non-null      float64
 3   streets_cnttmg                    4 non-null      float64
 4   streets_acceptable_cnttmg         4 non-null      float64
 5   streets_acceptable_milestmg       4 non-null      float64
 6   streets_filthy_cnttmg             4 non-null      float64
 7   streets_filthy_milestmg           4 non-null      float64
 8   sidewalks_rating_avgtmg           3 non-null      float64
 9   sidewalks_cnttmg                  4 non-null      float64
 10  sidewalks_acceptable_cnttmg       4 non-null      float64
 11  sidewalks_acceptable_milestmg     4 non-null      float64
 12  sidewalks_filthy_cnttmg           4 non-null      float64
 13  sidewalks_filthy_milestmg         4 non-null      float64
 14  linear_milestmg                   4 non-null      float64
 15  BOROUGHoyag                       0 non-null      object
 16  street_rating_avgoyag             0 non-null      object
 17  streets_cntoyag                   0 non-null      object
 18  streets_acceptable_cntoyag        0 non-null      object
 19  streets_acceptable_milesoyag      0 non-null      object
 20  streets_filthy_cntoyag            0 non-null      object
 21  streets_filthy_milesoyag          0 non-null      object
 22  sidewalks_rating_avgoyag          0 non-null      object
 23  sidewalks_cntoyag                 0 non-null      object
 24  sidewalks_acceptable_cntoyag      0 non-null      object
 25  sidewalks_acceptable_milesoyag    0 non-null      object
 26  sidewalks_filthy_cntoyag          0 non-null      object
 27  sidewalks_filthy_milesoyag        0 non-null      object
 28  linear_milesoyag                  0 non-null      object
 29  BOROUGHl3mg2                      4 non-null      object
 30  street_rating_avgl3mg2            4 non-null      float64
 31  streets_cntl3mg2                  4 non-null      float64
 32  streets_acceptable_cntl3mg2       4 non-null      float64
 33  streets_acceptable_milesl3mg2     4 non-null      float64
 34  streets_filthy_cntl3mg2           4 non-null      float64
 35  streets_filthy_milesl3mg2         4 non-null      float64
 36  sidewalks_rating_avgl3mg2         4 non-null      float64
 37  sidewalks_cntl3mg2                4 non-null      float64
 38  sidewalks_acceptable_cntl3mg2     4 non-null      float64
 39  sidewalks_acceptable_milesl3mg2   4 non-null      float64
 40  sidewalks_filthy_cntl3mg2         4 non-null      float64
 41  sidewalks_filthy_milesl3mg2       4 non-null      float64
 42  linear_milesl3mg2                 4 non-null      float64
 43  BOROUGHoyal3m                     4 non-null      object
 44  street_rating_avgoyal3m           4 non-null      float64
 45  streets_cntoyal3m                 4 non-null      float64
 46  streets_acceptable_cntoyal3m      4 non-null      float64
 47  streets_acceptable_milesoyal3m    4 non-null      float64
 48  streets_filthy_cntoyal3m          4 non-null      float64
 49  streets_filthy_milesoyal3m        4 non-null      float64
 50  sidewalks_rating_avgoyal3m        4 non-null      float64
 51  sidewalks_cntoyal3m               4 non-null      float64
 52  sidewalks_acceptable_cntoyal3m    4 non-null      float64
 53  sidewalks_acceptable_milesoyal3m  4 non-null      float64
 54  sidewalks_filthy_cntoyal3m        4 non-null      float64
 55  sidewalks_filthy_milesoyal3m      4 non-null      float64
 56  linear_milesoyal3m                4 non-null      float64'''

nullif = lambda x: x if x > 0 else np.nan 
    
def boro_cleanup(big_df, yyyy, mm):
    #create a filter that returns None if streets_cnt is null (or zero, which became null after aggregation)
    #this won't work aggregated. Need to do it before aggregation
    percent_round = Precision().percent_round
    not_percent_round = Precision().not_percent_round
    answer = pd.DataFrame()
    answer['Borough'] = big_df.BOROUGH

    answer['Month'] =  str(yyyy) + pad_month(mm)
    answer['PercentAcceptablyCleanStreets'] = 	((big_df.streets_acceptable_milestmg / big_df.linear_milestmg)).astype('float')
    answer['PercentFilthyStreets']	 = ((big_df.streets_filthy_milestmg  / big_df.linear_milestmg)).astype('float')  #linear miles is never null for any section or district and does not change
    answer['PercentAcceptablyCleanSidewalks'] = ((big_df.sidewalks_acceptable_milestmg / big_df.linear_milestmg)).astype('float')
    answer['PercentFilthySidewalks'] = ((big_df.sidewalks_filthy_milestmg  / big_df.linear_milestmg)).astype('float')
    oya_acceptable_streets = (big_df.streets_acceptable_milesoyag / big_df.linear_milesoyag)
    oya_acceptable_sidewalks =  (big_df.sidewalks_acceptable_milesoyag / big_df.linear_milesoyag)
    #change in percent, not percent change.
    answer['ChangeInPercentCleanStreetsYearly'] = (answer.PercentAcceptablyCleanStreets - oya_acceptable_streets).astype('float')   # (final - initial) / initial
    answer['ChangeInPercentCleanSidewalksYearly'] = (answer.PercentAcceptablyCleanSidewalks - oya_acceptable_sidewalks).astype('float')
    
    ###### can't calculate percent clean at the end. ######
    ###### Have to calculate percent clean each month and aggregate the percentages as per MMR. #####
    ### answer['ThreeMonthAveragePercentCleanStreets']	= (big_df.streets_acceptable_milesl3mg2 / big_df.linear_milesl3mg2).astype('float')
    ### answer['ThreeMonthAveragePercentCleanSidewalks'] = (big_df.sidewalks_acceptable_milesl3mg2 / big_df.linear_milesl3mg2).astype('float')
    ### oyal3m_acceptable_streets = (big_df.streets_acceptable_milesoyal3m / big_df.linear_milesoyal3m)
    ### oyal3m_acceptable_sidewalks =  (big_df.sidewalks_acceptable_milesoyal3m / big_df.linear_milesoyal3m)
    answer['ThreeMonthAveragePercentCleanStreets'] = big_df.percent_clean_streets_3m_l3mg2
    answer['ThreeMonthAveragePercentCleanSidewalks'] = big_df.percent_clean_sidewalks_3m_l3mg2
    oyal3m_acceptable_streets = big_df.percent_clean_streets_3m_oyal3m
    oyal3m_acceptable_sidewalks = big_df.percent_clean_sidewalks_3m_oyal3m
    answer['ChangeIn3MonthAverageCleanStreets'] = (answer.ThreeMonthAveragePercentCleanStreets - oyal3m_acceptable_streets).astype('float')
    answer['ChangeIn3MonthAverageCleanSidewalks'] = (answer.ThreeMonthAveragePercentCleanSidewalks - oyal3m_acceptable_sidewalks).astype('float')
    #print(f"threeMonthAverage%CleanStreets: {answer.ThreeMonthAveragePercentCleanStreets} \n oyal3m_acceptable_streets: {oyal3m_acceptable_streets} answer= {answer.ThreeMonthAveragePercentCleanStreets - oyal3m_acceptable_streets}")
    
    answer['PercentAcceptablyCleanStreets'] =answer['PercentAcceptablyCleanStreets'].apply(percent_round)
    answer['PercentFilthyStreets']	 = answer['PercentFilthyStreets'].apply(percent_round)
    answer['PercentAcceptablyCleanSidewalks'] = answer['PercentAcceptablyCleanSidewalks'].apply(percent_round)
    answer['PercentFilthySidewalks'] = answer['PercentFilthySidewalks'].apply(percent_round)
    answer['ChangeInPercentCleanStreetsYearly'] = answer['ChangeInPercentCleanStreetsYearly'].apply(percent_round)
    answer['ChangeInPercentCleanSidewalksYearly'] = answer['ChangeInPercentCleanSidewalksYearly'].apply(percent_round)
    answer['ThreeMonthAveragePercentCleanStreets']	= answer['ThreeMonthAveragePercentCleanStreets'].apply(percent_round)
    answer['ThreeMonthAveragePercentCleanSidewalks'] = answer['ThreeMonthAveragePercentCleanSidewalks'].apply(percent_round)
    answer['ChangeIn3MonthAverageCleanStreets'] = answer['ChangeIn3MonthAverageCleanStreets'].apply(percent_round)
    answer['ChangeIn3MonthAverageCleanSidewalks'] = answer['ChangeIn3MonthAverageCleanSidewalks'].apply(percent_round)
    
    #answer.to_csv('boro_answer.csv')
    return answer
