#from . import percent_clean_scores_section as pcss
import percent_clean_scores_section as pcss
import pandas as pd
import numpy as np
#from .connector import Connector
#from .precision import Precision
from connector import Connector
from precision import Precision

pad_month= lambda x: str(x) if (len(str(int(x))) == 2) else '0' + str(int(x))

def scorecard_districts(fd, yyyy, mm, connector):
    #find this month
    print(f"districts: {yyyy}-{mm}")
    tm = pcss.scorecard_sections(fd, yyyy, mm, connector, True)
    #find one year ago
    oya = pcss.scorecard_sections(fd, int(yyyy - 1), mm, connector, True)
    #find last three months
    l3m = pcss.scorecard_sections(fd, yyyy, mm, connector, False)
    #find last three months one year ago
    oyal3m = pcss.scorecard_sections(fd, int(yyyy - 1), mm, connector, False)
    #group by district
    #print(oya.info())
    tmg = group_by_district(tm)
    oyag = group_by_district(oya)
    l3mg = group_by_district_3_month(l3m)
    oyal3mg = group_by_district_3_month(oyal3m)
    #calculate percent changes.
    big_combine = final_district_df_combine(tmg, oyag, l3mg, oyal3mg)
    dclean =  districts_cleanup(big_combine, yyyy, mm)
    #right join to district table from DB to include all the districts. Move to second position.
    dclean =  pd.merge(dclean, connector.district, how='right', on='District' )
    dist_col = dclean.pop('District')
    dclean.insert(loc=0, column='District', value=dist_col)
    dclean['Month'] = dclean['Month'].apply(lambda x: str(yyyy) + pad_month(mm))
    dclean.insert(0, 'Borough', dclean.pop('Borough'))
    del dclean['district_no']
    dclean.reset_index(drop=True, inplace=True)
    #dclean.to_csv('answer_district.csv', index=False)

    
    return dclean


def group_by_district(df):
    df = df.groupby(['BOROUGH', 'DISTRICT']).agg( street_rating_avg=('STREET_RATING_AVG', np.mean),
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

def group_by_district_3_month(df):
    df = df.groupby(['MONTH', 'BOROUGH', 'DISTRICT']).agg( street_rating_avg=('STREET_RATING_AVG', np.mean),
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
    df = df.groupby(['BOROUGH', 'DISTRICT']).agg( street_rating_avg=('street_rating_avg', np.mean),
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
                                           linear_miles=('linear_miles', np.sum)                                   
                                        )
    df['cnt'] = df['streets_cnt'].apply(nullif)
    df['sidewalks_cnt'] = df['sidewalks_cnt'].apply(nullif)
    df.reset_index(inplace=True)
    #df.to_csv("three_month_agg.csv")
    return df


def final_district_df_combine(tmg, oyag, l3mg, oyal3mg):
    df = tmg
    df = pd.merge(df, oyag, how="left", on="DISTRICT", suffixes=['tmg', 'oyag'])
    df = pd.merge(df, l3mg, how="left", on="DISTRICT", suffixes=['oyag2', 'l3mg'])
    df = pd.merge(df, oyal3mg, how="left", on="DISTRICT", suffixes=['l3mg2', 'oyal3m'])
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

nullif = lambda x: np.nan if x <= 0.0000001 else x

def my_round(number, decimals):
    try: 
        return (round(round(number, decimals) * 100, decimals))
    except:
        return None
def districts_cleanup(big_df, yyyy, mm):
    #create a filter that returns None if streets_cnt is null (or zero, which became null after aggregation)
    #this won't work aggregated. Need to do it before aggregation
    #st_cnt_filter = big_df['streets_cnttmg'].apply(lambda x: 1 if nullif(x) is not None else None)
    #sw_cnt_filter = big_df['sidewalks_cnttmg'].apply(lambda x: 1 if nullif(x) is not None else None)   
    st_cnt_filter = 1
    sw_cnt_filter = 1
    my_round = Precision.my_round
    answer = pd.DataFrame()
    answer['Borough'] = big_df.BOROUGHtmg
    answer['District']	= big_df.DISTRICT
    #answe['.']DistrictNo = 	
    answer['Month'] =  str(yyyy) + pad_month(mm)
    answer['PercentAcceptablyCleanStreets'] = 	((big_df.streets_acceptable_milestmg / big_df.linear_milestmg)).astype('float')
    answer['PercentFilthyStreets']	 = ((big_df.streets_filthy_milestmg  / big_df.linear_milestmg)).astype('float')  #linear miles is never null for any section or district and does not change
    answer['PercentAcceptablyCleanSidewalks'] = ((big_df.sidewalks_acceptable_milestmg / big_df.linear_milestmg)).astype('float')
    answer['PercentFilthySidewalks'] = ((big_df.sidewalks_filthy_milestmg  / big_df.linear_milestmg)).astype('float')
    oya_acceptable_streets = (big_df.streets_acceptable_milesoyag / big_df.linear_milesoyag)
    oya_acceptable_sidewalks =  (big_df.sidewalks_acceptable_milesoyag / big_df.linear_milesoyag)
    #change in percent, not percent change.
    answer['ChangeInPercentCleanStreetsYearly'] = (answer.PercentAcceptablyCleanStreets - oya_acceptable_streets).astype('float')   # #(final - initial) / initial	
    answer['ChangeInPercentCleanSidewalksYearly'] = (answer.PercentAcceptablyCleanSidewalks - oya_acceptable_sidewalks).astype('float')
    answer['ThreeMonthAveragePercentCleanStreets']	= (big_df.streets_acceptable_milesl3mg2 / big_df.linear_milesl3mg2).astype('float')
    answer['ThreeMonthAveragePercentCleanSidewalks'] = (big_df.sidewalks_acceptable_milesl3mg2 / big_df.linear_milesl3mg2).astype('float')
    oyal3m_acceptable_streets = (big_df.streets_acceptable_milesoyal3m / big_df.linear_milesoyal3m)
    oyal3m_acceptable_sidewalks =  (big_df.sidewalks_acceptable_milesoyal3m / big_df.linear_milesoyal3m)
    answer['ChangeIn3MonthAverageCleanStreets'] = (answer.ThreeMonthAveragePercentCleanStreets - oyal3m_acceptable_streets).astype('float')
    answer['ChangeIn3MonthAverageCleanSidewalks'] = (answer.ThreeMonthAveragePercentCleanSidewalks - oyal3m_acceptable_sidewalks).astype('float')
    #print(f"threeMonthAverage%CleanStreets: {answer.ThreeMonthAveragePercentCleanStreets} \n oyal3m_acceptable_streets: {oyal3m_acceptable_streets} answer= {answer.ThreeMonthAveragePercentCleanStreets - oyal3m_acceptable_streets}")
    
    answer['PercentAcceptablyCleanStreets'] =answer['PercentAcceptablyCleanStreets'].apply(my_round)
    answer['PercentFilthyStreets']	 = answer['PercentFilthyStreets'].apply(my_round)
    answer['PercentAcceptablyCleanSidewalks'] = answer['PercentAcceptablyCleanSidewalks'].apply(my_round)
    answer['PercentFilthySidewalks'] = answer['PercentFilthySidewalks'].apply(my_round)
    answer['ChangeInPercentCleanStreetsYearly'] = answer['ChangeInPercentCleanStreetsYearly'].apply(my_round)
    answer['ChangeInPercentCleanSidewalksYearly'] = answer['ChangeInPercentCleanSidewalksYearly'].apply(my_round)
    answer['ThreeMonthAveragePercentCleanStreets']	= answer['ThreeMonthAveragePercentCleanStreets'].apply(my_round)
    answer['ThreeMonthAveragePercentCleanSidewalks'] = answer['ThreeMonthAveragePercentCleanSidewalks'].apply(my_round)
    answer['ChangeIn3MonthAverageCleanStreets'] = answer['ChangeIn3MonthAverageCleanStreets'].apply(my_round)
    answer['ChangeIn3MonthAverageCleanSidewalks'] = answer['ChangeIn3MonthAverageCleanSidewalks'].apply(my_round)
    #stray index left over.
    answer.reset_index(inplace=True, drop=True)
    del answer[answer.columns[0]]
    #
    #answer.to_csv('districts_answer.csv')
    return answer