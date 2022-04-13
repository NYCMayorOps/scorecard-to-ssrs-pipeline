from importlib_metadata import Sectioned
import percent_clean_scores_section as pcss
import pandas as pd
import numpy as np



pad_month= lambda x: str(x) if (len(str(int(x))) == 2) else '0' + str(int(x))

def scorecard_bids(fd, yyyy, quarter):
    #find this month
    mm = None
    print("tm")
    if quarter == 1:
        mm = 6
    elif quarter == 2:
        mm = 9
    elif quarter == 3:
        mm = 12
    elif quarter == 4:
        mm = 3
        yyyy = yyyy + 1
    else:
        raise Exception("not a valid quarter")
    print(f"bid yyyy-mm: {yyyy}-{mm}")
    tm = pcss.scorecard_sections(fd, yyyy, mm, True)
    if tm.empty:
        pass
        #raise Exception("scorecard_sections for this month returned empty.")
    #find one year ago
    oya =  pcss.scorecard_sections(fd, int(yyyy - 1), mm, True)
    #find last three months
    print("l3m")
    l3m = pcss.scorecard_sections(fd, yyyy, mm, False)
    if l3m.empty:
        pass
        #raise Exception(f"last three months is empty for {yyyy}-{mm}")
    #print(l3m.info())
    #find last three months one year ago
    print("oyal3m")
    oyal3m = pcss.scorecard_sections(fd, int(yyyy - 1), mm, False)
    #group by district
    #print(oya.info())
    print("l3mg")
    l3mg = group_by_bid(l3m)
    if l3mg.empty:
        pass
        #raise Exception(f"last three months group_by_bid empty for {yyyy}-{mm}")
    print("oyal3mg")
    oyal3mg = group_by_bid(oyal3m)
    #calculate percent changes.
    print("tmg")
    tmg = group_by_bid(tm)
    if tmg.empty:
        pass
        #raise Exception("tmg group by bid returned empty.")
    print("oyag")
    oyag = group_by_bid(oya)



    big_combine = bid_df_combine(tmg, oyag, l3mg, oyal3mg)
    dclean =  bid_cleanup(big_combine, yyyy, mm)
    #not needed. All boros will always be present so no need to right join to a list of boroughs.
    #dclean =  pd.merge(dclean, district, how='right', on='District' )
    dclean['Month'] = dclean['Month'].apply(lambda x: str(yyyy) + pad_month(mm))
    #dclean['Borough'] = dclean['Borough_y']
    #dclean.drop(['Borough_x', 'Borough_y'], axis=1, inplace=True)
    #dclean.insert(0, 'Borough', dclean.pop('Borough'))
    dclean.to_csv('answer_bid.csv')
    return dclean
    
def find_quarter(month):
    test = (str(month)[-2:])
    if test == 'an':
        return None
    else: 
        mm = int(str(month)[-4: -2])
    mm = int(mm)
    if mm >= 4 and mm <= 6:
        return 1
    elif mm >=7 and mm <= 9:
        return 2
    elif mm >= 10 and mm <= 12:
        return 3
    elif mm >= 1 and mm <= 3:
        return 4
    else:
        raise Exception("month is out of bounds for quarter assignment.")

nullif = lambda x: None if x <= 0.0000001 else x

def group_by_bid(df):
    if df.empty:
        pass
        #raise Exception("group_by_bid started off empty")
    df['quarter'] = df['MONTH'].apply( lambda x: find_quarter(x))
    df.drop('LINEAR_MILES', axis=1, inplace=True)
    bids = pd.read_csv('bid_section_crosswalk.csv')
    bids.reset_index(drop=True, inplace=True)
    df = pd.merge(df, bids, on='SECTION', how='inner')
    df.reset_index(drop=True, inplace=True)
    #print(df.info())
    df.to_csv("bid_right_before_group_by.csv")
    df = df.groupby(['BidName', 'BidHumanName', 'quarter']).agg( street_rating_avg=('STREET_RATING_AVG', np.mean),
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
    if df.empty:
        pass
        #raise Exception("groupby resulted in empty dataframe.")
    df['cnt'] = df['streets_cnt'].apply(nullif)
    df['sidewalks_cnt'] = df['sidewalks_cnt'].apply(nullif)
    df.reset_index(inplace=True)
    return df
    



def bid_df_combine(tmg, oyag, l3mg, oyal3mg):
    #need to have all bids present, not just the bids in this month
    bids = pd.read_csv('bids_distinct.csv')
    df = pd.merge(tmg, bids, on='BidName', how='right')
    #print(df.info())
    if df.empty:
        pass
        #raise Exception("bid_df_combine started off with tmg empty")
    df = pd.merge(df, oyag, how="left", on="BidName", suffixes=['tmg', 'oyag'])
    print(f"ltmg:")
    #
    # print(l3mg.info())
    df = pd.merge(df, l3mg, how="left", on="BidName", suffixes=['oyag2', 'l3mg'])
    df = pd.merge(df, oyal3mg, how="left", on="BidName", suffixes=['l3mg2', 'oyal3m'])
    if df.empty:
        raise Exception("bid_df_combine returned an empty dataframe")
    print(f"bid_df_combined:")
    #print(df.info())
    return df

def my_round(number, decimals):
    try: 
        return (round(round(number, decimals) * 100, decimals))
    except:
        return None

    
def bid_cleanup(big_df, yyyy, mm):
    #create a filter that returns None if streets_cnt is null (or zero, which became null after aggregation)
    #this won't work aggregated. Need to do it before aggregation
    #st_cnt_filter = big_df['streets_cnttmg'].apply(lambda x: 1 if nullif(x) is not None else None)
    #sw_cnt_filter = big_df['sidewalks_cnttmg'].apply(lambda x: 1 if nullif(x) is not None else None)   
    lambda_round = lambda x: my_round(x, 3)
    answer = pd.DataFrame()
    answer['BidName'] = big_df.BidHumanName
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
    
    answer['PercentAcceptablyCleanStreets'] =answer['PercentAcceptablyCleanStreets'].apply(lambda_round)
    answer['PercentFilthyStreets']	 = answer['PercentFilthyStreets'].apply(lambda_round)	 
    answer['PercentAcceptablyCleanSidewalks'] = answer['PercentAcceptablyCleanSidewalks'].apply(lambda_round) 
    answer['PercentFilthySidewalks'] = answer['PercentFilthySidewalks'].apply(lambda_round)
    answer['ChangeInPercentCleanStreetsYearly'] = answer['ChangeInPercentCleanStreetsYearly'].apply(lambda_round) 
    answer['ChangeInPercentCleanSidewalksYearly'] = answer['ChangeInPercentCleanSidewalksYearly'].apply(lambda_round) 
    answer['ThreeMonthAveragePercentCleanStreets']	= answer['ThreeMonthAveragePercentCleanStreets'].apply(lambda_round) 	
    answer['ThreeMonthAveragePercentCleanSidewalks'] = answer['ThreeMonthAveragePercentCleanSidewalks'].apply(lambda_round)  
    answer['ChangeIn3MonthAverageCleanStreets'] = answer['ChangeIn3MonthAverageCleanStreets'].apply(lambda_round)  
    answer['ChangeIn3MonthAverageCleanSidewalks'] = answer['ChangeIn3MonthAverageCleanSidewalks'].apply(lambda_round) 
    return answer
