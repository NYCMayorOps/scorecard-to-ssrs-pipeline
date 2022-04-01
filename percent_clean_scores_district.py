import percent_clean_scores_section as pcss
import pandas as pd
import numpy as np

fd = pd.read_csv('fulcrum_data.csv')

def scorecard_districts(fd, yyyy, mm):
    #find this month
    tm = pcss.scorecard_sections(fd, yyyy, mm, True)
    #find one year ago
    oya = pcss.scorecard_sections(fd, int(yyyy - 1), mm, True)
    #find last three months
    l3m = pcss.scorecard_sections(fd, yyyy, mm, False)
    #find last three months one year ago
    oyal3m = pcss.scorecard_sections(fd, int(yyyy - 1), mm, False)
    #group by district
    #print(oya.info())
    tmg = group_by_district(tm)
    oyag = group_by_district(oya)
    l3mg = group_by_district(l3m)
    oyal3mg = group_by_district(oyal3m)
    #calculate percent changes.
    return final_district_df_combine(tmg, oyag, l3mg, oyal3mg)
    


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
    df.reset_index(inplace=True)
    return df

def final_district_df_combine(tmg, oyag, l3mg, oyal3mg):
    df = tmg
    df = pd.merge(df, oyag, how="left", on="DISTRICT")
    df = pd.merge(df, l3mg, how="left", on="DISTRICT")
    df = pd.merge(df, oyal3mg, how="left", on="DISTRICT")
    return df