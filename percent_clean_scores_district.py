import percent_clean_scores_section as pcss
import pandas as pd
import numpy as np

fd = pd.read_csv('fulcrum_data.csv')

def percent_clean_scores_district(fd, yyyy, mm):
    #find this month
    tm = pcss.scorecard_sections(fd, yyyy, mm, True)
    #find one year ago
    oya = pcss.scorecard_sections(fd, int(yyyy - 1), mm, True)
    #find last three months
    l3m = pcss.scorecard_sections(fd, yyyy, mm, False)
    #find last three months one year ago
    oyal3m = pcss.scorecard_sections(fd, int(yyyy - 1), mm, False)
    #group by district
    tmg = group_by_district(tm)
    oyag = group_by_district(oya)
    l3mg = group_by_district(l3m)
    oyal3mg = group_by_district(oyal3m)
    #calculate percent changes.
    


def group_by_district(df):
    df = df[['BOROUGH', 'DISTRICT', 'STREETS_ACCEPTABLE_MILES', 'STREETS_FILTHY_MILES', 'SIDEWALKS_ACCEPTABLE_MILES', 'SIDEWALKS_FILTHY_MILES']]
    df = df.groupby(['BOROUGH', 'DISTRICT']).agg(streets_acceptable_miles=('STREETS_ACCEPTABLE_MILES', np.sum),
                                           streets_filthy_miles=('STREETS_FILTHY_MILES', np.sum),
                                           sidewalks_acceptable_miles=('SIDEWALKS_ACCEPTABLE_MILES', np.sum),
                                           sidewalks_filthy_miles=('SIDEWALKS_FILTHY_MILES', np.sum),                                        
                                        )
    df.reset_index(inplace=True)
    return df