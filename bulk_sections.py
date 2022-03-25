import pandas as pd
import percent_clean_scores_section as pcss
from datetime import datetime

def bulk_sections(fd, start_year, start_month, end_year, end_month):
    fd = pcss.load_fulcrum_data(fd, start_year, start_month, False, end_year, end_month )
    this_agg = pcss.aggregate(fd)
    a = pcss.merge_linear_miles(this_agg)
    a = pcss.rating_calculation(a)
    a = pcss.merge_district(a)
    answer = pcss.final_format(a)
    return answer.sort_values(by=['SECTION', 'MONTH'])


def concat_bds(borough, district, section):
    return str(int(borough)) + str(int(district)) +  str(int(section))
if __name__ == '__main__':
    scorecard1 = pd.read_csv('fd-2019-8-to-2021-10.csv')
    #print(scorecard1['_updated_at'][0])
    scorecard1['currentmonth'] = scorecard1['_updated_at'].map(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f').month)
    scorecard1['currentyear'] =  scorecard1['_updated_at'].map(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f').year)

    scorecard1.borough_no = (scorecard1.borough_no.astype('Int64')).astype('str')
    scorecard1.district_no = (scorecard1.district_no.astype('Int64')).astype('str')
    scorecard1.section_no = (scorecard1.section_no.astype('Int64')).astype('str')
    scorecard1 = scorecard1.assign(bds_join_on = lambda x: x['borough'] + x['district_no'] + x['section_no'])

    crosswalk = pd.read_csv('scorecard_v1_bds_crosswalk.csv')
    crosswalk.borough_no = (crosswalk.borough_no.astype('Int64')).astype('str')
    crosswalk.district_no = (crosswalk.district_no.astype('Int64')).astype('str')
    crosswalk.section_no = (crosswalk.section_no.astype('Int64')).astype('str')
    crosswalk = crosswalk.assign(bds_join_on = lambda x: x['borough'] + x['district_no'] + x['section_no'])
    #print("crosswalk:")
    #print(crosswalk['bds_join_on'][0])
    
   
    scorecard1 = pd.merge(scorecard1, crosswalk, how='inner', on='bds_join_on' )
    #print("scorecard1:")
    #print(scorecard1.info())
    scorecard1_xform = pd.DataFrame()
    #mapping scorecard 1 to scorecard 2.
    scorecard1_xform['_project_id'] =         scorecard1['_project_id'] 
    scorecard1_xform['_assigned_to_id'] =     scorecard1['_assigned_to_id'] 
    scorecard1_xform['_status'] =             None
    scorecard1_xform['_latitude'] =           scorecard1['_latitude'] 
    scorecard1_xform['_longitude'] =          scorecard1['_longitude'] 
    scorecard1_xform['_created_at'] =         scorecard1['_created_at'] 
    scorecard1_xform['_updated_at'] =         scorecard1['_updated_at'] 
    scorecard1_xform['_version'] =            None
    scorecard1_xform['_updated_by_id'] =      scorecard1['_updated_by_id'] 
    scorecard1_xform['_server_created_at'] =  scorecard1['_server_created_at'] 
    scorecard1_xform['_server_updated_at'] =  None 
    scorecard1_xform['_title'] =              scorecard1['blockface'] 
    scorecard1_xform['currentmonth'] =        scorecard1['currentmonth'].astype('Int64') 
    scorecard1_xform['currentyear'] =         scorecard1['currentyear'].astype('Int64')
    scorecard1_xform['stop_number'] =         None
    scorecard1_xform['blockface_title'] =     scorecard1['blockface'] 
    scorecard1_xform['bid_identifier'] =      None 
    scorecard1_xform['bid_algorithm_flag'] =  None
    scorecard1_xform['borough'] =             scorecard1['borough_x'] 
    scorecard1_xform['district_no'] =         scorecard1['district_name'] #these won't match
    scorecard1_xform['section_no'] =          scorecard1['section_name'] 
    scorecard1_xform['blockface'] =           scorecard1['blockface'] 
    scorecard1_xform['street_1'] =            scorecard1['street_1'] 
    scorecard1_xform['sidewalk_1'] =          scorecard1['sidewalk_1'] 
    scorecard1_xform['street_2'] =            scorecard1['street_2'] 
    scorecard1_xform['sidewalk_2'] =          scorecard1['sidewalk_2'] 
    scorecard1_xform['street_3'] =            scorecard1['street_3'] 
    scorecard1_xform['sidewalk_3'] =          scorecard1['sidewalk_3'] 
    scorecard1_xform['street_4'] =            scorecard1['street_4'] 
    scorecard1_xform['sidewalk_4'] =          scorecard1['sidewalk_4'] 
    scorecard1_xform['LogTime'] =             scorecard1['_created_at'] 


    scorecard2 = pd.read_csv('fd-2021-11-present.csv')
    print("scorecard2:")
    #print(scorecard2.info())
    scorecard_both = pd.concat([scorecard1_xform, scorecard2], ignore_index=True )
    answer = bulk_sections(scorecard_both, 2018, 1, 2023, 11)
    print("##### answer #####")
    print(answer.info())