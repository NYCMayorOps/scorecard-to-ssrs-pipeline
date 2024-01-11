import os
import platform
import pandas as pd
from datetime import datetime

#from connector import Connector
from dotenv import load_dotenv
from pathlib import Path

if platform.system() == 'Windows':
    dotenv_path = Path( f'c:\\Users\\{os.getlogin()}\\secrets\\.env')
    load_dotenv(dotenv_path=dotenv_path)
    reporting_root = os.getenv('REPORTING_ROOT')
    assert reporting_root is not None
    load_dotenv(dotenv_path=dotenv_path)
    import percent_clean_scores_section as pcss
    import percent_clean_scores_district as pcsd
    import percent_clean_scores_bid as pcsbid
    import percent_clean_scores_bid_citywide as pcsbidc
    import percent_clean_scores_boro as pcsb
    from connector import Connector

else:
    from . import percent_clean_scores_section as pcss
    from . import percent_clean_scores_district as pcsd
    from . import percent_clean_scores_bid as pcsbid
    from . import percent_clean_scores_bid_citywide as pcsbidc
    from . import percent_clean_scores_boro as pcsb
    from .connector import Connector
    from airflow.models import Variable
    reporting_root = Variable.get('reporting_root')

def bulk_blockfaces(fd, start_year, start_month, end_year, end_month, connector) -> pd.DataFrame:
    blockfaces = pcss.load_fulcrum_data(fd, start_year, start_month, False, connector, end_year, end_month )
    return blockfaces

def bulk_sections(blockfaces, connector):
    this_agg = pcss.aggregate(blockfaces)
    a = pcss.merge_linear_miles(this_agg, connector)
    a = pcss.rating_calculation(a)
    a = pcss.merge_district(a, connector)
    answer = pcss.final_format(a)
    return answer.sort_values(by=['SECTION', 'MONTH'])

def bulk_districts(fd, start_year, start_month, end_year, end_month, connector):
    #make a list of months-year combos
    pr = pd.period_range(start=f'{start_year}-{start_month}', end=f"{end_year}-{end_month}", freq='m')
    #iterate over the month year combos
    pr_tupes = [(period.year, period.month) for period in pr]
    print(pr_tupes)
    df = pd.DataFrame()
    for t in pr_tupes:
        #print(f"district: {t[0]}-{t[1]}")
        this_df = pcsd.scorecard_districts(fd, t[0], t[1], connector)
        #df = df.append(this_df, ignore_index=True)
        df = pd.concat([df, this_df], ignore_index=True)
    #save output to one file
    return df

quarter_map = {
    1: 1,
    2: 1,
    3: 1,
    4: 2,
    5: 2,
    6: 2,
    7: 3,
    8: 3,
    9: 3,
    10: 4,
    11: 4,
    12: 4
}



def bulk_bids(fd, start_year, end_year, connector):
    df = pd.DataFrame()
    for year in range(start_year, end_year + 1):
        for quarter in range(1,5):
            print(f"{year}-Q{quarter}")
            #df = df.append(pcsbid.scorecard_bids(fd, year, quarter, connector))
            df = pd.concat([df, pcsbid.scorecard_bids(fd, year, quarter, connector)], ignore_index=True)
    return df

def bulk_bids_citywide(fd, start_year, end_year, connector):
    df = pd.DataFrame()
    for year in range(start_year, end_year + 1):
        for quarter in range(1,5):
            print(f"{year}-Q{quarter}")
            #df = df.append(pcsbidc.scorecard_bids_citywide(fd, year, quarter, connector))
            df = pd.concat([df, pcsbidc.scorecard_bids_citywide(fd, year, quarter, connector)], ignore_index=True)
    return df                


def concat_bds(borough, district, section):
    return str(int(borough)) + str(int(district)) +  str(int(section))

def bulk_citywide(fd, start_year, start_month, end_year, end_month, connector):
    pr = pd.period_range(start=f'{start_year}-{start_month}', end=f"{end_year}-{end_month}", freq='m')
    #iterate over the month year combos
    pr_tupes = [(period.year, period.month) for period in pr]
    #print(pr_tupes)
    df = pd.DataFrame()
    for t in pr_tupes:
        print(f"citywide: {t[0]}-{t[1]}")
        this_df = pcsb.scorecard_citywide(fd, t[0], t[1], connector)
        #df = df.append(this_df, ignore_index=True)
        df = pd.concat([df, this_df], ignore_index=True)
    return df

def bulk_boros(fd, start_year, start_month, end_year, end_month, connector):
    pr = pd.period_range(start=f'{start_year}-{start_month}', end=f"{end_year}-{end_month}", freq='m')
    #iterate over the month year combos
    pr_tupes = [(period.year, period.month) for period in pr]
    #print(pr_tupes)
    df = pd.DataFrame()
    for t in pr_tupes:
        print(f"citywide: {t[0]}-{t[1]}")
        this_df = pcsb.scorecard_boros(fd, t[0], t[1], connector)
        #df = df.append(this_df, ignore_index=True)
        df = pd.concat([df, this_df], ignore_index=True)
    return df

def execute():
    connector = Connector()
    scorecard1 = pd.read_csv(Path(reporting_root) / 'fd-2019-8-to-2021-10.csv')
    #print(scorecard1['_updated_at'][0])
    scorecard1['currentmonth'] = scorecard1['_updated_at'].map(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f').month)
    scorecard1['currentyear'] =  scorecard1['_updated_at'].map(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f').year)

    scorecard1.borough_no = (scorecard1.borough_no.astype('Int64')).astype('str')
    scorecard1.district_no = (scorecard1.district_no.astype('Int64')).astype('str')
    scorecard1.section_no = (scorecard1.section_no.astype('Int64')).astype('str')
    scorecard1 = scorecard1.assign(bds_join_on = lambda x: x['borough'] + x['district_no'] + x['section_no'])

    crosswalk = pd.read_csv(Path(reporting_root) / 'scorecard_v1_bds_crosswalk.csv')
    crosswalk.borough_no = (crosswalk.borough_no.astype('Int64')).astype('str')
    crosswalk.district_no = (crosswalk.district_no.astype('Int64')).astype('str')
    crosswalk.section_no = (crosswalk.section_no.astype('Int64')).astype('str')
    crosswalk = crosswalk.assign(bds_join_on = lambda x: x['borough'] + x['district_no'] + x['section_no'])
    #print("crosswalk:")
    #print(crosswalk['bds_join_on'][0])
    
   
    scorecard1 = pd.merge(scorecard1, crosswalk, how='inner', on='bds_join_on' )
    crosswalk = None
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
    scorecard1_xform['district_no'] =         scorecard1['district_name']
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



    scorecard2 = connector.fd
    scorecard_both = pd.concat([scorecard1_xform, scorecard2], ignore_index=True )
    scorecard1_xform, scorecard2 = None, None
    scorecard_irm= pd.read_csv(Path(reporting_root) / 'fulcrum_irm_2017_to_2019.csv')
    
    ###scorecard irm
    '''
     RegId,
     First Name,
     Last Name,
     Email,
     Inspector ID,
     St Segment 1,
     SW Segment 1,
     ST Segment 2,
     SW Segment 2,
     ST Segment 3,
     SW Segment 3,
     ST Segment 4,
     SW Segment 4,
     Bulk,
     BoroDistrictSection,
     RegDate,
     LastPageSaved,
     LastUpdateDate,
     Blockface Number
    '''


    scorecard_irm['currentmonth'] = scorecard_irm['RegDate'].map(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f').month)
    scorecard_irm['currentyear'] =  scorecard_irm['RegDate'].map(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f').year)
    
    crosswalk = pd.read_csv(Path(reporting_root) / 'scorecard_v1_bds_crosswalk.csv')
    scorecard_irm = pd.merge(scorecard_irm, crosswalk, how='inner', on='BoroDistrictSection' )
    #scorecard_irm.to_csv("scorecard_irm_pre_output.csv")
    #create new dataframe
    scorecard_irm_xform = pd.DataFrame()
    scorecard_irm_xform['_project_id'] =         None 
    scorecard_irm_xform['_assigned_to_id'] =     None
    scorecard_irm_xform['_status'] =             None
    scorecard_irm_xform['_latitude'] =           None
    scorecard_irm_xform['_longitude'] =          None
    scorecard_irm_xform['_created_at'] =         scorecard_irm['RegDate']
    scorecard_irm_xform['_updated_at'] =         scorecard_irm['LastUpdateDate']
    scorecard_irm_xform['_version'] =            None
    scorecard_irm_xform['_updated_by_id'] =      scorecard_irm['Inspector ID']
    scorecard_irm_xform['_server_created_at'] =  scorecard_irm['LastUpdateDate']
    scorecard_irm_xform['_server_updated_at'] =  None
    scorecard_irm_xform['_title'] =              None
    scorecard_irm_xform['currentmonth'] =        scorecard_irm['currentmonth'].astype('Int64')
    scorecard_irm_xform['currentyear'] =         scorecard_irm['currentyear'].astype('Int64')
    scorecard_irm_xform['stop_number'] =         None
    scorecard_irm_xform['blockface_title'] =     None
    scorecard_irm_xform['bid_identifier'] =      None
    scorecard_irm_xform['bid_algorithm_flag'] =  None
    scorecard_irm_xform['borough'] =             scorecard_irm['borough']
    scorecard_irm_xform['district_no'] =         scorecard_irm['district_name']
    scorecard_irm_xform['section_no'] =          scorecard_irm['section_name']
    scorecard_irm_xform['blockface'] =           scorecard_irm['Blockface Number']
    scorecard_irm_xform['street_1'] =            scorecard_irm['St Segment 1']
    scorecard_irm_xform['sidewalk_1'] =          scorecard_irm['SW Segment 1']
    scorecard_irm_xform['street_2'] =            scorecard_irm['ST Segment 2']
    scorecard_irm_xform['sidewalk_2'] =          scorecard_irm['SW Segment 2']
    scorecard_irm_xform['street_3'] =            scorecard_irm['ST Segment 3']
    scorecard_irm_xform['sidewalk_3'] =          scorecard_irm['SW Segment 3']
    scorecard_irm_xform['street_4'] =            scorecard_irm['ST Segment 4']
    scorecard_irm_xform['sidewalk_4'] =          scorecard_irm['SW Segment 4']
    scorecard_irm_xform['LogTime'] =             scorecard_irm['LastUpdateDate']
    #scorecard_irm_xform.to_csv('scorecard_irm.csv')
    #print("##### answer #####")
    fd_all = pd.concat([scorecard_irm_xform, scorecard_both], ignore_index=True )
    #fd_all.to_csv('fd_all.csv')
    scorecard_irm_xform, scorecard_both = None, None
    #the filter adds the my_date2 column.
    fd_all = connector.ryan_filter(fd_all)
    start_month=1
    start_year=2017
    today = datetime.today()
    end_month=today.month
    end_year=today.year
    print(f"end_date: {end_year}, {end_month}")

    drop_text = ""
    if connector.delete_if_months_do_not_match:
        drop_text = "drop_mismatch_months"
    else:
        drop_text = "no_drop"
    print(drop_text)

    #turn this off to send to production db.
    dry_run = False

    fd_all = bulk_blockfaces(fd_all, start_year, start_month, end_year, end_month, connector)
    #answer.to_csv(f"bulk_convert_sections-{drop_text}-{answer.MONTH.min()}_to_{answer.MONTH.max()}.csv", index=False)
    #fd_all.to_sql('ResultBlockface',connector.conn, if_exists='replace')
    
    answer = bulk_sections(fd_all, connector)
    if not dry_run:
        answer.to_sql('ResultSection',connector.conn, if_exists='replace')
    
    answer = bulk_citywide(fd_all, start_year, start_month, end_year, end_month, connector)
    #answer.to_csv(f"bulk_convert_citywide-{drop_text}-{answer.Month.min()}_to_{answer.Month.max()}.csv", index=False)
    #answer.to_csv(Path(os.getenv('MAYOR_DASHBOARD_ROOT')) / 'output' / 'scorecard' / f'scorecard_citywide_backfill-{drop_text}.csv', index=False)
    if not dry_run:
        answer.to_sql('ResultCitywide',connector.conn, if_exists='replace')
    
    answer = bulk_boros(fd_all, start_year, start_month, end_year, end_month, connector)
    #answer.to_csv(f"bulk_convert_boro-{drop_text}-{answer.Month.min()}_to_{answer.Month.max()}.csv", index=False)
    if not dry_run:
        answer.to_sql('ResultBoro',connector.conn, if_exists='replace')

    answer = bulk_districts(fd_all, start_year, start_month, end_year, end_month, connector)
    #answer.to_csv(f"bulk_convert_districts-{drop_text}-{answer.Month.min()}_to_{answer.Month.max()}.csv", index=False )
    if not dry_run:
        answer.to_sql('ResultDistrict',connector.conn, if_exists='replace')
    

    #bids not available before 11-2021 
    start_year = 2021
    end_year = today.year

    answer = bulk_bids(fd_all, start_year, end_year, connector)
    #answer.to_csv(f"bulk_convert_bids-{drop_text}-{answer.quarter.min()}_to_{answer.quarter.max()}.csv", index=False)
    #answer.to_csv(Path(os.getenv('MAYOR_DASHBOARD_ROOT')) / 'output' / 'scorecard' / 'scorecard_bids_backfill-{drop_text}.csv', index=False)
    if not dry_run:
        answer.to_sql('ResultBid',connector.conn, if_exists='replace')

    answer = bulk_bids_citywide(fd_all, start_year, end_year, connector)
    #answer.to_csv(f"bulk_convert_bids_citywide-{drop_text}-{answer.quarter.min()}_to_{answer.quarter.max()}.csv", index=False)
    if not dry_run:
        answer.to_sql('ResultBidCitywide',connector.conn, if_exists='replace')

if __name__ == "__main__":
    execute()
    print("done.")