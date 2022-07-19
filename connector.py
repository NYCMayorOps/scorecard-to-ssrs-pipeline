import os
import pyodbc
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, DateTime, \
     ForeignKey, event
from sqlalchemy.orm import scoped_session, sessionmaker, backref, relation
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path

#dotenv_path = Path( 'c:\\Users\\sscott1\\secrets\\.env')
#load_dotenv(dotenv_path=dotenv_path)
from airflow.models import Variable


class Connector:
    delete_if_months_do_not_match = True
    fd = pd.DataFrame()
    fd_mock = pd.DataFrame()
    fd_bids_mock = pd.DataFrame()
    linear_miles= pd.DataFrame()
    district = pd.DataFrame() 
    bid_linear_miles = pd.DataFrame() 
    conn = None
    reporting_root = None


    def __init__(self):
        self.reporting_root =  Variable.get("reporting_root")
        #self.reporting_root = os.getenv('REPORTING_ROOT')
        self.conn = self.db_connect()
        print("connected to db")
        self.fd = self.get_fd_last_2_years()
        self.linear_miles = self.get_linear_miles()
        assert (self.linear_miles.empty == False)
        self.bid_linear_miles = self.get_bid_linear_miles()
        assert (self.bid_linear_miles.empty == False)
        self.district = self.get_district()
        self.fd_mock = self.ryan_filter(pd.read_csv(Path(self.reporting_root) / 'fd_mock_all.csv'))
        self.fd_bids_mock = self.ryan_filter(pd.read_csv(Path(self.reporting_root) / 'fd_bids.csv'))
        print("connection initialized")

    def db_connect(self):
        connection_string = Variable.get("CONNECTION_STRING_SQL_ALCHEMY")
        #connection_string = os.getenv('CONNECTION_STRING_SQL_ALCHEMY')
        conn_str=connection_string
        #print(conn_str)
        return create_engine(conn_str)

    def test_fulcrum_data(self):
        df = pd.read_sql('SELECT TOP(5) * FROM [dbo].[Fulcrum_Data]', self.conn)
        #print(df.info())
    
    #read past two years into fulcrum data
    def get_fd_last_2_years(self):
        two_years_ago = datetime.now() - relativedelta(years=2)
        return  pd.read_sql(f"SELECT * FROM [dbo].[Fulcrum_Data] WHERE [_created_at] >= PARSE('{two_years_ago}' AS DATETIME);", self.conn)

    #get linear miles
    def get_linear_miles(self):
        answer =  pd.read_sql(f"SELECT * FROM [dbo].[LinearMiles];", self.conn)
        answer.LINEAR_MILES = answer.LINEAR_MILES.astype(float)
        return answer
        
    #get bid linear miles
    def get_bid_linear_miles(self):
        answer = pd.read_sql(f"SELECT * FROM [dbo].[bid_linear_miles];", self.conn)
        answer.linear_miles = answer.linear_miles.astype(float)
        return answer
    #get districts
    def get_district(self):
        return pd.read_sql(f"SELECT * FROM [dbo].[District];", self.conn)

    def set_string(self, df, key_list):
        '''
        SET val = @val WHERE [key] = @key;
        ''' 
        pass

    def insert_string(self, df, db_name, key_list):
        f'''
        INSERT INTO {db_name} VALUES(); 
        '''
        pass

    def upload_query(self, df, key_list, db_name):
        return f'''BEGIN TRANSACTION; 
        UPDATE {db_name} WITH (UPDLOCK, SERIALIZABLE) 
        {self.set_string(df, key_list)}
        IF @@ROWCOUNT = 0 
        BEGIN 
        {self.insert_string(df, db_name, key_list)}
        END 
        COMMIT TRANSACTION;
        '''

    #upload Section
    def upload_sections(self, df):
        for index, row in df.iterrows():
            self.conn.execute(f'''''')
    
    def ryan_filter(self, fd):
        fd['my_date2'] = pd.to_datetime(fd['_updated_at'], format='%Y-%m-%d %H:%M:%S')
        fd_copy = fd.copy()
        my_filter = []
        i = 0
        j = 0
        for index, row in fd_copy.iterrows():
            j += 1
            try:
                assert(int(row['currentmonth']) == row.my_date2.month and int(row['currentyear']) == row.my_date2.year )
                my_filter.append(True)
            except:
                #logging.warn(f"failed to match {row.currentmonth}, {row.currentyear}, {row.my_date2}. Deleting from fulcrum data")
                my_filter.append(False)
                i += 1
        #### output prod fd
        prod_copy = fd.copy()
        prod_copy = prod_copy.drop('my_date2', axis=1)
        #prod_copy.to_csv('fd_prod_copy.csv')
        ####
        if self.delete_if_months_do_not_match:
            print(f"{i} rows deleted \n of {j} rows; \n that is {i / j} percent.")
            fd = fd[my_filter]
        else:
            print(f"this run preserves all {j} rows of fuclcrum data.")
        fd.reset_index(drop=True, inplace=True)     
        fd = fd.copy()
        #print(fd.info())
        return fd

