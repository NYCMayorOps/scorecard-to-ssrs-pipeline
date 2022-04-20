import os
import pyodbc
from turtle import fd
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, DateTime, \
     ForeignKey, event
from sqlalchemy.orm import scoped_session, sessionmaker, backref, relation
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta
load_dotenv()

class Connector:
    fd = pd.DataFrame()
    fd_mock = pd.read_csv('fd_mock_all.csv')
    fd_bids_mock = pd.read_csv('fd_bids.csv')
    linear_miles= pd.DataFrame()
    district = pd.DataFrame() #pd.read_csv('district.csv')
    bid_linear_miles = pd.DataFrame #pd.read_csv('bid_linear_miles.csv')
    conn = None

    def __init__(self):
        self.conn = self.db_connect()
        self.fd = self.get_fd_last_2_years()
        self.linear_miles = self.get_linear_miles()
        self.bid_linear_miles = self.get_bid_linear_miles()
        self.district = self.get_district()

    def db_connect(self):
        conn_str=os.getenv("CONNECTION_STRING_SQL_ALCHEMY")
        print(conn_str)
        return create_engine(conn_str)

    def test_fulcrum_data(self):
        df = pd.read_sql('SELECT TOP(5) * FROM [dbo].[Fulcrum_Data]', self.conn)
        print(df.info())
    
    #read past two years into fulcrum data
    def get_fd_last_2_years(self):
        two_years_ago = datetime.now() - relativedelta(years=2)
        return pd.read_sql(f"SELECT * FROM [dbo].[Fulcrum_Data] WHERE [_created_at] >= PARSE('{two_years_ago}' AS DATETIME);", self.conn)

    #get linear miles
    def get_linear_miles(self):
        return pd.read_sql(f"SELECT * FROM [dbo].[LinearMiles];", self.conn)

    #get bid linear miles
    def get_bid_linear_miles(self):
        return pd.read_sql(f"SELECT * FROM [dbo].[bid_linear_miles];", self.conn)

    #get districts
    def get_district(self):
        return pd.read_sql(f"SELECT * FROM [dbo].[District];", self.conn)


    #upload Section
    #upload district
    #upload borough
    #upload citywide
    #upload bid
    #upload bid citywide
