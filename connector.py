import os
import pyodbc
from turtle import fd
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, DateTime, \
     ForeignKey, event
from sqlalchemy.orm import scoped_session, sessionmaker, backref, relation
from sqlalchemy.ext.declarative import declarative_base

from dotenv import load_dotenv

load_dotenv()

class Connector:
    fd = pd.read_csv('fd_mock_all.csv')    
    fd_mock = pd.read_csv('fd_mock_all.csv')
    fd_bids_mock = pd.read_csv('fd_bids.csv')
    linear_miles= pd.read_csv('linear_miles.csv')
    district = pd.read_csv('district.csv')
    bid_linear_miles = pd.read_csv('bid_linear_miles.csv')
    conn = None

    def __init__(self):
        self.db_connect()
        #self.test_fulcrum_data()

    def db_connect(self):
        conn_str=os.getenv("CONNECTION_STRING_SQL_ALCHEMY")
        print(conn_str)
        self.conn = create_engine(conn_str)

    def test_fulcrum_data(self):
        df = pd.read_sql('SELECT TOP(5) * FROM [dbo].[Fulcrum_Data]', self.conn)
        print(df.info())
