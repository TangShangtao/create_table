
import pandas as pd
import polars as pl
import os
import datetime as dt
import sqlalchemy
from loguru import logger
from tqdm import tqdm

import config

project = os.path.basename(os.getcwd())

market_data_db = sqlalchemy.create_engine(f"postgresql+psycopg2://postgres:Tt1234567890@localhost:5432/market_data")

def market_data_db_execute(statement):
    conn = market_data_db.connect()
    return conn.execute(sqlalchemy.text(statement))

logger.add(f"logs/{project}.log")