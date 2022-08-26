import sys
import os
import pandas as pd
from tqdm.notebook import tqdm

from sqlalchemy.engine import create_engine
from connections.connections import oracle_url, postgre_url

from typing import Generator
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from datetime import datetime

def uploading_sales():
    oracle_engine = create_engine(oracle_url())
    postgre_engine = create_engine(postgre_url(), pool_pre_ping = True, echo = True)
    chunksize = 3000000


    with postgre_engine.connect() as con:
        rs = con.execute('''
                            DROP TABLE pmix_sales;
                         ''')


    qry = f'''
              SELECT product_id, pbo_location_id, sales_dt, gross_sales_amt, 
                     gross_sales_amt_discount, sales_qty, sales_qty_discount
              FROM SAS_INTERF.IA_PMIX_SALES
              WHERE CHANNEL_CD = 'ALL' and sales_dt <> current_date
           '''
    table_generator = pd.read_sql_query(qry, oracle_engine, chunksize = 1500000)


    for table in tqdm(table_generator):
        srtd_table = table[['product_id', 'pbo_location_id', 'sales_dt', 'gross_sales_amt', 
                            'gross_sales_amt_discount', 'sales_qty', 'sales_qty_discount']].copy()
        print(f'table size: {srtd_table.shape}')
        srtd_table['sales_dt'] = pd.to_datetime(srtd_table['sales_dt'])
        srtd_table.to_sql('pmix_sales', postgre_engine, if_exists='append', index=False)


    with postgre_engine.connect() as con:
        rs = con.execute('''
                            DROP TABLE dates_last40;
                         ''')


    with postgre_engine.connect() as con:
        rs = con.execute('''
                            CREATE TABLE dates_last40 as
                            SELECT distinct sales_dt as dt
                            FROM pmix_sales
                         ''')


    with postgre_engine.connect() as con:
        rs = con.execute('''
                            DELETE FROM pmix_sales_history
                            WHERE sales_dt >= (select min(dt) from dates_last40)
                         ''')


    with postgre_engine.connect() as con:
        rs = con.execute('''
                            INSERT INTO pmix_sales_history
                            SELECT *
                            FROM pmix_sales
                         ''')


    with postgre_engine.connect() as con:
        rs = con.execute('''
                            DROP TABLE assort_matrix_history;
                         ''')


    qry = f'''
    SELECT *
    FROM SAS_INTERF.IA_ASSORT_MATRIX_HISTORY
           '''

    table_generator = pd.read_sql_query(qry, oracle_engine, chunksize=chunksize)

    for table in tqdm(table_generator):
        print(f'table size: {table.shape}')

        table.to_sql('assort_matrix_history', postgre_engine, if_exists='append', index=False)


    with postgre_engine.connect() as con:
        rs = con.execute('''
                            DROP TABLE price_history;
                         ''')


    def lower_clmns_names(clmns: list) -> dict:
        rnm_clmns = [clmn.lower() for clmn in clmns]
        rnm_clmns_d = dict(zip(clmns, rnm_clmns))
        return rnm_clmns_d


    qry = f'''
    SELECT product_id, pbo_location_id, gross_price_amt, start_dt, end_dt
    FROM SAS_INTERF.IA_PRICE_HISTORY
           '''
    table_generator = pd.read_sql_query(qry, oracle_engine, chunksize=chunksize)

    for table in tqdm(table_generator):
        clmn_names = lower_clmns_names(table.columns)
        table = table.rename(columns=clmn_names)
        table['start_dt'] = pd.to_datetime(table['start_dt'])
        table['end_dt'] = pd.to_datetime(table['end_dt'])

        table.to_sql('price_history', postgre_engine, if_exists='append', index=False)


    with postgre_engine.connect() as con:
        rs = con.execute('''
                            DROP TABLE skelet_assort_last40
                         ''') 


    with postgre_engine.connect() as con:
        rs = con.execute('''
                            CREATE TABLE skelet_assort_last40 as
                            SELECT product_id, pbo_location_id, dt
                            FROM assort_matrix_history INNER JOIN dates_last40
                                 ON dt BETWEEN start_dt and end_dt
                            ORDER BY dt;
                         ''')


    with postgre_engine.connect() as con:
        rs = con.execute('''
                            DROP TABLE skelet_prhist_last40
                         ''')


    with postgre_engine.connect() as con:
        rs = con.execute('''
                            CREATE TABLE skelet_prhist_last40 as
                            SELECT product_id, pbo_location_id, dt, gross_price_amt
                            FROM  price_history INNER JOIN dates_last40
                                 ON dt BETWEEN start_dt and end_dt
                            ORDER BY dt;
                         ''')


    with postgre_engine.connect() as con:
        rs = con.execute('''
                            DROP TABLE skelet_assort_prhist_last40
                         ''')


    with postgre_engine.connect() as con:
        rs = con.execute('''
                            CREATE TABLE skelet_assort_prhist_last40 as
                            SELECT a.product_id, a.pbo_location_id, a.dt, gross_price_amt
                            FROM  skelet_assort_last40 a LEFT JOIN skelet_prhist_last40 p
                                 ON a.product_id = p.product_id and
                                    a.pbo_location_id = p.pbo_location_id and
                                    a.dt = p.dt;
                         ''')


    with postgre_engine.connect() as con:
        rs = con.execute('''
                            DELETE FROM sales
                            WHERE dt >= (select min(dt) from dates_last40)
                         ''')


    with postgre_engine.connect() as con:
        rs = con.execute('''
                            INSERT INTO sales
                            SELECT s.product_id, s.pbo_location_id, dt,
                                   CASE WHEN (gross_price_amt is null and sales_qty <> 0 and sales_qty is not Null) 
                                        then gross_sales_amt / sales_qty 
                                        else gross_price_amt end as gross_price_amt,
                                   CASE WHEN sales_qty = 0 or sales_qty is null or
                                                 (gross_sales_amt <> 0 and 
                                                 gross_price_amt * sales_qty / gross_sales_amt >= 0.99 and
                                                 gross_price_amt * sales_qty / gross_sales_amt <= 1.01)
                                        then gross_price_amt
                                        else gross_sales_amt / sales_qty end as pmix_price,

                                   gross_sales_amt, sales_qty

                            FROM  skelet_assort_prhist_last40 s LEFT JOIN pmix_sales p
                                 ON s.product_id = p.product_id and
                                    s.pbo_location_id = p.pbo_location_id and
                                    dt = sales_dt
                            ORDER BY dt;
                         ''')