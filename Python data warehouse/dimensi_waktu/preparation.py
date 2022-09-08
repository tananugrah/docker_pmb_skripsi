import awswrangler as wr
from helpers.access import *
import asyncio

import pandas as pd
import numpy as np
import pg8000
import sys


def connection_postgres():
    try:

        con = pg8000.connect(
            host=host_dwh,
            port='5432',
            database = database_dwh,
            user = user_dwh,
            password = password_dwh
        )

    except pg8000.Error as er:
        print("OOps: Something Else:", er)

    else:
        return con
 

async def read_data(sql_statement): #read data from sources
    try:

        connection_= connection_postgres()
        datareader = wr.postgresql.read_sql_query(
            sql=sql_statement,
            con=connection_
        )

        connection_.close()

    except:
        print('Opps: Check your statemen query:', sys.exc_info())

    else:
        return datareader


async def operation_read_data(sql_statement): #thread

    try:
        tasks = []
        tasks.append(asyncio.ensure_future(read_data(sql_statement)))
        original_result_read_data = await asyncio.gather(*tasks)

    except:
        print('operations read_data exeception in :', sys.exc_info())
    else:
        return original_result_read_data


# prepare data replace string = null to empty value
async def prepare_data(reader_data): 
    df_dirty= pd.DataFrame(reader_data)

    for col in list(df_dirty.select_dtypes(include=['string', 'object']).columns):
        df_dirty[col].fillna(value=np.nan, inplace=True)

    for col in list(df_dirty.select_dtypes(include=['Int64', 'boolean', 'float64']).columns):
        df_dirty[col].fillna(value=np.nan, inplace=True)


    return df_dirty


async def operation_prepare_data(data_accessibility):

    try:
        tasks = []
        tasks.append(asyncio.ensure_future(prepare_data(data_accessibility)))
        original_result_prepare_data = await asyncio.gather(*tasks)

    except:
        print('operations prepare_data exeception in :', sys.exc_info())
    else:
        return original_result_prepare_data


async def ingestion_data_to_dwh (df): #ingests data to dwh
    try:
        # componen dataframe
        df_postgresql = pd.DataFrame(df)
        if len(df_postgresql) != 0:
            
            # convert string  
            df_postgresql[[
                'id_pmb',
                'tanggal_ujian',
                'tahun_akademik'
            ]] = df_postgresql[[ 
                'id_pmb',
                'tanggal_ujian',
                'tahun_akademik'
            ]].astype('string')

            # convert datetime
            df_postgresql[[
                'tanggal_lahir',
                'tanggal_bayar'
            ]] = df_postgresql[[ 
                'tanggal_lahir',
                'tanggal_bayar'
            ]].astype('datetime64[ns]')


            # convert int
            # df_postgresql[[ 
            # ]] = df_postgresql[[ 
            # ]].astype('Int64')

            # convert float
            df_postgresql[[
                'id_tahun',
                'umur'
            ]] = df_postgresql[[ 
                'id_tahun',
                'umur'
            ]].astype('float64')

            # convert boolean
            # df_postgresql[[
            # ]] = df_postgresql[[ 
            # ]].astype('boolean')

            print('dataframe info after prepare: \n') 
            print(df_postgresql.info()) #show the information standard of data

            # ingestion statemen
            wr.postgresql.to_sql(
                df_postgresql,
                con=connection_postgres(),
                schema="public",
                table="dim_waktu",
                mode="upsert",
                chunksize=200,
                upsert_conflict_columns=[
                  'id_pmb'
                ]
            )
        else:
            print('Dataframe empty dataset')
    except:
        print('error in \n', sys.exc_info())
    else:
        print('success')
        connection_postgres().close()


async def operation_ingestion_data(data_ingestion_df):

    try:
        tasks = []
        tasks.append(asyncio.ensure_future(ingestion_data_to_dwh(data_ingestion_df)))
        original_result_ingestion_data = await asyncio.gather(*tasks)

    except:
        print('operations ingestion_data exeception in :', sys.exc_info())
    else:
        return original_result_ingestion_data
