from datetime import datetime
import os

from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import Entity

import pandas as pd

# NOTE: You must set the environment variable BS_CONNECTION_STRING to the connection
# string for the Azure Table
CONNECTION_STRING = os.environ['BS_CONNECTION_STRING']

def get_table_service():
    table_service = TableService(connection_string=CONNECTION_STRING)

    return table_service

def query_aq_data(sensor_id,
                  from_date=None,
                  to_date=None,
                  cols=None,
                  max_rows=None):
    table_service = get_table_service()

    # Example filter string
    # "RowKey eq 'aq-deployment_nesta-7' and PartitionKey gt '1558530015398' and PartitionKey lt '1558560612372'",

    filter_str = f"RowKey eq '{sensor_id}'"
    
    if from_date is not None:
        filter_str += f" PartitionKey gt '{datetime_to_pk(from_date)}'"
    
    if to_date is not None:
        filter_str += f" PartitionKey lt '{datetime_to_pk(to_date)}'"

    if cols is None:
        selected_cols = None
    else:
        cols.insert(0, 'PartitionKey')
        cols.insert(0, 'RowKey')
        selected_cols = ",".join(cols)

    print(filter_str)

    results = table_service.query_entities('PublicData',
                                           filter=filter_str,
                                           select=selected_cols,
                                           num_results=max_rows)
    
    df = pd.DataFrame(results)
    df['PartitionKey'] = pk_to_datetime(df['PartitionKey'])
    df = df.drop('etag', axis=1)
    df = df.set_index('PartitionKey')

    return df

def pk_to_datetime(pk):
    if type(pk) is pd.core.series.Series:
        pk_in_s = pk.str.slice(0, -3)
        parsed_result = pk_in_s.map(lambda x: datetime.utcfromtimestamp(int(x)))
    elif type(pk) is str:
        pk_in_s = pk[:-3]
        parsed_result = datetime.utcfromtimestamp(int(pk_in_s))

    return parsed_result


def datetime_to_pk(dt):
    timestamp = (dt - datetime(1970, 1, 1)).total_seconds()
    timestamp *= 1000

    return f'{timestamp:.0f}'


