from datetime import datetime
from dateutil.parser import parse
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

def query_aq_data(sensor_id=None,
                  from_date=None,
                  to_date=None,
                  cols=None,
                  max_rows=None,
                  verbose=False):
    table_service = get_table_service()

    if from_date == to_date and from_date is not None:
        raise ValueError('from_date and to_date are the same!')

    if type(from_date) is str:
        from_date = parse(from_date)

    if type(to_date) is str:
        to_date = parse(to_date)

    # Example filter string
    # "RowKey eq 'aq-deployment_nesta-7' and PartitionKey gt '1558530015398' and PartitionKey lt '1558560612372'",
    filter_str = None
    
    if sensor_id is not None:
        filter_str = f"RowKey eq '{sensor_id}'"
    
    if from_date is not None:
        filter_str += f" and PartitionKey gt '{datetime_to_pk(from_date)}'"
    
    if to_date is not None:
        filter_str += f" and PartitionKey lt '{datetime_to_pk(to_date)}'"

    if cols is None:
        selected_cols = None
    else:
        cols.insert(0, 'PartitionKey')
        cols.insert(0, 'RowKey')
        selected_cols = ",".join(cols)

    if verbose:
        print(filter_str)

    results = table_service.query_entities('PublicData',
                                           filter=filter_str,
                                           select=selected_cols,
                                           num_results=max_rows)
    
    df = pd.DataFrame(results)

    if len(df) == 0:
        raise ValueError('No results returned')

    # Convert from unix timestamp (in ms) to actual datetime
    df['PartitionKey'] = pk_to_datetime(df['PartitionKey'])

    # We never use the weird etag col, so drop it
    df = df.drop('etag', axis=1)

    df = df.rename(columns={'RowKey':'sensor_id',
                            'PartitionKey': 'timestamp'})
    df = df.set_index('timestamp')

    # Convert all columns except the sensor ID to floating point values
    columns = list(df.columns)
    columns.remove('sensor_id')

    for column in columns:
        df[column] = df[column].astype(float)

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


