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
    """
    Queries air quality data from the Azure table managed by Steven Johnston. Data can be selected by sensor_id
    (default is to include all sensors) and filtered by date range. The returned data can be limited to specific
    columns and/or a maximum number of rows.

    Parameters:
     - `sensor_id`: The ID of the sensor to return data for. Must be in the format it is stored in the Azure
     table, such as 'aq-deployment_nesta-4' not just 'nesta-4'. If None then don't filter by sensor_id
     - `from_date`: Filter the data to only data recorded after this date. Can be a datetime instance or a
     a string (which will be passed to 'dateutil.parser.parse'). Note that giving a string date with no time
     information (eg. '2019-07-01') will be interpreted as midnight on that day.
     - `to_date`: Filter the data to only data recorded before this date. Can be a datetime instance or a
     a string (which will be passed to 'dateutil.parser.parse'). Note that giving a string date with no time
     information (eg. '2019-07-01') will be interpreted as midnight on that day.
     - `cols`: A list of specific columns to return. If None then returns all columns.
     - `max_rows`: The maximum number of rows to return. If None then returns all rows.
     - `verbose`: If True prints out useful debugging information such as the generated query

    Returns:
    A pandas DataFrame containing the queried data
    """
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
    else:
        filter_str = "RowKey ne '0'"
    
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


