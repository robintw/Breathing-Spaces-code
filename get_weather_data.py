import pandas as pd
import requests
import io

def getWeatherData(station, day, month, year, timespan):
    """
    Function to return a data frame of minute, or daily weather data for a single Wunderground PWS station.
    
    Args:
        station (string): Station code from the Wunderground website
        day (int): Day of month for which data is requested
        month (int): Month for which data is requested
        year (int): Year for which data is requested
        timespan (string): 'month' or 'day'
    
    Returns:
        Pandas Dataframe with weather data for specified station and date.
        
    Function altered from https://www.shanelynn.ie/analysis-of-weather-data-using-pandas-python-and-seaborn/
    """
    url = "http://www.wunderground.com/weatherstation/WXDailyHistory.asp?ID={station}&day={day}&month={month}&year={year}&graphspan={timespan}&format=1"
    full_url = url.format(station=station, day=day, month=month, year=year, timespan=timespan)
    print(full_url)
    # Request data from wunderground data
    response = requests.get(full_url, headers={'User-agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'})
    data = response.text
    # remove the excess <br> from the text data
    data = data.replace('<br>', '')
    # Convert to pandas dataframe (fails if issues with weather station)
    dataframe = pd.read_csv(io.StringIO(data), index_col=False)
    dataframe['station'] = station

    return dataframe
