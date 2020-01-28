# Breathing Spaces code
This repository contains the code written as part of the _Breathing Spaces_ project (see https://breathingspaces.org.uk/) to import, process, analyse and plot data from the network of air quality sensors deployed in Southampton (principally around St Denys).

This repository is expected to exist in a folder hierarchy as follows:

  - Breathing Spaces
    - Code (_this repository_)
    - Data (see other repository for the contents)
      - AURN
      - BS Sensors
      - ...

## Software setup

### Python
The majority of the code in this repository depends on Python, with a range of data science packages installed. The best way to get all the dependencies is to install the [Anaconda Python distribution](https://www.anaconda.com/distribution/) and then install from one of the `environment.yml` files provided in this repository.

The following command will install all of the packages used for this project, with the exact versions used by the original developer (Robin Wilson). It will probably only work on OS X, as that is what the original developer used:

```
conda env create -f environment_exact_versions.yml
```

The following command will install all of the packages used for this project, using just the names of the packages and therefore getting the latest versions and all of the OS-specific dependencies. This should work on any operating system, but may provide later versions than the ones used in the original analysis which may cause problems if there have been breaking changes:

```
conda env create -f environment_just_packages.yml
```

### Go
One piece of code is written in the Go language, and this is used to import back data to InfluxDB. If you're not doing this particular task then there is no need to install Go. If you are, then read below.

 1. Install Go from https://golang.org/
 2. Put `csv_to_influx.go` somewhere inside your `$GOPATH`
 3. Change to the directory containing `csv_to_influx.go` and run `go get .`
 4. To run the code, run `go run csv_to_influx.go` with the relevant command-line arguments (see notes below)

## Repository contents

### Analysis of air quality data
 - `azure_table_interface.py` - provides a nice Python interface to data stored in Azure Tables. Does not know what the data is (ie. doesn't do anything to do with the air quality data) but deals with the format of the tables and the datetime formats.
 - `get_aq_data.py` - provides a nice Python interface for getting the air quality data from two sources: either Flo's manually-exported data or the Azure Tables. For the latter it uses the `azure_table_interface.py` functions.
 - `get_weather_data.py` - simple function to get weather data from Weather Underground
 - The majority of the `.ipynb` files not described below contain code for individual analyses/plots of the air quality data. For example `Compare Azure and Flo Data.ipynb` compares data from the Azure Table and the data that Flo provided. `Plot Marathon Time-Series.ipynb` plots a time series graph of the time around the marathon when many roads were closed.

### Importing data
  - `Process back data before ingestion to InfluxDB.ipynb` - processes the back data obtained from Flo ready for importing into InfluxDB. This includes quite a few complex processes, but it is well-documented in the notebook. This produces output files in `../Data/BS Sensors` so make sure this directory exists.
  - `csv_to_influx.go` - code, written in the Go language, for importing CSV data to InfluxDB while also dealing with null values properly. Taken from https://github.com/jpillora/csv-to-influxdb, licensed under the MIT license. This code requires lots of command-line arguments to specify which InfluxDB instance to use, authentication methods and columns to use for various purposes. A command like the following should work:

  ```
  go run csv_to_influx.go --server https://aqdata.uksouth.cloudapp.azure.com:8086 --database aqdata --username USERNAME --password PASSWORD -m ttn -tag-columns dev_id,p_corrected,p_display --timestamp-column timestamp --timestamp-format "unix" --treat-null CSVFILE.csv
  ```

  This will import `CSVFILE.csv` to the `aqdata` database on the InfluxDB server, importing into the `ttn` measurement with various columns specified as tags, the `timestamp` column specified as a timestamp in unix nanosecond format (Note: That is nanoseconds since Unix epoch, rather than seconds since Unix epoch) and dealing with null values sensibly.