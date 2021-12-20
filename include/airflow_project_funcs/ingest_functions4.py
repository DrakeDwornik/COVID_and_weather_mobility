import logging
import urllib
import pandas as pd
import os
import json
import urllib.request
import zipfile
from datetime import date
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


# loading

def unzip_file():
    with zipfile.ZipFile("/Users/dwornikdrake/dev/airflow/includes/airflow_project/working_files/mobility.zip",
                         "r") as zip_ref:
        zip_ref.extractall("/Users/dwornikdrake/dev/airflow/includes/airflow_project/working_files/extracted_zip")


def mobility_cleanup():
    filenames = os.listdir("/Users/dwornikdrake/dev/airflow/includes/airflow_project/working_files/extracted_zip")
    for filename in filenames:
        if filename.startswith("2020_US") or filename.startswith("2021_US"):
            pass
        else:
            os.remove(
                f"/Users/dwornikdrake/dev/airflow/includes/airflow_project/working_files/extracted_zip/{filename}")
    os.remove("/Users/dwornikdrake/dev/airflow/includes/airflow_project/working_files/mobility.zip")


def load_mobility_data():
    url = 'https://www.gstatic.com/covid19/mobility/Region_Mobility_Report_CSVs.zip'
    urllib.request.urlretrieve(url,
                               '/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/mobility.zip')
    with zipfile.ZipFile("/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/mobility.zip",
                         "r") as zip_ref:
        zip_ref.extractall("/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/extracted_zip")
    filenames = os.listdir("/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/extracted_zip")
    for filename in filenames:
        if filename.startswith("2020_US") or filename.startswith("2021_US"):
            pass
        else:
            os.remove(
                f"/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/extracted_zip/{filename}")
    os.remove("/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/mobility.zip")
    filename = '2020_US_Region_Mobility_Report.csv'
    df = pd.read_csv(f"/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/extracted_zip/{filename}")
    filename = '2021_US_Region_Mobility_Report.csv'
    df2 = pd.read_csv(
        f"/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/extracted_zip/{filename}")
    df3 = df.append(df2)
    df3['date'] = pd.to_datetime(df3['date'])
    df4 = df3[['country_region_code', 'sub_region_1', 'sub_region_2', 'census_fips_code', 'date',
               'retail_and_recreation_percent_change_from_baseline', 'parks_percent_change_from_baseline',
               'grocery_and_pharmacy_percent_change_from_baseline', 'transit_stations_percent_change_from_baseline',
               'workplaces_percent_change_from_baseline', 'residential_percent_change_from_baseline']]
    ncc_mobility = df4[df4["census_fips_code"] == 10003]
    dc_mobility = df4[df4["census_fips_code"] == 11001]
    ncc_mobility.to_pickle("/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/ncc_mobility.pkl")
    dc_mobility.to_pickle("/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/dc_mobility.pkl")


def dl_weather_data_ncc():
    params = {"datasetid": "GHCND", "locationid": "FIPS:10003", "startdate": "2020-02-15", "enddate": "2021-02-14"}
    base_url = f"https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
    headers = {"Accept": "*/*", "Accept-Encoding": "gzip, deflate, br", "Connection": "keep-alive",
               "token": "ghMjqMzwiiVOVbvZOfUOgHmilxSCCBvS", "Content": "application/json"}
    get_all_data(base_url, params, headers, filename="2020_json_ncc_")
    today = date.today()
    end = today.strftime("%Y-%m-%d")
    params = {"datasetid": "GHCND", "locationid": "FIPS:10003", "startdate": "2021-02-15", "enddate": end}
    get_all_data(base_url, params, headers, filename="2021_json_ncc_")


def dl_weather_data_dc():
    params = {"datasetid": "GHCND", "locationid": "FIPS:11001", "startdate": "2020-02-15", "enddate": "2021-02-14"}
    base_url = f"https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
    headers = {"Accept": "*/*", "Accept-Encoding": "gzip, deflate, br", "Connection": "keep-alive",
               "token": "oebeUQMqFfjWpaiLWLxheFizGqkQDhOw", "Content": "application/json"}
    get_all_data(base_url, params, headers, filename="2020_json_dc_")
    today = date.today()
    end = today.strftime("%Y-%m-%d")
    params = {"datasetid": "GHCND", "locationid": "FIPS:11001", "startdate": "2021-02-15", "enddate": end}
    get_all_data(base_url, params, headers, filename="2021_json_dc_")


# from json_helper.py
def read_all_json_files(path_to_json_files, contains):
    file_list = os.listdir(path_to_json_files)
    json_objects = []
    file_list.sort()
    for file in file_list:
        if contains in file:
            if file.upper().endswith(".JSON"):
                json_object = json_helper(os.path.join(path_to_json_files, file))
                # json_objects += json_helper(os.path.join(path_to_json_files, file))
                json_objects += json_object
                os.remove(os.path.join(path_to_json_files, file))
    return json_objects


def json_helper(file_path):
    """
    opens a file, reads the json string and returns the json object
    :param file_path: path to file
    :return: json object
    """
    json_object = None
    file = None
    try:
        file = open(file_path)
    except OSError as e:
        print("Error opening the file. Please ensure the file exists and has appropriate permissions.")
        logging.error(e)
    else:
        json_object = json_read(file)
    finally:
        file.close() if file else logging.warning("No file resource available to close.")
        print("asdasdasdasd")
        return json_object


def json_read(file):
    json_object = json.load(file)
    return json_object


#### graphing
def graph_mobility(mobility, percip, place):
    x = mobility.index
    fig, ax = plt.subplots()
    plt.xticks(rotation=90)
    ax2 = ax.twinx()
    fig.set_size_inches(15, 9)

    ax.plot(x, mobility['grocery_and_pharmacy_percent_change_from_baseline'], color='red', label="groceries")
    ax.plot(x, mobility['residential_percent_change_from_baseline'], color='blue', label="home")
    ax.plot(x, mobility['parks_percent_change_from_baseline'], color='orange', label="parks")
    ax.plot(x, mobility['transit_stations_percent_change_from_baseline'], color='yellow', label="transit")
    ax.plot(x, mobility['workplaces_percent_change_from_baseline'], color='pink', label="work")
    ax.plot(x, mobility['retail_and_recreation_percent_change_from_baseline'], color='purple',
            label="recreation")
    ax2.plot(x, percip, color="green", label="precipitation")
    ax.grid()
    ax.legend(loc="lower left")
    ax.set(xlabel='Year and Week', ylabel='percent change from baseline',
           title=f'weekly mobility in {place} 2-15-2020 to today')
    ax2.set(ylabel="precipitation", ylim=[0, 1000])
    ax2.legend()
    q = plt.savefig(
        f"/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/{place}_graph.jpg")


def graph_difference(mobility1, mobility2, place1, place2):
    print("entered graph difference")
    x = mobility1.index
    print("set x")
    fig, ax = plt.subplots()
    print("split subplots")
    fig.set_size_inches(15, 9)
    print("set figure")
    plt.xticks(rotation=90)
    print("set xticks")
    ax.plot(x, mobility1['residential_percent_change_from_baseline'] - mobility2[
        'residential_percent_change_from_baseline'], color='cyan', label="home difference")
    print("first plot")
    ax.plot(x, mobility1['transit_stations_percent_change_from_baseline'] - mobility2[
        'transit_stations_percent_change_from_baseline'], color='yellow', label="transit difference")
    ax.plot(x, mobility1['workplaces_percent_change_from_baseline'] - mobility2[
        'workplaces_percent_change_from_baseline'], color='red', label="work difference")
    ax.plot(x, mobility1['retail_and_recreation_percent_change_from_baseline'] - mobility2[
        'retail_and_recreation_percent_change_from_baseline'], color='purple', label="recreation difference")
    ax.grid()
    print("added grid")
    ax.legend(loc="lower left")
    ax.set(xlabel='Year and Week', ylabel=f'{place1} change - {place2} change',
           title=f'weekly mobility change, {place1} and {place2}')
    print("saving fig")
    q = plt.savefig(
        f"/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/difference{place1}_{place2}.jpg")


def parse_params(params) -> str:
    parsed_params = ""
    separator = "?"
    for key, value in params.items():
        parsed_params += separator
        parsed_params += f"{key}={value}"
        separator = "&"
    return parsed_params


def get_data_once(base_url, params, headers, limit=1000, offset=1):
    url = f"{base_url}{parse_params(params)}&limit={limit}&offset={offset}"
    print(url)
    request = urllib.request.Request(url, headers=headers)
    response = urllib.request.urlopen(request)
    json_data = json.loads(response.read())
    if json_data != {}:
        return json_data['results']
    else:
        return {}


def get_all_data(base_url, params, headers, limit=1000, filename="json_"):
    offset = 1
    data_exists = True
    file_no = 0
    while data_exists:
        json_data = get_data_once(base_url, params, headers, limit, offset)
        if json_data != {}:
            with open(
                    f"/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/{filename}{file_no:02d}.json",
                    "w") as file:
                json.dump(json_data, file)
            offset += 1000
            file_no += 1
        else:
            data_exists = False


def download_mobility():
    download_file()
    unzip_file()
    mobility_cleanup()


def download_file():
    url = 'https://www.gstatic.com/covid19/mobility/Region_Mobility_Report_CSVs.zip'
    urllib.request.urlretrieve(url,
                               '/Users/dwornikdrake/dev/airflow/includes/airflow_project/working_files/mobility.zip')


def load_the_graphs():
    weekly_percip_dc = pd.read_pickle(
        "/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/weekly_percip_dc.pkl")
    weekly_mobility_dc = pd.read_pickle(
        "/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/weekly_mobility_dc.pkl")
    weekly_percip_ncc = pd.read_pickle(
        "/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/weekly_percip_ncc.pkl")
    weekly_mobility_ncc = pd.read_pickle(
        "/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/weekly_mobility_ncc.pkl")
    graph_mobility(weekly_mobility_dc, weekly_percip_dc, "DC")
    graph_mobility(weekly_mobility_ncc, weekly_percip_ncc, "NCC")
    graph_difference(weekly_mobility_dc, weekly_mobility_ncc, "DC", "NCC")


def convert_data_to_weekly():
    dc_mobility = pd.read_pickle(
        "/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/dc_mobility.pkl")
    ncc_mobility = pd.read_pickle(
        "/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/ncc_mobility.pkl")
    percip_dc = pd.read_pickle("/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/dc_percip.pkl")
    percip_ncc = pd.read_pickle("/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/ncc_percip.pkl")
    weekly_percip_dc = percip_dc.groupby(percip_dc['date'].dt.strftime('%Y-week %W'))['value'].mean()
    weekly_mobility_dc = dc_mobility.groupby(dc_mobility['date'].dt.strftime('%Y-week %W')).mean()
    weekly_percip_ncc = percip_ncc.groupby(percip_ncc['date'].dt.strftime('%Y-week %W'))['value'].mean()
    weekly_mobility_ncc = ncc_mobility.groupby(ncc_mobility['date'].dt.strftime('%Y-week %W')).mean()
    weekly_percip_dc.to_pickle(
        "/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/weekly_percip_dc.pkl")
    weekly_mobility_dc.to_pickle(
        "/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/weekly_mobility_dc.pkl")
    weekly_percip_ncc.to_pickle(
        "/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/weekly_percip_ncc.pkl")
    weekly_mobility_ncc.to_pickle(
        "/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/weekly_mobility_ncc.pkl")


def load_weather_data_ncc():
    ncc_weather = pd.DataFrame(
        read_all_json_files("/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files", "_ncc_"))
    ncc_weather['date'] = pd.to_datetime(ncc_weather['date'])
    percip_ncc = ncc_weather.loc[ncc_weather["datatype"] == "PRCP"][["date", "value"]]
    percip_ncc.to_pickle("/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/ncc_percip.pkl")


def load_weather_data_dc():
    dc_weather = pd.DataFrame(
        read_all_json_files("/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files", "_dc_"))
    dc_weather['date'] = pd.to_datetime(dc_weather['date'])
    percip_dc = dc_weather.loc[dc_weather["datatype"] == "PRCP"][["date", "value"]]
    percip_dc.to_pickle("/Users/dwornikdrake/dev/airflow/include/airflow_project/working_files/dc_percip.pkl")
