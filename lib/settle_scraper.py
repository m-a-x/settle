import requests as req
import json
import pandas as pd
import datetime
import statsmodels.api as sm
from sklearn.externals import joblib
import sys


def dayofweek_int_to_str(dayofweek_int):
    day_int_to_str = { 0: 'Mon', 1: 'Tue', 2: 'Wed', 3: 'Thu', 4: 'Fri', 5: 'Sat', 6: 'Sun'}
    return day_int_to_str[dayofweek_int]


def hodrick_prescott_filter(date_series, lamb):
    """ calculates hodrick_prescott trend filter on a series """
    filtered_series = sm.tsa.filters.hpfilter(date_series, lamb=lamb)[1]
    return filtered_series


def process_page_data(url):
    """
    -gets the next page in the Density API and extracts time and number of people
    connected to wifi for that page.
    -returns tuple of (page data as dataframe, next page url)
    """
    page = req.get(url)
    content = json.loads(page.content) # load API's json content
    page_df = pd.DataFrame(content['data']).loc[:,['client_count','dump_time']]
    page_df.dump_time = pd.to_datetime(page_df.dump_time)
    page_df = page_df.set_index('dump_time') # get num on wifi and time of measure
    page_df.index = page_df.index.rename('t')
    page_df.columns = ['count']
    next_page = content['next_page'] # get url for next page
    return (page_df, next_page)


def scrape_location_data(place, yr_1, m_1, d_1, h_1, min_1, yr_f, m_f, d_f, h_f, min_f):
    """
    -for a building's location id (provided by ADI Density) and
    starting year, month, day, hour (HH), minute (MM) (24 hour time) and
    ending year, month, day, hour (HH), minute (MM) (24 hour time)
    returns a dataframe with the index datetime of measure and value count people on wifi
    """
    building_to_location_id_dic = {'john_jay': '125','jjs': '155', "Butler_Library_4":'132',
                                   "Butler_Library_5":'133', "Butler_Library_6":'134',
                                   "Science_and_Engineering_Library":'145', "Uris":'23',
                                   "Butler_Library_2":'130', "Butler_Library_3":'131'}
    t_1 = str(yr_1) + '-' + str(m_1) + '-' + str(d_1) + 'T' + str(h_1) + ':' + str(min_1)
    t_f = str(yr_f) + '-' + str(m_f) + '-' + str(d_f) + 'T' + str(h_f) + ':' + str(min_f)
    api_key = pd.read_pickle('../data/key.pkl').iloc[0][0]
    url = "http://density.adicu.com/window/" + t_1 + "/" + t_f + "/group/" + building_to_location_id_dic[place] + "?auth_token=" + api_key
    page = req.get(url) # get first page content
    content = json.loads(page.content)
    try:
        next_page = content['next_page'] # get url for next page
    except:
        next_page = None
    page_data = pd.DataFrame(content['data']).loc[:,['client_count','dump_time']] # get data for this page
    page_data.dump_time = pd.to_datetime(page_data.dump_time) # convert time on page to datetime
    location_df = page_data.set_index('dump_time') # set as index
    location_df.index = location_df.index.rename('t') # rename index
    location_df.columns = ['count']  # rename column
    page_count = 1
    location_df.sort_index(ascending = True, inplace=True)
    while next_page is not None:  # do it until there is no url for a next page
        print 'scraping page ' + str(page_count) + ' for ' + place
        page_count += 1
        (next_df, next_page) = process_page_data(next_page)
        location_df = location_df.append(next_df)  # add each page's data to the total
    return location_df


def scrape_all_historical(place, save_pkl=True):
    current_time = datetime.datetime.now()
    month, yesterday = current_time.month, current_time.day - 1
    spr_2016 = scrape_location_data(place, 2016, 1, 18, 0, 0, 2016, 5, 7, 11, 45)
    fall_2016 = scrape_location_data(place, 2016, 9, 2, 0, 0, 2016, month, yesterday, 11, 45)
    historical_2016 = spr_2016.append(fall_2016)
    historical_2016['weekday'] = historical_2016.index.map(lambda x: dayofweek_int_to_str(x.dayofweek))
    historical_2016.sort_index(inplace=True)
    hist_grouped_weekdays = historical_2016.groupby('weekday')
    historical_weekday_dic = {}
    for weekday, weekday_df in hist_grouped_weekdays:  # bad use mapping
        weekday_df = weekday_df.drop('weekday', 1)
        grouped_dates = weekday_df.groupby(weekday_df.index.date)
        dates_data = {}
        for date, date_df in grouped_dates:   # what is going on here
            date_series = pd.Series(date_df['count'])
            date_series.index = date_series.index.time
            dates_data[date] = date_series
        dates_data = pd.DataFrame(dates_data)
        dates_data.index = dates_data.index.rename('t')
        dates_data = trim_df_by_daily_count(dates_data, place)
        dates_data = trim_df_by_open_hrs(place, dates_data, weekday=weekday)
        historical_weekday_dic[weekday] = dates_data
    if save_pkl:
        joblib.dump(historical_weekday_dic, '../data/historical_dic_' + place + '.pkl')
    return historical_weekday_dic


def filter_weekday_df(weekday_df, lamb):
    weekday_dic = {}
    for date in weekday_df.columns:
        series = weekday_df[date]
        series = hodrick_prescott_filter(series, lamb)
        weekday_dic[date] = series
    filtered_weekday_df = pd.DataFrame(weekday_dic)
    filtered_weekday_df.dropna(axis=1, inplace=True)
    return filtered_weekday_df

    
def check_if_building_open(placename, current_time, start=None, stop=None):
    if start is not None:
        if current_time < start:
            sys.exit('Sorry but it is too early to predict for ' + placename + '.')
    if stop is not None:
        if current_time > stop:
            sys.exit('Sorry but it is too late to predict for ' + placename + '.')


def trim_df_by_daily_count(df, place):
    """ trims a df of many days data by deleting days with less than threshold count """
    min_count_thresholds = {'jjs': 900, 'john_jay': 1800, 'Uris': 1700, 'Science_and_Engineering_Library': 1300}
    try:
        min_count_per_day = min_count_thresholds[place]
    except:
        min_count_per_day = 800
    sum_daily_counts = df.sum().sort_values()
    to_drop = []
    for date in sum_daily_counts.index:
        sum_count = sum_daily_counts.loc[date]
        if sum_count < min_count_per_day:
            to_drop.append(date)
    print 'number of dropped ' + str(len(to_drop))
    new_df = df.drop(to_drop, 1)
    return new_df


def trim_df_by_open_hrs(placename, df, weekday=None):
    """
    trims a dataframe by whether or not the place is open on the weekday
    """
    current_time = datetime.datetime.now()
    if weekday is None:
        weekday = dayofweek_int_to_str(current_time.weekday())
    just_time = current_time.time()
    if placename == "jjs":
        start = datetime.time(12,00)
        check_if_building_open(placename, just_time, start=start)
        mask = (df.index >= start)
        df = df.loc[mask]
    elif placename == "john_jay":
        start = datetime.time(11,00)
        stop = datetime.time(21,00)
        if (weekday == 'Fri') or (weekday == 'Sat'):
            print ' sorry john jay is closed today'
        if weekday== 'Sun':
            start = datetime.time(10,00)
        check_if_building_open(placename, just_time, start=start, stop=stop)
        mask = (df.index >= start) & (df.index <= stop)
        df = df.loc[mask]
    elif placename == "Uris":
        start = datetime.time(8,00)
        stop = datetime.time(23,45)
        if (weekday == 'Sat'):
            start = datetime.time(10,00)
            stop = datetime.time(21,00)
        if (weekday == 'Sun'):
            start = datetime.time(10,00)
            stop = datetime.time(23,00)
        if (weekday == 'Thu'):
            start = datetime.time(10,00)
            stop = datetime.time(23,00)
        if (weekday == 'Fri'):
            start = datetime.time(8,00)
            stop = datetime.time(21,00)
        check_if_building_open(placename, just_time, start=start, stop=stop)
        mask = (df.index >= start) & (df.index <= stop)
        df = df.loc[mask]
    elif placename == "Science_and_Engineering_Library":
        start = datetime.time(9,00)
        if (weekday == 'Sat') or (weekday == 'Sun'):
            start = datetime.time(11,00)
        check_if_building_open(placename, just_time, start=start)
        mask = (df.index >= start)
        df = df.loc[mask]
    else:
        start = datetime.time(7,30)
        mask = (df.index >= start)
        df = df.loc[mask]
    return df


def get_current_data(place):
    """
    gets the (relevant) current data for today for the user's desired place
    """
    current_time = datetime.datetime.now()
    (year, month, day, hour, mins) = (current_time.year, current_time.month, current_time.day, current_time.hour, current_time.minute)
    location_data = scrape_location_data(place, year, month, day, 00, 00, year, month, day, hour, mins - mins%15 + 1)   # sus mod math
    location_data.index = [x.time() for x in location_data.index]
    location_data.columns = [datetime.datetime.now().date()]
    trimmed_current = trim_df_by_open_hrs(place, location_data)
    return trimmed_current

    
def load_relevant_data(place, lamb):
    weekday = dayofweek_int_to_str(datetime.datetime.now().weekday())
    print 'fetching current data...'
    current_data = get_current_data(place)
    try:
        historical_weekday_dic = joblib.load('../data/historical_dic_' + place + '.pkl')
    except:
        historical_weekday_dic = scrape_all_historical(place)
    historical_prior_raw = historical_weekday_dic[weekday]
    historical_prior_filtered = filter_weekday_df(historical_prior_raw, lamb)
    first_measure = historical_prior_filtered.index[0]
    current_data = hodrick_prescott_filter(current_data.loc[first_measure:], lamb)
    return current_data, historical_prior_filtered