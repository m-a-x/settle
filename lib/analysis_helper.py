import requests as req
import json
import pandas as pd
import datetime
import statsmodels.api as sm
import sys


def process_page_data(url):
    '''
    -gets the next page in the Density API and extracts time and number of people
    connected to wifi for that page.
    -returns tuple of (page data as dataframe, next page url)
    '''
    page = req.get(url)
    content = json.loads(page.content) # load API's json content
    page_df = pd.DataFrame(content['data']).loc[:,['client_count','dump_time']]
    page_df.dump_time = pd.to_datetime(page_df.dump_time)
    page_df = page_df.set_index('dump_time') # get num on wifi and time of measure
    page_df.index = page_df.index.rename('t')
    page_df.columns = ['count']
    next_page = content['next_page'] # get url for next page
    return (page_df, next_page)


def scrape_location_data(location_name, yr_1, m_1, d_1, h_1, min_1, yr_f, m_f, d_f, h_f, min_f, output_path=None):
    '''
    -for a building's location id (provided by ADI Density) and
    starting year, month, day, hour (HH), minute (MM) (24 hour time) and
    ending year, month, day, hour (HH), minute (MM) (24 hour time)
    returns a dataframe with the index datetime of measure and value count people on wifi
    '''
    building_to_location_id_dic = {'john_jay': '125','jjs': '155', "Butler_Library_4":'132', "Butler_Library_5":'133', "Butler_Library_6":'134', "Science_and_Engineering_Library":'145', "Uris":'23', "Butler_Library_2":'130', "Butler_Library_3":'131'}
    t_1 = str(yr_1) + '-' + str(m_1) + '-' + str(d_1) + 'T' + str(h_1) + ':' + str(min_1)
    t_f = str(yr_f) + '-' + str(m_f) + '-' + str(d_f) + 'T' + str(h_f) + ':' + str(min_f)
    yr_str = str(yr_1)
    if m_f >8:
        sem = 'fall'
    else:
        sem = 'spr'
    url = "http://density.adicu.com/window/" + t_1 + "/" + t_f + "/group/" + building_to_location_id_dic[location_name] + "?auth_token=HMOPZA257UIFUJKIDTAKAV0F1D6CG23A"
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
    location_df.columns = ['count'] # rename column
    page_count = 1
    location_df.sort_index(ascending = True, inplace=True)
    while next_page is not None: # do it until there is no url for a next page
        print 'scraping page ' + str(page_count) + ' for ' + location_name
        page_count += 1
        (next_df, next_page) = process_page_data(next_page)
        location_df = location_df.append(next_df) # add each page's data to the total
    if output_path is not None:
        location_df.to_csv(output_path  + location_name + '_' + sem + '_' + yr_str + '.csv')
    return location_df


def df_to_weekday_dic(df):
    ''' convert dataframe of data to a dic of form: {weekday: weekday_df} '''
    weekday_col = []
    weekday_dic = {}
    for dayofweek_int in df.index.dayofweek:
        weekday_col.append(dayofweek_int_to_str(dayofweek_int))
    df['weekday'] = pd.Series(weekday_col, index=df.index)
    df = df.sort_index()
    grouped_day = df.groupby('weekday')
    for weekday, weekday_df in grouped_day:
        weekday_df = weekday_df.drop('weekday', 1)
        weekday_dic[weekday] = weekday_df
    return weekday_dic


def dayofweek_int_to_str(dayofweek):
    day_num_to_str = { 0: 'Mon', 1: 'Tue', 2: 'Wed', 3: 'Thur', 4: 'Fri', 5: 'Sat', 6: 'Sun'}
    return day_num_to_str[dayofweek]


def check_if_building_open(placename, current_time, sta=None, stp = None):
    if sta is not None:
        if current_time < sta:
            sys.exit('Sorry but it is too early to predict for ' + placename + '.')
    if stp is not None:
        if current_time > stp:
            sys.exit('Sorry but it is too late to predict for ' + placename + '.')


def trim_df_by_open_hrs(placename, df, weekday=None):
    current_time = datetime.datetime.now()
    if weekday is None:
        weekday = dayofweek_int_to_str(current_time.weekday())
    just_time = current_time.time()
    if placename == "jjs":
        start = datetime.time(11,45)
        check_if_building_open(placename, just_time, sta=start)
        mask = (df.index >= start)
        df = df.loc[mask]
    elif placename == "john_jay":
        start = datetime.time(10,45)
        stop = datetime.time(21,15)
        if (weekday == 'Fri') or (weekday == 'Sat'):
            sys.exit(' sorry john jay is closed today')
        if weekday== 'Sun':
            start = datetime.time(9,45)
        check_if_building_open(placename, just_time, sta=start, stp = stop)
        mask = (df.index >= start) & (df.index <= stop)
        df = df.loc[mask]
    elif placename == "Uris":
        start = datetime.time(7,45)
        stop = datetime.time(23,45)
        if (weekday == 'Sat'):
            start = datetime.time(9,45)
            stop = datetime.time(21,15)
        if (weekday == 'Sun'):
            start = datetime.time(9,45)
            stop = datetime.time(23,15)
        if (weekday == 'Thu'):
            start = datetime.time(9,45)
            stop = datetime.time(23,15)
        if (weekday == 'Fri'):
            start = datetime.time(7,45)
            stop = datetime.time(21,15)
        check_if_building_open(placename, just_time, sta=start, stp = stop)
        mask = (df.index >= start) & (df.index <= stop)
        df = df.loc[mask]
    elif placename == "Science_and_Engineering_Library":
        start = datetime.time(8,45)
        if (weekday == 'Sat') or (weekday == 'Sun'):
            start = datetime.time(10,45)
        check_if_building_open(placename, just_time, sta=start)
        mask = (df.index >= start)
        df = df.loc[mask]
    else:
        start = datetime.time(6,45)
        mask = (df.index >= start)
        df = df.loc[mask]
    return df


def trim_df_by_daily_count(df, count_threshold):
    ''' trims a df of many days data by deleting days with less than threshold count '''
    sum_count_series = df.sum().sort_values()
    to_drop = []
    for date in sum_count_series.index:
        sum_count = sum_count_series.loc[date]
        if sum_count < count_threshold:
            to_drop.append(date)
    print 'number of dropped ' + str(len(to_drop))
    new_df = df.drop(to_drop, 1)
    return new_df


def trim_big_dic_by_open_hrs(placename, weekday_series_dic):
    min_count_thresholds = {'jjs': 900, 'john_jay': 1800, 'Uris': 1700, 'Science_and_Engineering_Library': 1300 }
    try:
        min_count_per_day = min_count_thresholds[placename]
    except:
        min_count_per_day = 900
    trimmed_dic = {}
    for wkday, date_series_df in weekday_series_dic.items():
            trimmed_df = trim_df_by_open_hrs(placename, date_series_df, weekday=wkday)
            trimmed_dic[wkday] = trim_df_by_daily_count(trimmed_df, min_count_per_day)
    return trimmed_dic

    
def count_series_by_weekday_dic(placename, semester, data_path, df = None):
    if df is None:
        try:
            df = pd.read_csv(data_path + placename + '_' + semester + '.csv', index_col='t')
        except:
            print 'hah' ############
    df.index = pd.to_datetime(df.index)
    weekday_dic = df_to_weekday_dic(df)
    weekday_series_dic = {}
    for weekday, weekday_df in weekday_dic.items():
        date_series_dic = {}
        grouped_date = weekday_df.groupby(weekday_df.index.date)
        for date, date_df in grouped_date:
            date_series = pd.Series(date_df['count'])
            date_series.index = date_series.index.time
            date_series_dic[date] = date_series
        date_series_df = pd.DataFrame(date_series_dic)
        date_series_df.index = date_series_df.index.rename('t')
        weekday_series_dic[weekday] = date_series_df
    return weekday_series_dic


def read_filtered_csv(placename, semester, data_path, lamb):
    try:
        filtered = pd.read_csv(data_path + 'filtered_' + placename + '_' + semester + '.csv', index_col = 't')
        print 'file already found, not writing filtered for ' + placename + ' ' + semester
        filtered.columns = pd.to_datetime(filtered.columns)
        filtered.columns = [x.date() for x in filtered.columns]
    except:
        try:
            semester_data = pd.read_csv(data_path + placename + '_' + semester + '.csv', index_col = 't')
        except:
            scrape_location_data(placename, 2016,1,18,0,0,2016,4,30,11,45,output_path=data_path)
            semester_data = pd.read_csv(data_path + placename + '_' + semester + '.csv', index_col = 't')
        semester_data_dic = count_series_by_weekday_dic(placename, semester, data_path, df=semester_data)
        semester_data_dic = trim_big_dic_by_open_hrs(placename, semester_data_dic)
        filtered = write_filtered(placename, semester, data_path, lamb, weekday_series_dic=semester_data_dic)
        print 'no preexisting file found, wrote filtered for ' + placename + ' ' + semester
    grouped_filtered = filtered.groupby(lambda x: x.weekday(), axis=1)
    filtered_weekday_dic = {}
    try:
        for weekday, weekday_filtered_df in grouped_filtered:
            weekday_filtered_df.index = pd.to_datetime(weekday_filtered_df.index)
            weekday_filtered_df.index = [x.time() for x in weekday_filtered_df.index]
            filtered_weekday_dic[dayofweek_int_to_str(weekday)] = weekday_filtered_df
    except:
        try:
            for weekday, weekday_filtered_df in grouped_filtered:
                filtered_weekday_dic[dayofweek_int_to_str(weekday)] = weekday_filtered_df
        except:
            filtered_weekday_dic = read_filtered_csv(placename, semester, data_path, lamb)
    return filtered_weekday_dic


def write_filtered(placename, semester, data_path, lamb, weekday_series_dic=None):
    if weekday_series_dic is None:
        weekday_series_dic = count_series_by_weekday_dic(placename, semester, data_path)
        weekday_series_dic = trim_big_dic_by_open_hrs(placename, weekday_series_dic)
    filtered_series_dic = hp_filter_weekday_dic(weekday_series_dic, lamb)
    df = pd.concat(filtered_series_dic.values(), axis = 1)
    df = df.reindex_axis(sorted(df.columns), axis=1)
    try:
        pd.read_csv(data_path + 'filtered_' + placename + '_' + semester + '.csv')
        print 'file already found, not writing filtered for ' + placename + ' ' + semester
    except:
        df.to_csv(data_path + 'filtered_' + placename + '_' + semester + '.csv')
        print 'no preexisting file found, wrote filtered for ' + placename + ' ' + semester
    return df


def hodrick_prescott_filter(date_series, l):
    ''' calculates hodrick_prescott trend filter on a series '''
    filtered_series = sm.tsa.filters.hpfilter(date_series, lamb=l)[1]
    return filtered_series


def hp_filter_weekday_dic(weekday_dic, lamb):
    ''' hp filters a dic of form {weekday: df} and drops axis with less than 15 vals '''
    filtered_dic = {}
    for weekday, date_series_df in weekday_dic.items():
        weekday_dic = {}
        for date in date_series_df.columns:
            series = date_series_df[date]
            series = hodrick_prescott_filter(series, lamb)
            weekday_dic[date] = series
        weekday_df = pd.DataFrame(weekday_dic)
        filtered_dic[weekday] = weekday_df.dropna(axis=1)
    return filtered_dic


def get_current_data(placename):
    current_time = datetime.datetime.now()
    (year, month, day, hour, mins) = (current_time.year, current_time.month, current_time.day, current_time.hour, current_time.minute)
    location_data = scrape_location_data(placename, year, month, day, 00, 00, year, month, day, hour, mins - mins%15 + 1)
    location_data.index = [x.time() for x in location_data.index]
    location_data.columns = [datetime.datetime.now().date()]
    trimmed_current = trim_df_by_open_hrs(placename, location_data)
    return trimmed_current


def load_all_data(placename, semester, n_predictions, data_path, lamb, current_data=None):
    weekday = dayofweek_int_to_str(datetime.datetime.now().weekday())
    if current_data is None:
        print 'starting scraping'
        current_data = get_current_data(placename)
        print 'done scraping'
    all_prior = read_filtered_csv(placename, semester, data_path, lamb)
    weekday_prior = all_prior[weekday]
    last_measure = current_data.index[-1]
    first = weekday_prior.index[0]
    trimmed_current = current_data.loc[first:last_measure]
    return trimmed_current, weekday_prior
