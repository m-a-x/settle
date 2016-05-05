''' TODO:
- MARK WHEN CURVE DEPARTS FROM REST OF GROUP -> THAT PERIODICITY OF LACK OF RELEVANCE -> TRAIN ONLY FROM RELEVANT DATA
- put labels on graphs ffs
- include avery and whatever
- only use reading week training data during reading week
 '''
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import analysis_helper as helper
from sklearn.svm import SVR
import easygui as eGUI


def forecast_master(current,weekday_prior, n_predictions):
    '''
    Runs an individual forecast for a set of current and prior data
    (lowest layer)
    '''
    to_predict = np.array(current).T
    next_to_predict = list(to_predict[0]) # init next array of predictions
    for i in range(0, n_predictions): # for each prediction in the future
        filtered_X = []
        next_X = []
        for old_date in weekday_prior.columns: # for each day from this sem. that is the same wkday as today
            filtered_X.append(weekday_prior[old_date].iloc[:len(to_predict[0])]) # get the same chunk of data that is currently avail. for today (~X)
            next_X.append(weekday_prior[old_date].iloc[len(to_predict[0])]) # get the historical datapoint after that, the one we're tryna guess (~y) 
        clf = SVR(kernel = 'linear', C=.0008) # set up Support Vector Regression
        clf.fit(filtered_X, next_X) # do the hard brain work of fitting the model
        next_to_predict.append(clf.predict(to_predict)[0]) # predict the next point ##################### ??????????????????????????????
        next_X.append(weekday_prior[old_date][-1]) #????????????????????????
        to_predict = np.array([next_to_predict])
    return pd.Series(to_predict[-1], index=weekday_prior.index[:len(to_predict[-1])])

  
def pred(n_predictions, n_earlier_predictions, place, color, data_path, lamb):
    '''
    runs all of the users choices of most recent and past predictions
    (middle layer)
    '''
    current, weekday_prior = helper.load_all_data(place, 'spr_2016', n_predictions, data_path, lamb)
    curf = helper.hodrick_prescott_filter(current, lamb) #######################
    cur_to_plot = current
    predicted = forecast_master(curf,weekday_prior, n_predictions)
    pred_to_plot = predicted
    if n_earlier_predictions > 0:
        for i in range(1, n_earlier_predictions + 1):
            n_sub_preds = int(n_predictions) + i
            ind = len(current.index) - i
            curf, all_pr = helper.load_all_data(place, 'spr_2016', n_sub_preds, data_path, lamb, current_data=curf)
            predicted = forecast_master(curf[:ind], weekday_prior, n_sub_preds)
            plt.plot(predicted.index[ind-1:ind + n_sub_preds], predicted.values[ind-1:ind + n_sub_preds], color=color, linewidth=.9 - (float(i) -1)/(n_earlier_predictions + 6), linestyle='dashed')
            print 'plotted'
    plt.plot(pred_to_plot.index[-n_predictions-1:], pred_to_plot.values[-n_predictions-1:], color=color, linewidth=1.2, linestyle='dashed', label = 'Predicted')
    plt.scatter(pred_to_plot.index[-n_predictions-1:], pred_to_plot.values[-n_predictions-1:], color='black', label = 'Predicted')
    plt.plot(cur_to_plot.index, cur_to_plot.values, color='blue', linewidth=2., label = 'Today')
    historical = weekday_prior[weekday_prior.columns[:9]].iloc[:len(predicted.index)]
    plt.plot(historical.index, historical.values, linewidth=.2,color='blue', linestyle=':')


def run_application():
    '''
    main application that runs all user interaction/scraping/prediction functions
    (highest layer)
    '''
    data_path = eGUI.diropenbox(msg='Where do you want to load/save data')
    data_path = data_path +"/"
    msg ="Forecast Dining Halls or Libraries?"
    title = "Predict your Future"
    choices = ["Dining Halls", "Libraries"]
    choice = eGUI.choicebox(msg, title, choices)
    msg = "About how many minutes into the future shall I predict? (<300)"
    integer = eGUI.integerbox(msg, title, default = '30', upperbound = 300)
    n_predictions = int(integer)/15
    msg = "How many earlier predictions shall I show?"
    n_earlier_predictions = int(eGUI.integerbox(msg, title, default = '4',upperbound=40))
    if n_predictions == 0:
        n_predictions = 1
    if choice == "Libraries":
        msg ="Which library do you want to explore?"
        title = "Where 2 Study"
        places = ["All Butler", "Science_and_Engineering_Library", "Uris", "Butler_Library_2", "Butler_Library_3", "Butler_Library_4", "Butler_Library_5", "Butler_Library_6"]
        place = eGUI.choicebox(msg, title, places)
    else:
        msg ="Which dining hall do you want to explore?"
        title = "Where 2 Eat"
        places = ["john_jay", "jjs"]
        place = eGUI.choicebox(msg, title, places)
    if place == "All Butler":
        i = 0
        colors = ['red', 'orange', 'green', 'purple', 'pink']
        for lib in [ "Butler_Library_2", "Butler_Library_3", "Butler_Library_4", "Butler_Library_5", "Butler_Library_6"]:
            pred(n_predictions, n_earlier_predictions, lib, colors[i], data_path, .5)
            i += 1
    else:
        pred(n_predictions, n_earlier_predictions, place, 'purple', data_path, .5)
    plt.title( 'Current and Predicted Occupancy for ' + place + ' Today')
    plt.ylabel('users connected')
    plt.xlabel('time')
    plt.show()


run_application()
