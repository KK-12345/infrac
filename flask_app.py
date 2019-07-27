import pandas as pd
import datetime
from dateutil import tz
from flask import Flask, request, jsonify

app = Flask(__name__)

file_data_frame = None
file_name = 'infrac.csv'


@app.before_first_request
def read_csv():
    global file_data_frame
    file_data_frame = pd.read_csv(file_name, delimiter=',', parse_dates=['build_request_start', 'build_start', 'build_end'],
                                  header=None, names=['uid', 'user_id', 'build_request_start', 'build_start',
                                                      'build_end', 'build_delete_flag', 'exit_code', 'size'])


def builds_for_today():
    global file_data_frame
    df = file_data_frame
    tzone = tz.tzoffset(None, -14400)
    result = df[(df['build_start'] >= datetime.datetime.today().replace(tzinfo=tzone)) & (df['build_end'] <= datetime.datetime.today().replace(tzinfo=tzone))]
    return jsonify({'status': 200, 'result': {'uid_list': list(result['uid']), 'count': int(result.count()['uid'])}})


@app.route('/builds_in_window', methods=['POST'])
def get_builds_in_time_window():
    try:
        data = request.get_json()
        start_date = data.get('start_date', None)
        tzone = tz.tzoffset(None, -14400)
        if start_date:
            start_date = datetime.datetime.strptime(start_date, "%d/%m/%Y").replace(tzinfo=tzone)
        end_date = data.get('end_date', None)
        if end_date:
            end_date = datetime.datetime.strptime(end_date, "%d/%m/%Y").replace(tzinfo=tzone)
    except Exception as e:
        jsonify({'status': 501, 'result': e.result})

    if not start_date and not end_date:
        return builds_for_today()
    global file_data_frame
    df = file_data_frame
    result = df[(df['build_start'] >= start_date) & (df['build_end'] <= end_date)]
    return jsonify({'status': 200, 'result': {'uid_list': list(result['uid']), 'count': int(result.count()['uid'])}})


@app.route('/build_success_rate', methods=['GET'])
def get_build_success_rate():
    global file_data_frame
    df = file_data_frame
    success_rate = 100 * (df[df['exit_code'] == 0].count()['exit_code'] / df['exit_code'].count())
    return jsonify({'percentage_success_rate': success_rate})


@app.route('/top_n_failure_code', methods=['POST'])
def get_top_n_failure_exit_codes():

    try:
        data = request.get_json()
        n = int(data.get('top'))
    except ValueError:
        jsonify({'status': 501, 'result': 'top-n value should be an integer'})
    except Exception as e:
        return jsonify({'status': 501, 'result': e.result})

    global file_data_frame
    df = file_data_frame[file_data_frame['exit_code'] != 0]
    filter_result = df[['exit_code', 'uid']].groupby(['exit_code'])['uid'].count().reset_index(name='count') \
        .sort_values(['count'], ascending=False)
    filter_result = filter_result.head(n)
    return jsonify({'status': 200, 'result': {'exit_codes': list(filter_result['exit_code'])}})


@app.route('/top_n_users', methods=['POST'])
def get_top_n_users():
    try:
        data = request.get_json()
        n = int(data.get('top'))
    except ValueError:
        return jsonify({'status': 501, 'result': 'top-n value should be an integer'})
    except Exception as e:
        return jsonify({'status': 501, 'result': e.result})

    global file_data_frame
    filter_result = file_data_frame[['user_id', 'uid']].groupby(['user_id'])['uid'].count().reset_index(name='count')\
        .sort_values(['count'], ascending=False)
    filter_result = filter_result.head(n)
    return jsonify({'status': 200, 'result': {'uid_list': list(filter_result['user_id'])}})


if __name__ == '__main__':
    app.run()
