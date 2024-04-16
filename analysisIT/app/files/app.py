from flask import Flask, render_template, request, jsonify
from datetime import datetime, timedelta
import worker
import chartBuilder
import processing
import matplotlib.pyplot as plt
import io
import base64

app = Flask(__name__)


@app.route('/')
def index():
    data_frame = processing.read_data()
    if data_frame is not None:
        count_of_data = data_frame.shape[0]
    else:
        count_of_data = 0
    return render_template('index.html', data_count=count_of_data)


@app.route('/parse', methods=['POST'])
def parse_handler():
    """
    Gets the time interval from the form and runs the parser
    """
    time_interval_in_seconds = request.form
    start_interval_in_seconds = int(time_interval_in_seconds['start_interval'])
    end_interval_in_seconds = int(time_interval_in_seconds['end_interval'])
    arg = get_arguments_for_parser(start_interval_in_seconds, end_interval_in_seconds)
    pr = worker.Worker(*arg)
    pr.run()

    data_frame = processing.get_dataframe_from_db()
    count_of_data = data_frame.shape[0]
    return f'Parsing finished. Data count: {count_of_data}'


@app.route('/get_data_count', methods=['GET'])
def get_data_count():
    data_frame = processing.read_data()
    if data_frame is not None:
        count_of_data = data_frame.shape[0]
    else:
        count_of_data = 0

    return jsonify({'data_count': count_of_data})


def get_arguments_for_parser(left_timedelta, timedelta_in_seconds):
    """
    Creates a time interval rounded to the nearest five minutes
    """
    date_from = datetime.now() - timedelta(seconds=left_timedelta)
    rounded_date_from = (date_from - timedelta(minutes=date_from.minute % 5)).replace(second=0, microsecond=0)
    rounded_date_to = rounded_date_from - timedelta(seconds=timedelta_in_seconds)
    arguments_for_parser = [rounded_date_from, rounded_date_to]
    return arguments_for_parser


def build_graph(graph_type):
    chart_functions = {
        "chart_1": chartBuilder.show_chart_1,
        "chart_2": chartBuilder.show_chart_2,
        "chart_3": chartBuilder.show_chart_3,
        "chart_4": chartBuilder.show_chart_4,
        "chart_5": chartBuilder.show_chart_5,
        "chart_6": chartBuilder.show_chart_6
    }
    if graph_type in chart_functions:
        chart_functions[graph_type]()
    else:
        return None
    buffer = io.BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    image_png = buffer.getvalue()
    buffer.close()

    graph_url = base64.b64encode(image_png).decode('utf-8')
    return 'data:image/png;base64,' + graph_url


@app.route('/plot', methods=['POST'])
def plot():
    data = request.get_json()
    graph_type = data['graph']
    graph = build_graph(graph_type)
    return jsonify(graph)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
