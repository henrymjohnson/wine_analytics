from flask import Flask, jsonify, request
import logging
from flask_cors import CORS

from src.data_ingestion.wine_prices import average_wine_prices, ppi
from src.data_ingestion.demographics import population, disposable_income
from src.data_ingestion.trade_flows import imports_panel_data, imports_panel_data_all_countries
from src.data_ingestion.domestic_production import wine_production


app = Flask(__name__, static_folder='/app/src/web', static_url_path='/')
CORS(app, origins='http://localhost:8080')

app.logger.setLevel(logging.ERROR)
handler = logging.StreamHandler()
handler.setLevel(logging.ERROR)
app.logger.addHandler(handler)

@app.route("/info", methods=["GET"])
def information():
    data = {"message": "Information!"}
    return jsonify(data)

@app.route("/prices", methods=["GET"])
def avg_wine_prices():
    data = average_wine_prices.copy()
    data.columns = ['date', 'average_wine_price_us']
    data_dict = data.to_dict(orient='records')
    return jsonify(data_dict)

@app.route("/demographics", methods=["GET"])
def demographics():
    type = request.args.get('type')

    if type == 'population':
        data = population.copy()
        data.columns = ['date', 'population_size']
        data_dict = data.to_dict(orient='records')
        return jsonify(data_dict)
    elif type == 'disposable_income':
        data = disposable_income.copy()
        data.columns = ['date', 'disposable_income_amount']
        data_dict = data.to_dict(orient='records')
        return jsonify(data_dict)
    else:
        return jsonify({"error": "Invalid type parameter"}), 400

@app.route("/production", methods=["GET"])
def get_wine_production():
    df = wine_production.copy()
    df.columns = ['date', 'wine_production_us']
    data_dict = df.to_dict(orient='records')
    return jsonify(data_dict)

@app.route("/trade_flows", methods=["GET"])
def get_trade_flows():
    type = request.args.get('type')

    if type is None:
        df = imports_panel_data[0].copy()
        data_dict = df.to_dict(orient='records')
        return jsonify(data_dict)
    elif type == 'all':
        df = imports_panel_data_all_countries[0].copy()
        data_dict = df.to_dict(orient='records')
        return jsonify(data_dict)
    else:
        return jsonify({"error": "Invalid type parameter"}), 400


@app.route("/", methods=["GET"])
def serve_web():
    return app.send_static_file('index.html')

@app.route("/more_maps", methods=["GET"])
def serve_more_maps():
    return app.send_static_file('more_maps.html')


app.run(host="0.0.0.0", port=8080)
