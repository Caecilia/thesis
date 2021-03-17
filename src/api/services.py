import json
import requests
import psycopg2
import sys
from flask import Flask, request, render_template, jsonify

sys.path.append("..\src")
from database_connection import DatabaseConnection
from services_utils import *

user = "postgres"
password = "root"
host = "127.0.0.1"
port = "5432"
database = "da3t_db"

application = Flask("__name__")

database_connection = DatabaseConnection(user, password, host, database)
connection = database_connection.get_connection()
cursor = connection.cursor()

@application.route("/get-weather", methods = ["POST","GET"])
def get_weather():
	if request.method == "GET":
		datetime = request.args.get("datetime")
		station_id = request.args.get("station-id")
		
		datetime = datetime.split('-')[0] + "-" + datetime.split('-')[1] + "-" + datetime.split('-')[2] + " " + datetime.split('-')[3] + ":" + datetime.split('-')[4] + ":" + datetime.split('-')[5]

		if datetime and station_id:
			query = "SELECT * FROM weather WHERE datetime = %s AND weather_station = %s"

			try:
				cursor.execute(query, (datetime, station_id,))
				result = cursor.fetchall()
				
			except:
				return jsonify({"error": "Missing argument or wrong format"})
			
			if result:
				return jsonify({"date": str(result[0][0]).split(" ")[0],
							 "time": str(result[0][0]).split(" ")[1],
							 "weatherstation": result[0][1],
							 "description": result[0][2],
							 "temperature": int(result[0][3]),
							 "pressure": int(result[0][4]),
							 "humidity": int(result[0][5]),
							 "windspeed": int(result[0][6]),
							 "winddeg": int(result[0][7])})
			else:
				return jsonify({"error":"There is no weather for this datetime"})

		else:
			return jsonify({"error":"Missing argument"})

	if request.method == "POST":
		datetime = request.args.get("datetime")
		station_id = request.args.get("station-id")

		datetime = datetime.split('-')[0] + "-" + datetime.split('-')[1] + "-" + datetime.split('-')[2] + " " + datetime.split('-')[3] + ":" + datetime.split('-')[4] + ":" + datetime.split('-')[5]

		if datetime and station_id:
			query = "SELECT * FROM weather WHERE datetime = %s AND weather_station = %s"

			try:
				cursor.execute(query, (datetime, station_id,))
				result = cursor.fetchall()
				
			except:
				return jsonify({"error": "Missing argument or wrong format"})
			
			if result:
				return jsonify({"date": str(result[0][0]).split(" ")[0],
							 "time": str(result[0][0]).split(" ")[1],
							 "weatherstation": result[0][1],
							 "description": result[0][2],
							 "temperature": int(result[0][3]),
							 "pressure": int(result[0][4]),
							 "humidity": int(result[0][5]),
							 "windspeed": int(result[0][6]),
							 "winddeg": int(result[0][7])})
			else:
				return jsonify({"error":"There is no weather for this datetime"})

		else:
			return jsonify({"error":"Missing argument"})

@application.route("/get-day-context",methods=["GET","POST"])  
def get_day_context():
	if request.method == "GET":
		date = request.args.get("date")
		station_id = request.args.get("station-id")

		if date and station_id:
			query = "SELECT * FROM day_context WHERE date = %s AND weather_station = %s"

			try:
				cursor.execute(query, (date, station_id,))
				result = cursor.fetchall()
			except:
				return jsonify({"error": "Missing argument or wrong format"})
			
			if result:
				return  jsonify({"date":str(result[0][0]),
							 "sunrise":str(result[0][1]),
							 "sunset":str(result[0][2])
							})
			else:
				return jsonify({"error":"There is no day context for this date"})

		else:
			return jsonify({"error":"Missing argument"})
	
	if request.method == "POST":
		date = request.args.get("date")
		station_id = request.args.get("station_id")

		if date and station_id:
			query = "SELECT * FROM day_context WHERE date = %s AND weather_station = %s"

			try:
				cursor.execute(query, (date, station_id,))
				result = cursor.fetchall()
			except:
				return jsonify({"error": "Missing argument or wrong format"})
			
			if result:
				return  jsonify({"date":str(result[0][0]),
							 "sunrise":str(result[0][1]),
							 "sunset":str(result[0][2])
							})
			else:
				return jsonify({"error":"There is no day context for this date"})

		else:
			return jsonify({"error":"Missing argument"})

if __name__ == "__main__":
	application.run(debug = True)

