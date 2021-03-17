import json
import requests
import psycopg2
import sys
import os
from flask import Flask, request, render_template, jsonify

sys.path.append("..\src")
from database_connection import DatabaseConnection
from services_utils import *

# Settings:
user = "postgres"
password = "root"
host = "127.0.0.1"
port = "5432"
database = "da3t_db"
google_key = ""
datatourisme_url = "http://localhost:8080/"

application = Flask("__name__")

database_connection = DatabaseConnection(user, password, host, database)
connection = database_connection.get_connection()
cursor = connection.cursor()

images_folder = os.path.join('static' , 'images')
application.config['UPLOAD_FOLDER'] = images_folder
application.config['TEMPLATES_AUTO_RELOAD'] = True

# Service to get a hourly weather from the database:
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
				return jsonify({"error": "There is no weather for this datetime"})

		else:
			return jsonify({"error": "Missing argument"})

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
				return jsonify({"error": "There is no weather for this datetime"})

		else:
			return jsonify({"error": "Missing argument"})

# Service to get the daily context from the database:
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
				return jsonify({"error": "There is no day context for this date"})

		else:
			return jsonify({"error": "Missing argument"})
	
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
				return jsonify({"error": "There is no day context for this date"})

		else:
			return jsonify({"error": "Missing argument"})

# Service to get POI from Google Place:
# Example of testing arguments: type=airport&lat=46.1558&lon=-1.1532&radius=3000
@application.route("/get-google-place-poi", methods = ["POST", "GET"])
def get_google_place_poi():
	if request.method == "GET":
		type = request.args.get("type")
		lat = request.args.get("lat")
		lon = request.args.get("lon")
		radius = request.args.get("radius")

		if type and lat and lon and radius:
			return fetch_google_place_poi(lat, lon, radius, type, type, google_key)

		else:
			return jsonify({"error": "Missing argument"})

	if request.method == "POST":
		type = request.args.get("type")
		lat = request.args.get("lat")
		lon = request.args.get("lon")
		radius = request.args.get("radius")

		if type and lat and lon and radius:
			return fetch_google_place_poi(lat, lon, radius, type, type, google_key)

		else:
			return jsonify({"error": "Missing argument"})

# Service to get POI from Datatourisme:
# Example of testing arguments: type=EntertainmentAndEvent&lat=46.1558&lon=-1.1532&radius=30000
# The server containing the Datatourism data must be set up.
@application.route("/get-datatourisme-poi", methods = ["POST","GET"])
def get_datatourisme_poi():
	if request.method =="GET":
		type = str(request.args.get("type"))
		lat = request.args.get("lat")
		lon = request.args.get("lon")
		radius = float(request.args.get("radius"))

		if type and lat and lon and radius:
			radius = float(radius / 1000)
			
			datatourisme_request = requests.post(datatourisme_url, json = {"query": get_datatourisme_query(type = type, lat = lat, lon = lon, radius = radius)})
			return datatourisme_request.json()
			
		else:
			return jsonify({"error": "Missing argument"})

	if request.method == "POST":
		type = str(request.args.get("type"))
		lat = request.args.get("lat")
		lon = request.args.get("lon")
		radius = float(request.args.get("radius"))

		if type and lat and lon and radius:
			radius = float(radius / 1000)
			
			datatourisme_request = requests.post(datatourisme_url, json = {"query": get_datatourisme_query(type = type, lat = lat, lon = lon, radius = radius)})
			return datatourisme_request.json()
			
		else:
			return jsonify({"error": "Missing argument"})

# Service to get POI from Geodatamine:
# Example of testing arguments: type=historic&lat=46.1558&lon=-1.1532&radius=3000
# Resources:
# - https://geodatamine.fr/boundaries/search?text=la%20rochellle
# - https://geodatamine.fr/themes
# - https://www.gps-coordinates.net/api
@application.route("/get-geodatamine-poi", methods = ["GET", "POST"])
def get_geodatamine_poi():
	if request.method =="GET":
		lat = request.args.get("lat")
		lon = request.args.get("lon")
		type = request.args.get("type")
		radius = float(request.args.get("radius"))
		
		if type and lat and lon and radius:
			location = {"lat": lat, "lon": lon}

			address = get_address(location, google_key)
			id = get_identifiers(address)
			pois = get_pois(type, id)
			
			pois = []
			final_pois = []

			for poi in pois:
				for poi_features in poi["features"]:
					poi_lat = poi_features["geometry"]["coordinates"][1] 
					poi_lon = poi_features["geometry"]["coordinates"][0]
					locationPoi = {"lat": poi_lat , "lon": poi_lon}
					distance = get_distance(location , locationPoi)

					if distance <= float(radius / 1000):
						if does_poi_exist(poi_features["properties"]["osm_id"], final_pois) == False:
							final_pois.append(poi_features)
						
			if final_pois :
				return jsonify(final_pois)

			else :
				return jsonify({"error": "There is no result."})
		else:
			return jsonify({"error": "Missing argument"})

	if request.method == "POST":
		lat = request.args.get("lat")
		lon = request.args.get("lon")
		type = request.args.get("type")
		radius = float(request.args.get("radius"))
		
		if type and lat and lon and radius:
			location = {"lat": lat, "lon": lon}

			address = get_address(location, google_key)
			id = get_identifiers(address)
			pois = get_pois(type, id)
			
			pois = []
			final_pois = []

			for poi in pois:
				for poi_features in poi["features"]:
					poi_lat = poi_features["geometry"]["coordinates"][1] 
					poi_lon = poi_features["geometry"]["coordinates"][0]
					locationPoi = {"lat": poi_lat , "lon": poi_lon}
					distance = get_distance(location , locationPoi)

					if distance <= float(radius / 1000):
						if does_poi_exist(poi_features["properties"]["osm_id"], final_pois) == False:
							final_pois.append(poi_features)
						
			if final_pois :
				return jsonify(final_pois)

			else :
				return jsonify({"error": "There is no result."})
		else:
			return jsonify({"error": "Missing argument"})

# Service to get a visualization of the orchestration of the weather enrichment and the stops-moves detection services : To be revised !
# This service MUST be improved and generalized !
@application.route('/see-orchestration', methods = ['GET', 'POST'])
def see_orchestration():
	if request.method == 'GET' or request.method == 'POST' :
		trajectory_number = request.args.get('trajectory')
		radius = request.args.get('radius')
		time = request.args.get('time')
		station_id = str(request.args.get('station-id'))
		
		if trajectory_number and radius and time and station_id :
			query1 = "SELECT trajectory, time, ST_X(location::geometry), ST_Y(location::geometry) FROM point where trajectory = %s"
			query2 = "SELECT trajectory, time FROM point WHERE trajectory = %s"
			query3 = "SELECT datetime, weather_station, description FROM weather WHERE weather_station = %s"

			try:
				cursor.execute(query1, (trajectory_number,))
				result1 = cursor.fetchall()
				cursor.execute(query2, (trajectory_number,))
				result2 = cursor.fetchall()
				cursor.execute(query3, (station_id,))
				result3 = cursor.fetchall()

			except:
				return jsonify({'error': 'This trajectory does not exist.'})

			if result1 and result2 and result3 :
				trajectory = []
				i = 1

				for obj in result1:
					trajectory.append({"trajectory": obj[0],
												"time": obj[1],
												"lon": obj[2],
												"lat": obj[3],
												"number": i})
					i = i + 1

				# - Stops-moves algo start -
				points = []
				stops = []
				stops_durations = []

				i = 0
				j = 0

				while i < len(trajectory) - 1:
					j = i + 1

					while j < len(trajectory) and check_distance(trajectory[i], trajectory[j], float(radius)) == True :
						points.append(trajectory[j])
						j = j + 1

					if len(points) >= 2:
						center = get_center(points)
						points.insert(0, trajectory[i])

						if check_time(points[0]['time'], points[-1]['time'], float(time)) == True :
							stops.append(center)  
							stops_durations.append({"start": points[0]['time'], "end": points[-1]['time']})
							
					i = i + j
					j = 0
					points = []
				# - Stops-moves algo end -

				# - Weather algo start -
				trajectorys_weather = []
				weathers = []
				i = 1

				for obj2 in result2:
					trajectorys_weather.append({"trajectoire": obj2[0],
												"time": obj2[1],
												"num": i})
					i = i + 1

				j = 1

				for obj3 in result3:
					weathers.append({"datetime": obj3[0],
									"station": obj3[1],
									"description": obj3[2]})
					j = j + 1
				# - Weather algo end -

				# The service should be only this part:
				if len(stops_durations) > 0:
					visualise_orchestration(trajectory, stops_durations, float(time), trajectorys_weather, weathers)
					full_filename = os.path.join(application.config['UPLOAD_FOLDER'] , 'chart.png')
					return render_template("chart.html", user_image = full_filename)
			
				return jsonify(stops_durations)

			else:
				return jsonify({"error": "There is no result from queries."})

		else:
			return jsonify({"error": "Missing argument"})

if __name__ == "__main__":
	application.run(debug = True)