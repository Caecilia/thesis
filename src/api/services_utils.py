import requests
import sys
from geopy.distance import geodesic
from flask import jsonify
from datetime import datetime, time, date, timezone

import folium
import matplotlib.pyplot as plt 
import numpy as np
import matplotlib.patches as mpatches

sys.path.append("..\src")
from database_connection import DatabaseConnection


def fetch_google_place_poi(lat, lon, radius, keyword, type, key):
	url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={},{}&radius={}&keyword={}&type={}&key={}"
	url = url.format(lat, lon, radius, keyword, type, key)
	response = requests.get(url)

	if response.status_code == 200:
		json_response = response.json()
		return json_response

	else:
		return {"error": "Connection to Google Place failed{}".format(response.status_code)}

def get_datatourisme_query(type, lat, lon, radius):
	query = """query {
					poi (
						size: %d,
						filters: [
							{isLocatedAt: {schema_geo: { _geo_distance: {lng: "%f", lat: "%f", distance: "%d"}}}},
							{rdf_type: {_eq: "https://www.datatourisme.gouv.fr/ontology/core#%s"}}
						]
					)
					{
						total
						results {
							rdf_type_uri
							rdfs_label {
								value
								lang
							}
							rdfs_comment {
								value
								lang
							}
							hasContact {
								foaf_homepage
								schema_legalName
								schema_logo
								schema_email
								schema_telephone
								schema_givenName
								schema_familyName
							}

							takesPlaceAt {
								startTime
								endDate
								endTime
								startDate
							}

							hasBeenCreatedBy {
								foaf_homepage
								schema_legalName
								schema_logo
								schema_email
								schema_telephone
								schema_givenName
								schema_familyName
							}
							
							isLocatedAt {
								schema_address {
									schema_addressLocality
									schema_postalCode
									schema_streetAddress
								}

								schema_geo {
									schema_longitude
									schema_latitude
								}
							}
						}
					}
				}""" %(10, float(lon), float(lat), int(radius), type)
	return query

def get_address(location, google_key):
	url = "https://maps.googleapis.com/maps/api/geocode/json?latlng={},{}&language=fr&result_type=locality|plus_code&key={}"
	url = url.format(location["lat"], location["lon"], google_key)
	response = requests.get(url)

	if response.status_code == 200:
		return response.json()["results"][0]["address_components"][0]["short_name"]

	else:
		return jsonify({"error": "The address corresponding to the geographical coordinates was not found."})

def get_distance(origin, poi_location):
	origin = (origin["lat"], origin["lon"])
	poi = (poiLocation["lat"], poiLocation["lon"])
	return geodesic(origin, poi).km

def get_identifiers(address):
	url = "https://geodatamine.fr/boundaries/search?text={}"
	url = url.format(address)
	response = requests.get(url)

	if response.status_code == 200:
		identifiers = []

		for json_object in response.json():
			identifiers.append(json_object["id"])

		return identifiers

	else:
		return jsonify({"error": "The address identifier was not found."})

def get_pois(type, identifiers):
	pois = []

	for identifier in identifiers:
		url = "https://geodatamine.fr/data/{}/{}?format=geojson&aspoint=true"
		url = url.format(type, identifier)
		response = requests.get(url)
		
		if response.status_code == 200:
			pois.append(response.json())
		
		else:
			return jsonify({"error": "Geodatamine POI could not be fetched"})

	return pois

def does_poi_exist(id, pois):
	for poi in pois:
		if poi['properties']['osm_id'] == id:
			return True
	return False

## Orchestration functions:
user = "postgres"
password = "root"
host = "127.0.0.1"
port = "5432"
database = "da3t_db"

def check_distance(point_1, point_2, radius):
	p1 = (float(point_1['lat']), float(point_1['lon']))
	p2 = (float(point_2['lat']), float(point_2['lon']))
	if(geodesic(p1, p2).km * 1000 <= radius):
		return True
	else:
		return False

def get_center(points):
	lat = 0
	lon = 0

	for point in points:
		lat = lat + float(point['lat'])
		lon = lon + float(point['lon'])

	center = {'lat': float(lat / len(points)),'lon': float(lon / len(points))}
	return center

def check_time(time_point_1, time_point_2, time):
	if(get_unix_time(time_point_2) - get_unix_time(time_point_1)) >= (time * 60):
		return True
		
	else:
		return False

def get_unix_time(dt):
	dt = str(dt)
	date = dt.split(' ')[0]
	time = dt.split(' ')[1]

	timestamp = datetime(int(date.split('-')[0]), 
						int(date.split('-')[1]), 
						int(date.split('-')[2]), 
						int(time.split(':')[0]), 
						int(time.split(':')[1])).replace(tzinfo = timezone.utc).timestamp()
						
	return int(timestamp)

def trajectory_to_hour(trajectory):
	start_hour = float(str(trajectory[0]['time']).split(' ')[1].split(':')[0])
	start_minute = float(int(str(trajectory[0]['time']).split(' ')[1].split(':')[1]) / 60)
	start = start_hour + start_minute

	end_hour = float(str(trajectory[-1]['time']).split(' ')[1].split(':')[0])
	end_minute = float(int(str(trajectory[-1]['time']).split(' ')[1].split(':')[1]) / 60)
	end = end_hour + end_minute

	trj_start_end = {"start": start, "end": end}
	return trj_start_end

def stop_to_hour(stop):
	start_hour = float(str(stop["start"]).split(' ')[1].split(':')[0])
	start_minute = float(int(str(stop["start"]).split(' ')[1].split(':')[1]) / 60)
	start = start_hour + start_minute

	end_hour = float(str(stop["end"]).split(' ')[1].split(':')[0])
	end_minute = float(int(str(stop["end"]).split(' ')[1].split(':')[1]) / 60)
	end = end_hour + end_minute

	stop_start_end = {"start": start, "end": end}
	return stop_start_end

def time_to_hour(point):
    date = str(point).split(' ')[0]
    time = str(point).split(' ')[1]

    hour = float(time.split(':')[0])
    minute = float(int(time.split(':')[1]) / 60)

    time_hour = hour + minute
    return time_hour

def visualise_orchestration(trajectory, stops_durations, entering_time, trajectory_weather, weathers):
	fig, gnt = plt.subplots()
	gnt.set_ylim(0, 150)
	trajectory_duration = trajectory_to_hour(trajectory)

	start = trajectory_duration["start"] 
	end = trajectory_duration["end"] 

	gnt.set_xlim(start, end)
	gnt.set_xticks(np.arange(start, end, 1))

	plt.xticks(rotation = 90)

	gnt.set_xlabel('Trajectory duration (hour)') 
	gnt.set_ylabel('Orchestration') 
	gnt.set_yticks([10, 40])
	gnt.set_yticklabels(['stops/moves', 'weather'])
	gnt.grid(True)

	# - Stops-moves visualization start -

	# Stops visualization:
	for stop_duration in stops_durations:
		time = stop_to_hour(stop_duration)
		start_stop = time["start"]
		end_stop = time["end"]
		gnt.broken_barh([(start_stop, end_stop - start_stop)], (5, 15), facecolors = ('tab:red'))
		time = None
		start = None
		end = None

	length = len(stops_durations)

	# Moves visualization:
	for i in range(length - 1):
		stop1 = stops_durations[i]
		time1 = stop_to_hour(stop1)
		stop2 = stops_durations[i + 1]
		time2 = stop_to_hour(stop2)

		if time2["start"] > time1["end"]:
			gnt.broken_barh([(time1["end"], time2["start"] - time1["end"])], (5, 15), facecolors =('tab:orange'))
			
	first_stop = stops_durations[0]
	split_first_stop = stop_to_hour(first_stop)
	if trajectory_duration["start"] < split_first_stop["start"]:
		gnt.broken_barh([(trajectory_duration["start"], split_first_stop["start"] - trajectory_duration["start"])], (5, 15), facecolors = ('tab:orange'))

	last_stop = stops_durations[-1]
	split_last_stop = stop_to_hour(last_stop)

	if trajectory_duration['end'] > split_last_stop["end"] :
		gnt.broken_barh([(split_last_stop["end"], trajectory_duration["end"] - split_last_stop["end"])], (5, 15), facecolors = ('tab:orange'))

	# - Stops-moves visualization end -

	# - Weather visualization start -

	database_connection = DatabaseConnection(user, password, host, database)
	connection = database_connection.get_connection()
	cursor = connection.cursor()

	trajectory_date = str(trajectory_weather[0]['time']).split(' ')[0]
	trajectory_date_start = trajectory_date + ' 00:00:00'
	trajectory_date_end = trajectory_date + ' 23:00:00'

	query = "SELECT datetime, description FROM weather WHERE datetime BETWEEN %s AND %s"

	cursor.execute(query, (trajectory_date_start , trajectory_date_end))
	result = cursor.fetchall()

	weather_values = []

	if result :
		for obj in result:
			weather_values.append({'datetime': obj[0], 'description': obj[1]})
			
	start_end_trajectory = trajectory_to_hour(trajectory_weather)
	start_trajectory = int(str(start_end_trajectory['start']).split('.')[0])
	end_trajectory = int(str(start_end_trajectory['end']).split('.')[0])

	weather_values_to_hour = []
	i = 0

	for obj in weather_values :
		time = time_to_hour(obj['datetime'])
		weather_values_to_hour.append({"time": int(time), "description": obj["description"]})
		i = i + 1
	
	colors = ['tab:blue', 'gold', 'tab:green', 'indigo','tab:purple', 'tab:brown', 'tab:pink', 'tab:gray', 'tab:olive', 'tab:cyan','teal']
	i = start_trajectory
	j = end_trajectory
	k = 0

	current_hour = start_trajectory
	end_hour = end_trajectory
	color = 10

	previous_weather = "no_weather"
	current_weather = "no_weather"

	patches = []
	patches_map = {}

	while current_hour <= end_hour:
		if current_hour in weather_values_to_hour:
			current_weather = str(weather_values_to_hour[current_hour]['description'])
			if current_weather != previous_weather:
				color = 0 if (color == 10) else color + 1
				patches_map[colors[color]] = current_weather
					
		gnt.broken_barh([(current_hour, 1)], (30, 25), facecolors = (colors[color]))
		previous_weather = current_weather
		current_hour = current_hour + 1

	for key, value in patches_map.items():
		patch = mpatches.Patch(color = key, label = value)
		patches.append(patch)
		
	plt.legend(handles = patches)
	plt.grid(False)
	plt.tight_layout()

	plt.savefig("static/images/chart.png")