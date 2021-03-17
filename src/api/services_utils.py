import requests
from geopy.distance import geodesic
from flask import jsonify

def fetch_google_place_poi(lat, lon, radius, keyword, type, key):
	url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={},{}&radius={}&keyword={}&type={}&key={}"
	url = url.format(lat, lon, radius, keyword, type, key)
	response = requests.get(url)

	if response.status_code == 200:
		json_response = response.json()
		return json_response

	else:
		return {"error": "Connection to Google Place failed{}".format(response.status_code)}

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
