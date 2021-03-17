import requests
import json
import psycopg2
import sys
from datetime import datetime, timedelta, date, timezone

sys.path.append("..\src")
from database_connection import DatabaseConnection

def get_weather_data(location, unix_date, key):
	url = "https://api.openweathermap.org/data/2.5/onecall/timemachine?lang=fr&units=metric&lat={}&lon={}&dt={}&appid={}"
	url = url.format(location["lat"], location["lon"], unix_date, key)
	response = requests.get(url)

	if response.status_code == 200:
		 return response.json()

	else:
		print("Error while loading data from OpenWeatherMap.", response.status_code)

def get_unix_time(day):
	day_datetime = datetime(int(day.split("-")[0]), int(day.split("-")[1]), int(day.split("-")[2]))
	day_timestamp = day_datetime.replace(tzinfo = timezone.utc).timestamp()
	return int(day_timestamp)

def get_previous_day(number_days):
	previous_day = datetime.today() - timedelta(days = number_days)
	date = str(previous_day).split(" ")
	return date[0]

def convert_timestamp_to_utc(timestamp):
	return datetime.utcfromtimestamp(int(timestamp)).strftime("%Y-%m-%d %H:%M:%S")

def update_database(cursor, connection, json_data):
	print("- Update start -")

	for hourly_weather in json_data["hourly"] :
		try:
			cursor.execute("INSERT INTO public.weather (datetime, weather_station, description, temperature, pressure, humidity, wind_speed, wind_degree) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", 
			(convertToUTC(hourly_weather["dt"]), 
			"17300001", 
			hourly_weather["weather"][0]["description"], 
			hourly_weather["temp"], 
			hourly_weather["pressure"], 
			hourly_weather["humidity"], 
			hourly_weather["wind_speed"], 
			hourly_weather["wind_deg"]))

		except:
			print("Error while inserting hourly weather in the database.")
			
	try:
		cursor.execute("INSERT INTO public.day_context (date, sunrise, sunset, weather_station) VALUES(%s,%s,%s,%s)", 
		(convertToUTC(json_data["current"]["dt"]), 
		convertToUTC(json_data["current"]["sunrise"]).split(" ")[1], 
		convertToUTC(json_data["current"]["sunset"]).split(" ")[1], 
		"17300001"))
	except:
		print("Error while inserting day context in the database.")

	connection.commit()

	print("- Update complete -")

# Settings:
user = "postgres"
password = "root"
host = "127.0.0.1"
port = "5432"
database = "da3t_db"

key = "2a902434b5a204da0e235fa74ec8c19e"
location = {"lat": 46.166,"lon": -1.15}

database_connection = DatabaseConnection(user = user, password = password, host = host, database = database)

if database_connection:
	connection = database_connection.get_connection()
	cursor = connection.cursor()

	day_timestamp = get_unix_time(get_previous_day(1))
	# Get OpenWeatherMap data:
	weather_data = get_weather_data(location, day_timestamp, key)
	# Insert data on the database:
	update_database(cursor, connection, weather_data)

	day_timestamp = get_unix_time(get_previous_day(2))
	weather_data = get_weather_data(location, day_timestamp, key)
	update_database(cursor, connection, weather_data)

	day_timestamp = get_unix_time(get_previous_day(3))
	weather_data = get_weather_data(location, day_timestamp, key)
	update_database(cursor, connection, weather_data)

	day_timestamp = get_unix_time(get_previous_day(4))
	weather_data = get_weather_data(location, day_timestamp, key)
	update_database(cursor, connection, weather_data)

	day_timestamp = get_unix_time(get_previous_day(5))
	weather_data = get_weather_data(location, day_timestamp, key)
	update_database(cursor, connection, weather_data)

	cursor.close()
	connection.close()

else:
	print("Error while connecting to the database.")