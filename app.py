from flask import Flask, request
import sqlite3
import requests
from tqdm import tqdm

from flask import Flask, request
import json 
import numpy as np
import pandas as pd

app = Flask(__name__) 

#####
# Routing home
@app.route('/')
@app.route('/homepage')
def home():
    return 'Hello World'

## Routing GET functions
# Routing all stations
@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

# Routing stations id
@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    station_id = get_station_id(station_id, conn)
    return station_id.to_json()

# Routing all trips
@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()

# Routing trip id
@app.route('/trips/<trip_id>')
def route_trip_id(trip_id):
    conn = make_connection()
    trip_id = get_trip_id(trip_id, conn)
    return trip_id.to_json()

## Routing POST functions
# Routing POST stations
@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

# Routing POST trips
@app.route('/trips/add', methods=['POST'])
def route_add_trips():
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result

# Routing json
@app.route('/json') 
def json_example():
    
    req = request.get_json(force=True)
    
    name = req['name']
    age = req['age']
    address = req['address']
    
    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')

# -- Routing create static endpoints --
@app.route('/trips/average_duration/') 
def route_average_stations():
    conn = make_connection()
    average_time_to_each_stations = get_average_duration(conn)
    return average_time_to_each_stations

# -- Routing create dynamic endpoints --
@app.route('/trips/average_duration/<bike_id>') 
def route_duration_perbike(bike_id):
    conn = make_connection()
    average_time_perbike = get_average_duration_perbike(bike_id, conn)
    return average_time_perbike

# -- Routing POST endpoints  --
@app.route('/trips/period', methods=['POST'])
def route_activities_by_period():
    conn = make_connection()
    input_data = request.get_json(force=True)

    specific_period = input_data['period']
    average_activity_period = get_avg_act(specific_period, conn)

    return average_activity_period



### FUNCTIONS
# Make connections to sql server
def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

conn = make_connection()

## GET
# GET all stations
def get_all_stations(conn):
    query_station_all = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query_station_all, conn)
    return result

# GET stations id
def get_station_id(station_id, conn):
    query_station_id = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query_station_id, conn)
    return result 

# GET all trips
def get_all_trips(conn):
    query_trips_all = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query_trips_all, conn)
    return result

# GET trip id
def get_trip_id(trip_id, conn):
    query_trip_id = f"""SELECT * FROM trips WHERE id = {trip_id}"""
    result = pd.read_sql_query(query_trip_id, conn)
    return result

## POST
# POST stations
def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

# POST trips
def insert_into_trips(data_trips, conn):
    query_trips = f'''INSERT INTO trips values {data_trips}'''
    try:
        conn.execute(query_trips)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

# -- Query static end points --
def get_average_duration (conn):
    query_average_duration = f'''SELECT * FROM trips'''
    result = pd.read_sql_query(query_average_duration, conn)
    result_avg = pd.pivot_table(
                    data=result,
                    index='end_station_name',
                    values='duration_minutes',
                    aggfunc='mean'
                    )
    return result_avg.to_json()

# -- Query dynamic end points --
def get_average_duration_perbike(bike_id, conn):
    query_bike_id = f"""SELECT * FROM trips WHERE bikeid = {bike_id}"""
    result = pd.read_sql_query(query_bike_id, conn, parse_dates='start_time')
    result['days']=result['start_time'].dt.day_name()
    result_avg = pd.pivot_table(
                    data=result,
                    index='days',
                    values='duration_minutes',
                    aggfunc='mean'
                    )
    return result_avg.to_json()

# -- Query POST end points --
def get_avg_act(specific_period, conn):
    query_act = f''' SELECT * FROM trips WHERE start_time LIKE '{specific_period}%' '''
    selected_data = pd.read_sql_query(query_act, conn)
    result = selected_data.groupby('subscriber_type').agg({
    'bikeid' : 'count', 
    'start_station_name' : 'nunique', 
    'end_station_name' : 'nunique', 
    'duration_minutes' : 'mean'})

    return result.to_json()

#####

if __name__ == '__main__':
    app.run(debug=True, port=5000)