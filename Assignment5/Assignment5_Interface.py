#
# Assignment5 Interface
# Name: Abhilash Chaudhary
#

from pymongo import MongoClient
import os
import sys
import json
import re
from math import radians, sqrt, atan2, sin, cos

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    try:
    	f = open(saveLocation1, "w")
    	for record in collection.find({"city": re.compile('^' + re.escape(str(cityToSearch)) + '$', re.IGNORECASE)}):
    		name = record['name']
    		address = record['full_address']
    		city = record['city']
    		state = record['state']
    		location = str(name)+'$' + str(address) + '$' + str(city) + '$' + str(state) + '\n'
    		f.write(location.upper())

    	f.close()
    except Exception as e:
            traceback.print_exc()
 
def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    try:
	    f = open(saveLocation2, "w")
	    lat_1 = myLocation[0]
	    lon_1 = myLocation[1]
	    
	    records = collection.find({"categories": { "$in" : categoriesToSearch}})
	    for record in records:
	    	name = record['name']
	    	lat_2 = record['latitude']
	    	lon_2 = record['longitude']
	    	d = calculateDistanceFunction(lat_2, lon_2, lat_1, lon_1)
	    	if d <= maxDistance:
	    		business = str(name) + '\n'
	    		f.write(business.upper())
	    
	    f.close()
    except Exception as e:
            traceback.print_exc()

def calculateDistanceFunction(lat_2, lon_2, lat_1, lon_1):
    R = 3959
    lat_1 = float(lat_1)
    lat_2 = float(lat_2)
    lon_1 = float(lon_1)
    lon_2 = float(lon_2)
    
    rad_lat_1 = radians(lat_1)
    rad_lat_2 = radians(lat_2)
    dLat = radians(lat_2 - lat_1)
    dLon = radians(lon_2 - lon_1) 
    
    a = sin(dLat/2)*sin(dLat/2) + cos(rad_lat_1)*cos(rad_lat_2) * sin(dLon/2)*sin(dLon/2)
    c = 2*atan2(sqrt(a), sqrt(1-a))
    d = R*c
    
    return d
