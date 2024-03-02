
import json
import os
import re
import asyncio
import nest_asyncio
nest_asyncio.apply()
from datetime import datetime
from pyensign.ensign import authenticate, publisher, subscriber
from datetime import datetime
from collections import Counter
import time

'''
Create an Ensign_Creds json file that contains your ensign creds in the below format
{
  "ClientID" : "YOUR CLIENT ID"
  "ClientSecret" : "YOUR CLIENT SECRET"
}

ENSIGN_CREDS_PATH = 'Ensign_Creds.json'
'''
'''
Function that take a metro event and the entire weather events array. It takes the date of the
metro event and finds the average elements in the weather dictionaries that match the same
date and merges that data into it
'''
def merge_data(Weather_arr, Metro_event):
    if len(Metro_event) == 0:
        return False
    print(Metro_event)
    conditionYear = Metro_event['date_updated'].year
    condidtionMonth = Metro_event['date_updated'].month
    conditionDay = Metro_event['date_updated'].day
    
    temp = [d['temperature'] for d in Weather_arr if (d['start'].year == conditionYear and d['start'].month == condidtionMonth and d['start'].day == conditionDay)]
    avg_temp = sum(temp)/len(temp)
    
    humidity = [d['humidity'] for d in Weather_arr if (d['start'].year == conditionYear and d['start'].month == condidtionMonth and d['start'].day == conditionDay)]
    avg_humidity = sum(humidity)/len(humidity)
    
    precipitation_prob = [d['precipitation prob'] for d in Weather_arr if (d['start'].year == conditionYear and d['start'].month == condidtionMonth and d['start'].day == conditionDay)]
    for i in range(0,len(precipitation_prob)):
        if precipitation_prob[i] == None:
            precipitation_prob[i] = 0
    avg_precipitation_prob = sum(precipitation_prob)/len(precipitation_prob)
    
    summary = [d['summary'] for d in Weather_arr if (d['start'].year == conditionYear and d['start'].month == condidtionMonth and d['start'].day == conditionDay)]
    summary_count = Counter(summary)
    most_common_summary = summary_count.most_common(1)
    element, count_of_el = most_common_summary[0]
    
    windspeed = [d['wind speed'] for d in Weather_arr if (d['start'].year == conditionYear and d['start'].month == condidtionMonth and d['start'].day == conditionDay)]
    windSpeed_arr = []
    for inst in windspeed:
        speed = re.findall(r'\d+', inst)
        for s in speed:
            windSpeed_arr.append(int(s))
    WindSpeed_count = Counter(summary)
    most_common_WindSpeed = summary_count.most_common(1)
    elementWS, countWS = most_common_WindSpeed[0]
    avg_windspeed = sum(windSpeed_arr)/len(windSpeed_arr)
    
    
    Metro_event['date_updated'] = Metro_event['date_updated'].strftime("%Y-%m-%d %H:%M:%S")
    Metro_event['temp'] = avg_temp
    Metro_event['humidity'] = avg_humidity
    Metro_event['summary'] = element
    Metro_event['windspeed'] = avg_windspeed
    Metro_event['precipitation_prob'] = avg_precipitation_prob
   
    return Metro_event

Metro_arr = []
count_arr = []
'''
Subscribes to the Metro publisher and stores the events as they come in into the Metro_arr array object
'''
@authenticate(cred_path = ENSIGN_CREDS_PATH)
@subscriber("Metro_Data_Test_2")
async def get_updatesMetro(updates):
    print('Get  Metro Update Test')
    async for event in updates:
        await print_updateMetro(event)
        await asyncio.sleep(1)

async def print_updateMetro(event):
    event_norm = json.loads(event.data)
    format_string = '%Y-%m-%dT%H:%M:%S'
    event_norm['date_updated'] = datetime.strptime(event_norm['date_updated'], format_string)
    
    if event_norm not in Metro_arr:
        Metro_arr.append(event_norm)
    
    count_arr.append("plus one")
    
    
    await to_topic(Weather_arr, Metro_arr, len(count_arr) - 1)
    print(event_norm)

Weather_arr = []
'''
Subscriber that takes the data as it comes in and pushes it into the weather_arr object
'''
@authenticate(cred_path = ENSIGN_CREDS_PATH)
@subscriber("Daniel_Weather_Test_1")

async def get_updatesWeather(updates):
    print('Get  Weather Update Test')
    async for event in updates:
        await print_updateWeather(event)
        await asyncio.sleep(1)

async def print_updateWeather(event):
    event_norm = json.loads(event.data)
    print("Weather Event Norm")

    format_string = '%Y-%m-%dT%H:%M:%S'
    event_norm['start'] = datetime.strptime(event_norm['start'][:19], format_string)
    
    if event_norm not in Weather_arr:
        Weather_arr.append(event_norm)
    #await asyncio.sleep(1)
    print(event_norm)

flag = []
'''
Publishes the output from the merge_data function (a dictionary that will be pushed into the machine learning model) into the Ensign topic
"Merged_DC_Data_Final"
'''
@publisher('Merged_DC_Data_Final')
async def to_topic(Weather_arr, Metro_arr, count_to_topic):
 
    if len(flag) == 0 and len(Metro_arr) == 0:
        count_to_topic = 0
        print('At to_topic')
        print(count_to_topic)

    elif count_to_topic < len(Metro_arr):
        final_event = merge_data(Weather_arr, Metro_arr[count_to_topic])
        print("count is", count_to_topic)
        flag.append("here")
        print("END OF TO TOPIC")

        return final_event
        
        #await get_updatesMetro()


'''
Loops through the asyncrenous subscribers Weather and Metro topics and puts the data into
seperate arrays so that data manipulation can happen
'''
#count = 0
loop = asyncio.get_event_loop()
tasks = [get_updatesWeather(), get_updatesMetro()]
loop.run_until_complete(asyncio.gather(*tasks))
loop.close()

