#!/usr/bin/python3

#	DatenFuerDieStadt MQTT Service
#	Subscribes to The Things Network MQTT Broker for all uplink messages, converts them into a
#       Format the SensorCloud of the TH Köln can understand and publishes them to the SensorCloud
#       Broker

import time
import paho.mqtt.client as mqtt
import json
import uuid

gwID = "bcff9a23-47f6-489f-a1ba-b6c26723cc70"

def get_senID(devEUI):
	return { 
		"00AA76B1E9DEA616": "e6bfb32e-9e0f-4b20-8664-f9ce5229204b",
                "0014F1CAE3D257C1": "f61ae9b4-0d74-4d0b-8a67-602290d61425"
	}.get(devEUI)

def on_ttn_connect(client, userdata, flags, rc):
	print("Connected to The Things Network with result code " + str(rc))
	
	client.subscribe("+/devices/+/up")
	
def on_dfds_connect(client, userdata, flags, rc):
	print("Connected to Babeauf with result code " + str(rc))
	
def on_message(client, userdata, msg):
	payload = str(msg.payload).replace("b'", "").replace("'", "")
	parsedPayload = json.loads(payload)
	senID = str(get_senID(parsedPayload["hardware_serial"]))
	
	print("\nMessage received")
	print(json.dumps(parsedPayload, sort_keys = True, indent = 4))
	
	if (parsedPayload["port"] == 1):
            dfdsMessage = {}
            dfdsMessage["typ"] = "1"
            dfdsMessage["gw"] = gwID
            dfdsMessage["bn"] = senID
            dfdsMessage["bt"] = int(time.time()) * 1000	
            dfdsMessage["e"] = []
            
            #erster Messwert: nox
            if "rNox" in parsedPayload["payload_fields"]:
                noxData = {}
                noxData["n"] = str(uuid.uuid4()) + ":R1"
                noxData["t"] = -3000
                noxData["sv"] = parsedPayload["payload_fields"]["rNox"]
                dfdsMessage["e"].append(dict(noxData))
            
            #zweiter Messwert: co
            if "rCo" in parsedPayload["payload_fields"]:
                coData = {}
                coData["n"] = str(uuid.uuid4()) + ":R2"
                coData["t"] = -3000
                coData["sv"] = parsedPayload["payload_fields"]["rCo"]
                dfdsMessage["e"].append(dict(coData))
                              
            if "temp" in parsedPayload["payload_fields"]:
                tempData = {}
                tempData["n"] = str(uuid.uuid4()) + ":temp"
                tempData["t"] = -3000
                tempData["sv"] = parsedPayload["payload_fields"]["temp"]
                dfdsMessage["e"].append(dict(tempData))
                
            if "rssi" in parsedPayload["payload_fields"]:
                rssiData = {}
                rssiData["n"] = str(uuid.uuid4()) + ":rssi"
                rssiData["t"] = -3000
                rssiData["sv"] = parsedPayload["payload_fields"]["rssi"]
                dfdsMessage["e"].append(dict(rssiData))
                
            if "hum" in parsedPayload["payload_fields"]:
                humData = {}
                humData["n"] = str(uuid.uuid4()) + ":hum"
                humData["t"] = -3000
                humData["sv"] = parsedPayload["payload_fields"]["hum"]
                dfdsMessage["e"].append(dict(humData))
                
            if "pm10" in parsedPayload["payload_fields"]:
                pm10Data = {}
                pm10Data["n"] = str(uuid.uuid4()) + ":pm10"
                pm10Data["t"] = -3000
                pm10Data["sv"] = parsedPayload["payload_fields"]["pm10"]
                dfdsMessage["e"].append(dict(pm10Data))
                
            if "pm25" in parsedPayload["payload_fields"]:
                pm25Data = {}
                pm25Data["n"] = str(uuid.uuid4()) + ":pm25"
                pm25Data["t"] = -3000
                pm25Data["sv"] = parsedPayload["payload_fields"]["pm25"]
                dfdsMessage["e"].append(dict(pm25Data))
            
            dfdsClient.publish("mw/sc/" + senID, json.dumps(dfdsMessage))
            print("Message published")
            print(json.dumps(dfdsMessage, sort_keys = True, indent = 4))
	
ttnClient = mqtt.Client()
ttnClient.on_connect = on_ttn_connect
ttnClient.on_message = on_message
ttnClient.username_pw_set("user", "pw")
ttnClient.connect("eu.thethings.network", 1883, 60)

dfdsClient = mqtt.Client()
dfdsClient.on_connect = on_dfds_connect
dfdsClient.username_pw_set("user", "pw")
dfdsClient.connect("babeauf.nt.fh-koeln.de", 2883, 60)

#dfdsClient.loop_start()
#ttnClient.loop_start()

while True:
    dfdsClient.loop()
    ttnClient.loop()
