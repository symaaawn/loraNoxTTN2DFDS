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
		"000C97C70C6FEEC4": "e6bfb32e-9e0f-4b20-8664-f9ce5229204b"
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
	
	if (parsedPayload["port"] == 3):
            dfdsMessage = {}
            dfdsMessage["typ"] = "1"
            dfdsMessage["gw"] = gwID
            dfdsMessage["bn"] = senID
            dfdsMessage["bt"] = int(time.time()) * 1000	
            
            #erster Messwert: nox
            noxData = {}
            noxData["n"] = str(uuid.uuid4()) + ":R1"
            noxData["t"] = -3000
            try:
                    noxData["sv"] = parsedPayload["payload_fields"]["rNox"]
            except:
                    noxData["sv"] = 0
            
            #zweiter Messwert: co
            coData = {}
            coData["n"] = str(uuid.uuid4()) + ":R2"
            coData["t"] = -3000
            try:
                    coData["sv"] = parsedPayload["payload_fields"]["coData"]
            except:
                    coData["sv"] = 0
                    
            dfdsMessage["e"] = [noxData, coData]
            
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

while True:
    dfdsClient.loop()
    ttnClient.loop()
