#!/usr/bin/python3

#	DatenFuerDieStadt MQTT Service
#	Receives MQTT 
#
#
import paho.mqtt.client as mqtt
import json
	
def on_dfds_connect(client, userdata, flags, rc):
	print("Connected to Babeauf with result code " + str(rc))
	client.subscribe("sc/mw/#")
	
def on_message(client, userdata, msg):
	payload = str(msg.payload).replace("b'", "").replace("'", "")
	parsedPayload = json.loads(payload)
	print(json.dumps(parsedPayload, sort_keys = True, indent = 4))

dfdsClient = mqtt.Client()
dfdsClient.on_connect = on_dfds_connect
dfdsClient.on_message = on_message
dfdsClient.username_pw_set("sneugebauer", "sneugebauer")
dfdsClient.connect("babeauf.nt.fh-koeln.de", 2883, 60)

#print(uuid.uuid4())

dfdsClient.loop_forever()
