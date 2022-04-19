#!/usr/bin/env python3
import os
import mysql.connector as mariadb
import paho.mqtt.client as mqtt
import json

# Define env variable
mqtt_client_id="mqtt2mysql"
mqtt_encripted=False
try:
    mqtt_usr=os.environ['MQTT_USR']
    mqtt_pass=os.environ['MQTT_PASS']
    mqtt_adr=os.environ['MQTT_ADR']
    mqtt_port=int(os.environ['MQTT_PORT'])
    mysql_adr=os.environ['MYSQL_ADR']
    mysql_port=os.environ['MYSQL_PORT']
    mysql_db=os.environ['MYSQL_DB']
    mysql_usr=os.environ['MYSQL_USR']
    mysql_pass=os.environ['MYSQL_PASS']    
except Exception as e:
    print("one or more env variable unavaliable "+"{0}".format(e.__class__))

# Connection DB
mariadb_connection = mariadb.connect(host=mysql_adr,port=mysql_port,user=mysql_usr, password=mysql_pass, database=mysql_db)
cursor = mariadb_connection.cursor()

def on_connect(client, userdata, flags, rc):
    # This will be called once the client connects
    #print(f"Connected with result code {rc}")
    # Subscribe here!
    client.subscribe("modules/#")
def on_message(client, userdata, msg):
    #print(f"Message received [{msg.topic}]: {msg.payload}")
    #print(msg.payload.decode().replace('\r', ''))
    
    data=json.loads(msg.payload.decode().replace('\r', ''))
    module=msg.topic.replace('modules/', '').replace('/All', '')   
    sql = "INSERT INTO mqtt.modules (`module`,`MsgTimeStamp`,`PM25`,`Humidity`,`TemperatureC`,`TemperatureF`,`DewPointC`,`DewPointF`) VALUES ('"+module+"','"+data['MsgTimeStamp']+"','"+data['PM25']+"','"+data['Humidity']+"','"+data['TemperatureC']+"','"+data['TemperatureF']+"','"+data['DewPointC']+"','"+data['DewPointF']+"')"
    #  # Save Data into DB Table
    try:
      cursor.execute(sql)
    except mariadb.Error as error:
      print("Error: {}".format(error))
    mariadb_connection.commit()
    #print(sql)
    #print(data['TemperatureC'])
client = mqtt.Client(mqtt_client_id) # client ID "mqtt-test"
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(mqtt_usr,mqtt_pass)
if mqtt_encripted:
    mqtt_cert=os.environ['MQTT_CERT']
    client.tls_set(ca_certs="ca.crt", tls_version=ssl.PROTOCOL_TLSv1_2) # For encripted
client.connect(mqtt_adr, mqtt_port)
client.loop_forever()  # Start networking daemon

mariadb_connection.close()