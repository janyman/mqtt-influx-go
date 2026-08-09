# mqtt-influx-go

A small MQTT over TLS listener that converts JSON objects into points for InfluxDB 1.x.

Each JSON property becomes an Influx field. Keys listed in INFLUX_TAGS become string tags. Nested values are stored as JSON strings; null values are ignored. An optional TIMESTAMP_FIELD can contain an RFC3339 timestamp.

~~~sh
export MQTT_URL='ssl://broker.example.net:8883'
export MQTT_TOPIC='sensors/+/reading'
export MQTT_USERNAME='mqtt-user'
export MQTT_PASSWORD='change-me'
export MQTT_CA_FILE='/etc/mqtt/ca.pem'       # optional
export INFLUX_URL='http://127.0.0.1:8086'
export INFLUX_DATABASE='sensors'
export INFLUX_MEASUREMENT='readings'
export INFLUX_TAGS='device,location'
export TIMESTAMP_FIELD='timestamp'           # optional, RFC3339
go run .
~~~

Every setting also has a matching command-line flag. MQTT client certificates are supported with MQTT_CLIENT_CERT and MQTT_CLIENT_KEY; MQTT_INSECURE_SKIP_VERIFY=true is available for testing only. InfluxDB 1.x authentication uses INFLUX_USERNAME and INFLUX_PASSWORD.

Example message:

~~~json
{"device":"boiler","location":"basement","temperature":64.2,"running":true,"timestamp":"2024-01-02T03:04:05Z"}
~~~

The Influx database must already exist. The service reconnects to MQTT automatically and logs malformed messages or failed writes without stopping the listener.
