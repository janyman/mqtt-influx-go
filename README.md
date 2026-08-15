# mqtt-influx-go

A small MQTT over TLS listener that converts Modbus data sent by a Teltonika TRB145 as JSON payload into an InfluxDB 1.x point.

For each message, the metadata object ending in _input1 supplies the point timestamp. The register object ending in _input2 supplies the Modbus start address and register values. Each register becomes one point with a numeric `value` field and `location` tag; the optional `device` tag is set from `INFLUX_DEVICE`. The address and register-offset mappings are defined in `locationForRegister` in `main.go`. Unknown mappings cause the message to be rejected.

The default measurement is `temperature`. The Unix time value is preferred; time_iso_8601 is also accepted, including offsets such as +0300.

~~~sh
export MQTT_URL='ssl://broker.example.net:8883'
export MQTT_TOPIC='modbus/+/reading'
export MQTT_USERNAME='mqtt-user'
export MQTT_PASSWORD='change-me'
export MQTT_CA_FILE='/etc/mqtt/ca.pem'       # optional
export INFLUX_URL='http://127.0.0.1:8086'
export INFLUX_DATABASE='sensors'
export INFLUX_MEASUREMENT='modbus_registers'
go run .
~~~

Every setting has a matching command-line flag. MQTT client certificates are supported with MQTT_CLIENT_CERT and MQTT_CLIENT_KEY; MQTT_INSECURE_SKIP_VERIFY=true is available for testing only. InfluxDB 1.x authentication uses INFLUX_USERNAME and INFLUX_PASSWORD.

Example input:

~~~json
{"collection1_input1":{"time":1786293291,"local_time":"09/08/2026 19:34:51","time_iso_8601":"2026-08-09T19:34:51+0300","fw":"TRB1_R_00.07.23.7\n","name":"collection1","id":"1"},"collection1_input2":{"addr":"4354","data":"[55,29921,29911,29392,29478,29495,1,1,1245,1634,56,600]"}}
~~~

The Influx database must already exist. The service reconnects to MQTT automatically and logs malformed messages or failed writes without stopping the listener.


## Docker deployment

Build the image from the project directory:

~~~sh
docker build -t mqtt-influx-go:latest .
~~~

Run it by passing the configuration as environment variables. The container does not need an exposed port because it creates outbound connections to MQTT and InfluxDB. On Linux, --add-host=host.docker.internal:host-gateway makes services on the native host reachable; use host.docker.internal in MQTT_URL too when the broker is also on the host:

~~~sh
docker run -d \
  --name mqtt-influx-go \
  --add-host=host.docker.internal:host-gateway \
  --restart unless-stopped \
  -e MQTT_URL='ssl://broker.example.net:8883' \
  -e MQTT_TOPIC='modbus/+/reading' \
  -e MQTT_USERNAME='mqtt-user' \
  -e MQTT_PASSWORD='change-me' \
  -e INFLUX_URL='http://host.docker.internal:8086' \
  -e INFLUX_DATABASE='sensors' \
  -e INFLUX_MEASUREMENT='temperature'
  -e INFLUX_DEVICE='trb145' \
  mqtt-influx-go:latest
~~~



For a private MQTT CA certificate, mount the PEM file read-only and point MQTT_CA_FILE at the container path:

~~~sh
docker run -d \
  --name mqtt-influx-go \
  --add-host=host.docker.internal:host-gateway \
  --restart unless-stopped \
  -e MQTT_URL='ssl://broker.example.net:8883' \
  -e MQTT_TOPIC='modbus/+/reading' \
  -e MQTT_USERNAME='mqtt-user' \
  -e MQTT_PASSWORD='change-me' \
  -e MQTT_CA_FILE='/run/certs/mqtt-ca.pem' \
  -e INFLUX_URL='http://host.docker.internal:8086' \
  -e INFLUX_DATABASE='sensors' \
  -e INFLUX_MEASUREMENT='temperature'
  -e INFLUX_DEVICE='trb145' \
  -v "$PWD/certs/mqtt-ca.pem:/run/certs/mqtt-ca.pem:ro" \
  mqtt-influx-go:latest
~~~

For mutual TLS, also mount the client certificate and key and set MQTT_CLIENT_CERT and MQTT_CLIENT_KEY to their paths inside the container. Do not use MQTT_INSECURE_SKIP_VERIFY=true in production.

Check logs and stop/remove the container with:

~~~sh
docker logs -f mqtt-influx-go
docker stop mqtt-influx-go
docker rm mqtt-influx-go
~~~

The InfluxDB 1.x database must exist before starting the container. Store credentials through your deployment secret mechanism rather than committing them to shell history or source control.
