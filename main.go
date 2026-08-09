package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	influx "github.com/influxdata/influxdb1-client/v2"
)

type Config struct {
	MQTTURL, MQTTTopic, MQTTUsername, MQTTPassword, MQTTClientID                 string
	MQTTCAFile, MQTTClientCert, MQTTClientKey                                    string
	MQTTInsecureSkipVerify                                                       bool
	MQTTQoS                                                                      byte
	InfluxURL, InfluxDatabase, InfluxUsername, InfluxPassword, InfluxMeasurement string
	InfluxRetentionPolicy                                                        string
	InfluxTags                                                                   []string
	TimestampField                                                               string
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func parseInt(value string, fallback int) int {
	n, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return n
}

func loadConfig() Config {
	var tags string
	c := Config{}
	flag.StringVar(&c.MQTTURL, "mqtt-url", env("MQTT_URL", "ssl://localhost:8883"), "MQTT broker URL")
	flag.StringVar(&c.MQTTTopic, "mqtt-topic", env("MQTT_TOPIC", "sensors/#"), "MQTT topic filter")
	flag.StringVar(&c.MQTTUsername, "mqtt-username", env("MQTT_USERNAME", ""), "MQTT username")
	flag.StringVar(&c.MQTTPassword, "mqtt-password", env("MQTT_PASSWORD", ""), "MQTT password")
	flag.StringVar(&c.MQTTClientID, "mqtt-client-id", env("MQTT_CLIENT_ID", "mqtt-influx-go"), "MQTT client ID")
	flag.StringVar(&c.MQTTCAFile, "mqtt-ca-file", env("MQTT_CA_FILE", ""), "CA certificate PEM file")
	flag.StringVar(&c.MQTTClientCert, "mqtt-client-cert", env("MQTT_CLIENT_CERT", ""), "optional MQTT client certificate PEM file")
	flag.StringVar(&c.MQTTClientKey, "mqtt-client-key", env("MQTT_CLIENT_KEY", ""), "optional MQTT client private key PEM file")
	flag.BoolVar(&c.MQTTInsecureSkipVerify, "mqtt-insecure-skip-verify", strings.EqualFold(env("MQTT_INSECURE_SKIP_VERIFY", "false"), "true"), "disable TLS certificate verification")
	var qos uint
	flag.UintVar(&qos, "mqtt-qos", uint(parseInt(env("MQTT_QOS", "0"), 0)), "MQTT QoS (0, 1, or 2)")
	flag.StringVar(&c.InfluxURL, "influx-url", env("INFLUX_URL", "http://localhost:8086"), "InfluxDB 1.x HTTP URL")
	flag.StringVar(&c.InfluxDatabase, "influx-database", env("INFLUX_DATABASE", "mqtt"), "InfluxDB database")
	flag.StringVar(&c.InfluxUsername, "influx-username", env("INFLUX_USERNAME", ""), "InfluxDB username")
	flag.StringVar(&c.InfluxPassword, "influx-password", env("INFLUX_PASSWORD", ""), "InfluxDB password")
	flag.StringVar(&c.InfluxMeasurement, "influx-measurement", env("INFLUX_MEASUREMENT", "mqtt"), "InfluxDB measurement")
	flag.StringVar(&c.InfluxRetentionPolicy, "influx-retention-policy", env("INFLUX_RETENTION_POLICY", ""), "optional InfluxDB retention policy")
	flag.StringVar(&tags, "influx-tags", env("INFLUX_TAGS", ""), "comma-separated JSON keys to write as tags")
	flag.StringVar(&c.TimestampField, "timestamp-field", env("TIMESTAMP_FIELD", ""), "optional JSON field containing an RFC3339 timestamp")
	flag.Parse()
	c.MQTTQoS = byte(qos)
	for _, tag := range strings.Split(tags, ",") {
		if tag = strings.TrimSpace(tag); tag != "" {
			c.InfluxTags = append(c.InfluxTags, tag)
		}
	}
	return c
}

func (c Config) validate() error {
	if c.MQTTURL == "" || c.MQTTTopic == "" || c.InfluxURL == "" || c.InfluxDatabase == "" || c.InfluxMeasurement == "" {
		return errors.New("MQTT URL/topic and Influx URL/database/measurement are required")
	}
	if c.MQTTQoS > 2 {
		return errors.New("mqtt-qos must be 0, 1, or 2")
	}
	if (c.MQTTClientCert == "") != (c.MQTTClientKey == "") {
		return errors.New("MQTT client certificate and key must be supplied together")
	}
	return nil
}

func tlsConfig(c Config) (*tls.Config, error) {
	t := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: c.MQTTInsecureSkipVerify} // #nosec G402: explicitly configurable for private brokers.
	if c.MQTTCAFile != "" {
		pem, err := os.ReadFile(c.MQTTCAFile)
		if err != nil {
			return nil, fmt.Errorf("read MQTT CA file: %w", err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("MQTT CA file contains no certificates")
		}
		t.RootCAs = roots
	}
	if c.MQTTClientCert != "" {
		cert, err := tls.LoadX509KeyPair(c.MQTTClientCert, c.MQTTClientKey)
		if err != nil {
			return nil, fmt.Errorf("load MQTT client certificate: %w", err)
		}
		t.Certificates = []tls.Certificate{cert}
	}
	return t, nil
}

func scalar(value any) (any, bool) {
	switch value.(type) {
	case string, bool, float64:
		return value, true
	case nil:
		return nil, false
	default:
		b, err := json.Marshal(value)
		if err != nil {
			return nil, false
		}
		return string(b), true
	}
}

func pointFromJSON(payload []byte, c Config) (*influx.Point, error) {
	var message map[string]json.RawMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return nil, fmt.Errorf("decode JSON: %w", err)
	}

	var metadata map[string]any
	var metadataKey, dataKey string
	var dataObject map[string]any
	for key, raw := range message {
		var object map[string]any
		if err := json.Unmarshal(raw, &object); err != nil {
			continue
		}
		if _, hasData := object["data"]; hasData {
			dataKey, dataObject = key, object
		} else if _, hasTime := object["time"]; hasTime {
			metadataKey, metadata = key, object
		}
	}
	if dataKey == "" || metadataKey == "" {
		return nil, errors.New("JSON must contain metadata (*_input1) and register data (*_input2) objects")
	}

	var registers []float64
	dataText, ok := dataObject["data"].(string)
	if !ok {
		return nil, errors.New("register data field must be a JSON array encoded as a string")
	}
	if err := json.Unmarshal([]byte(dataText), &registers); err != nil {
		return nil, fmt.Errorf("decode register data: %w", err)
	}
	if len(registers) == 0 {
		return nil, errors.New("register data array is empty")
	}

	pointTime, err := messageTime(metadata)
	if err != nil {
		return nil, err
	}
	tags, fields := map[string]string{}, map[string]any{}
	collection := strings.TrimSuffix(metadataKey, "_input1")
	input := strings.TrimSuffix(dataKey, "_input2")
	if collection != "" {
		tags["collection"] = collection
	}
	if input != "" {
		tags["input"] = input
	}
	if id, ok := metadata["id"].(string); ok {
		tags["device_id"] = id
	}
	if name, ok := metadata["name"].(string); ok {
		tags["name"] = name
	}

	if addr, ok := dataObject["addr"].(string); ok {
		value, err := strconv.ParseInt(addr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse Modbus address %q: %w", addr, err)
		}
		fields["modbus_start_address"] = value
	} else {
		return nil, errors.New("register data object has no addr field")
	}
	for index, value := range registers {
		fields[fmt.Sprintf("register_%d", index)] = value
	}

	point, err := influx.NewPoint(c.InfluxMeasurement, tags, fields, pointTime)
	if err != nil {
		return nil, fmt.Errorf("create Influx point: %w", err)
	}
	return point, nil
}

func messageTime(metadata map[string]any) (time.Time, error) {
	if unix, ok := metadata["time"].(float64); ok {
		return time.Unix(int64(unix), 0).UTC(), nil
	}
	if text, ok := metadata["time_iso_8601"].(string); ok {
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05-0700"} {
			if parsed, err := time.Parse(layout, text); err == nil {
				return parsed, nil
			}
		}
		return time.Time{}, fmt.Errorf("parse time_iso_8601 %q", text)
	}
	return time.Time{}, errors.New("metadata object has no usable time or time_iso_8601 field")
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	c := loadConfig()
	if err := c.validate(); err != nil {
		log.Fatal(err)
	}
	tlsSettings, err := tlsConfig(c)
	if err != nil {
		log.Fatal(err)
	}
	influxClient, err := influx.NewHTTPClient(influx.HTTPConfig{Addr: c.InfluxURL, Username: c.InfluxUsername, Password: c.InfluxPassword})
	if err != nil {
		log.Fatalf("create Influx client: %v", err)
	}
	defer influxClient.Close()

	options := paho.NewClientOptions().AddBroker(c.MQTTURL).SetClientID(c.MQTTClientID).SetUsername(c.MQTTUsername).SetPassword(c.MQTTPassword).SetTLSConfig(tlsSettings).SetAutoReconnect(true)
	options.OnConnect = func(client paho.Client) {
		token := client.Subscribe(c.MQTTTopic, c.MQTTQoS, func(_ paho.Client, message paho.Message) {
			point, err := pointFromJSON(message.Payload(), c)
			if err != nil {
				log.Printf("drop message on %s: %v", message.Topic(), err)
				return
			}
			batch, err := influx.NewBatchPoints(influx.BatchPointsConfig{Database: c.InfluxDatabase, RetentionPolicy: c.InfluxRetentionPolicy})
			if err == nil {
				batch.AddPoint(point)
				err = influxClient.Write(batch)
			}
			if err != nil {
				log.Printf("write point: %v", err)
			}
		})
		token.Wait()
		if token.Error() != nil {
			log.Printf("subscribe: %v", token.Error())
			return
		}
		log.Printf("subscribed to %s", c.MQTTTopic)
	}
	options.OnConnectionLost = func(_ paho.Client, err error) { log.Printf("MQTT connection lost: %v", err) }
	client := paho.NewClient(options)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}
	defer client.Disconnect(250)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	log.Println("shutting down")
}
