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
	InfluxDevice                                                                 string
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
	flag.StringVar(&c.InfluxMeasurement, "influx-measurement", env("INFLUX_MEASUREMENT", "temperature"), "InfluxDB measurement")
	flag.StringVar(&c.InfluxRetentionPolicy, "influx-retention-policy", env("INFLUX_RETENTION_POLICY", ""), "optional InfluxDB retention policy")
	flag.StringVar(&c.InfluxDevice, "influx-device", env("INFLUX_DEVICE", ""), "device tag value")
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

const (
	// Registers per Vallox documentation
	FAN_SPEED                   = 4353
	EXTRACT_AIR_TEMPERATURE     = 4354
	EXHAUST_AIR_TEMPERATURE     = 4355
	OUTDOOR_AIR_TEMPERATURE     = 4356
	SUPPLY_CELL_AIR_TEMPERATURE = 4357
	SUPPLY_AIR_TEMPERATURE      = 4358
	EXTR_FAN_SPEED              = 4361
	SUPP_FAN_SPEED              = 4362
	RH_VALUE                    = 4363
	CO2_VALUE                   = 4364
	OPTIONAL_SENSOR_TEMPERATURE = 4389
	HR_CELL_STATUS              = 4616 // 0: heat recovery, 1: cool recovery, 2: bypass, 3: defrosting
	MLV_STATE                   = 4628 // on / off

	// Registers of the diplomat-modbus device - +1 because we decrement the startAddr before calling handleRegister()!
	EXT_REG_OUTDOOR_TEMP                 = (1010 + 0x00 - 1)
	EXT_REG_INDOOR_TEMP                  = (1010 + 0x01 - 1)
	EXT_REG_INDOOR_DEC_TEMP              = (1010 + 0x02 - 1)
	EXT_REG_TARGET_TEMP                  = (1010 + 0x03 - 1)
	EXT_REG_TARGET_DEC_TEMP              = (1010 + 0x04 - 1)
	EXT_REG_SUPPLYLINE_TEMP              = (1010 + 0x05 - 1)
	EXT_REG_RETURNLINE_TEMP              = (1010 + 0x06 - 1)
	EXT_REG_HOTWATER_TEMP                = (1010 + 0x07 - 1)
	EXT_REG_BRINEOUT_TEMP                = (1010 + 0x08 - 1)
	EXT_REG_BRINEIN_TEMP                 = (1010 + 0x09 - 1)
	EXT_REG_COOLING_TEMP                 = (1010 + 0x0a - 1)
	EXT_REG_SUPPLYLINE_SHUNT             = (1010 + 0x0b - 1)
	EXT_REG_ELECTRICAL_CURRENT           = (1010 + 0x0c - 1)
	EXT_REG_AUX_HEATER_STATUS_BITMAP     = (1010 + 0x0d - 1)
	EXT_REG_SUPPLYLINE_TARGET_TEMP       = (1010 + 0x0e - 1)
	EXT_REG_SUPPLYLINE_TARGET_TEMP_SHUNT = (1010 + 0x0f - 1)
	EXT_REG_STATUS1                      = (1010 + 0x10 - 1)
	EXT_REG_STATUS2                      = (1010 + 0x11 - 1)
	EXT_REG_PWM_OUT                      = (1010 + 0x12 - 1)
	EXT_REG_ALARM1                       = (1010 + 0x13 - 1)
	EXT_REG_ALARM2                       = (1010 + 0x14 - 1)
	EXT_REG_PRESSURE_PIPE_TEMP           = (1010 + 0x17 - 1)
	EXT_REG_INTEGRAL                     = (1010 + 0x19 - 1)
	EXT_REG_INTEGRAL_REACHED_A_LIMIT     = (1010 + 0x1a - 1)
	EXT_REG_HOT_WATER_SUPPLY_LIME_TEMP   = (1010 + 0x24 - 1)
	EXT_REG_PROGRAM_VERSION              = (1010 + 0x1d - 1)
	EXT_REG_FLOWLINE_PUMP_SPEED          = (1010 + 0x1e - 1)
	EXT_REG_BRINE_PUMP_SPEED             = (1010 + 0x1f - 1)
	EXT_REG_RUNTIME_COMPRESSOR           = (1010 + 0x68 - 1)
	EXT_REG_RUNTIME_COMPRESSOR_LOW_RES   = (1010 + 0x69 - 1)
	EXT_REG_RUNTIME_3KW                  = (1010 + 0x6a - 1)
	EXT_REG_RUNTIME_3KW_LOW_RES          = (1010 + 0x6b - 1)
	EXT_REG_RUNTIME_HOTWATER             = (1010 + 0x6c - 1)
	EXT_REG_RUNTIME_HOTWATER_LOW_RES     = (1010 + 0x6d - 1)
	EXT_REG_RUNTIME_6KW                  = (1010 + 0x72 - 1)
	EXT_REG_RUNTIME_6KW_LOW_RES          = (1010 + 0x73 - 1)
)

func centiKtoC(centiK float64) float64 {
	return (centiK - 27315) / 100.0
}

func handleRegister(addr int64, offset int, rawValue uint16) (measurement string, location string, ok bool, normalizedValue float64) {
	switch int(addr) + offset {
	case OUTDOOR_AIR_TEMPERATURE:
		return "temperature", "LTO01.TE01", true, centiKtoC(float64(rawValue))
	case SUPPLY_CELL_AIR_TEMPERATURE:
		return "temperature", "LTO01.TE05", true, centiKtoC(float64(rawValue))
	case SUPPLY_AIR_TEMPERATURE:
		return "temperature", "LTO01.TE07", true, centiKtoC(float64(rawValue))
	case OPTIONAL_SENSOR_TEMPERATURE:
		return "temperature", "LTO01.TE10", true, centiKtoC(float64(rawValue))
	case EXTRACT_AIR_TEMPERATURE:
		return "temperature", "LTO01.TE30", true, centiKtoC(float64(rawValue))
	case EXHAUST_AIR_TEMPERATURE:
		return "temperature", "LTO01.TE32", true, centiKtoC(float64(rawValue))
	case EXTR_FAN_SPEED:
		return "fanspeed_rpm", "LTO01.PF", true, float64(rawValue)
	case SUPP_FAN_SPEED:
		return "fanspeed_rpm", "LTO01.TF", true, float64(rawValue)
	case FAN_SPEED:
		return "fanspeed", "LTO01", true, float64(rawValue)
	case RH_VALUE:
		return "rh", "LTO01.ME30", true, float64(rawValue)
	case CO2_VALUE:
		return "co2", "LTO01.QE30", true, float64(rawValue)
	case HR_CELL_STATUS:
		return "heat_recovery_cell", "LTO01.status", true, float64(rawValue)
	case MLV_STATE:
		return "mlv_coil", "LTO01.status", true, float64(rawValue)
	case EXT_REG_OUTDOOR_TEMP:
		return "temperature", "MLP01.outdoor", true, float64(int16(rawValue))
	case EXT_REG_INDOOR_TEMP:
		return "temperature", "MLP01.indoor", true, float64(int16(rawValue))
	case EXT_REG_INDOOR_DEC_TEMP:
		return "temperature", "MLP01.indoor_dec", true, float64(int16(rawValue))
	case EXT_REG_TARGET_TEMP:
		return "temperature", "MLP01.setpoint", true, float64(int16(rawValue))
	case EXT_REG_TARGET_DEC_TEMP:
		return "temperature", "MLP01.setpoint_dec", true, float64(int16(rawValue))
	case EXT_REG_SUPPLYLINE_TEMP:
		return "temperature", "MLP01.supply_line", true, float64(int16(rawValue))
	case EXT_REG_RETURNLINE_TEMP:
		return "temperature", "MLP01.return_line", true, float64(int16(rawValue))
	case EXT_REG_HOTWATER_TEMP:
		return "temperature", "MLP01.hotwater", true, float64(int16(rawValue))
	case EXT_REG_BRINEIN_TEMP:
		return "temperature", "MLP01.brinein", true, float64(int16(rawValue))
	case EXT_REG_BRINEOUT_TEMP:
		return "temperature", "MLP01.brineout", true, float64(int16(rawValue))
	case EXT_REG_COOLING_TEMP:
		return "temperature", "MLP01.cooling", true, float64(int16(rawValue))
	case EXT_REG_SUPPLYLINE_SHUNT:
		return "temperature", "MLP01.supply_line_shunt", true, float64(int16(rawValue))
	case EXT_REG_ELECTRICAL_CURRENT:
		return "current", "MLP01.electrical_current", true, float64(int16(rawValue))
	case EXT_REG_AUX_HEATER_STATUS_BITMAP:
		return "status", "MLP01.aux_heater", true, float64(int16(rawValue))
	case EXT_REG_SUPPLYLINE_TARGET_TEMP:
		return "temperature", "MLP01.supply_line_setpoint", true, float64(int16(rawValue))
	case EXT_REG_SUPPLYLINE_TARGET_TEMP_SHUNT:
		return "temperature", "MLP01.supply_line_setpoint_shunt", true, float64(int16(rawValue))
	case EXT_REG_STATUS1:
		return "status", "MLP01.status1", true, float64(int16(rawValue))
	case EXT_REG_STATUS2:
		return "status", "MLP01.status2", true, float64(int16(rawValue))
	case EXT_REG_PWM_OUT:
		return "pwm", "MLP01.pwm_out", true, float64(int16(rawValue))
	case EXT_REG_ALARM1:
		return "alarm", "MLP01.alarm1", true, float64(int16(rawValue))
	case EXT_REG_ALARM2:
		return "alarm", "MLP01.alarm2", true, float64(int16(rawValue))
	case EXT_REG_PRESSURE_PIPE_TEMP:
		return "temperature", "MLP01.pressure_pipe", true, float64(int16(rawValue))
	case EXT_REG_HOT_WATER_SUPPLY_LIME_TEMP:
		return "temperature", "MLP01.hot_water_supply_lime", true, float64(int16(rawValue))
	case EXT_REG_INTEGRAL:
		return "integral", "MLP01.integral", true, float64(int16(rawValue))
	case EXT_REG_INTEGRAL_REACHED_A_LIMIT:
		return "status", "MLP01.integral_reached_a_limit", true, float64(int16(rawValue))
	case EXT_REG_PROGRAM_VERSION:
		return "program_version", "MLP01", true, float64(int16(rawValue))
	case EXT_REG_FLOWLINE_PUMP_SPEED:
		return "pump_speed", "MLP01.flowline_pump", true, float64(int16(rawValue))
	case EXT_REG_BRINE_PUMP_SPEED:
		return "pump_speed", "MLP01.brine_pump", true, float64(int16(rawValue))
	case EXT_REG_RUNTIME_COMPRESSOR:
		return "runtime", "MLP01.compressor", true, float64(rawValue)
	case EXT_REG_RUNTIME_COMPRESSOR_LOW_RES:
		return "runtime", "MLP01.compressor_low_res", true, float64(rawValue) * 256
	case EXT_REG_RUNTIME_3KW:
		return "runtime", "MLP01.3kw", true, float64(rawValue)
	case EXT_REG_RUNTIME_3KW_LOW_RES:
		return "runtime", "MLP01.3kw_low_res", true, float64(rawValue) * 256
	case EXT_REG_RUNTIME_HOTWATER:
		return "runtime", "MLP01.hotwater", true, float64(rawValue)
	case EXT_REG_RUNTIME_HOTWATER_LOW_RES:
		return "runtime", "MLP01.hotwater_low_res", true, float64(rawValue) * 256
	case EXT_REG_RUNTIME_6KW:
		return "runtime", "MLP01.6kw", true, float64(rawValue)
	case EXT_REG_RUNTIME_6KW_LOW_RES:
		return "runtime", "MLP01.6kw_low_res", true, float64(rawValue) * 256
	}
	return "", "", false, 0
}

func pointFromJSON(payload []byte, c Config) ([]*influx.Point, error) {
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

	var registers []uint16
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
	if addr, ok := dataObject["addr"].(string); ok {
		startAddress, err := strconv.ParseInt(addr, 10, 64)
		startAddress -= 1 //Decrement start address by 1, since it looks like Teltonika operates on "entity addresses" and "PDU address"
		if err != nil {
			return nil, fmt.Errorf("parse Modbus address %q: %w", addr, err)
		}
		points := make([]*influx.Point, 0, len(registers))
		for offset, value := range registers {
			measurement, location, ok, normalizedValue := handleRegister(startAddress, offset, value)
			if !ok {
				fmt.Printf("no location mapping for Modbus address %d offset %d\n", startAddress, offset)
				continue
			}
			tags := map[string]string{"location": location}
			if c.InfluxDevice != "" {
				tags["device"] = c.InfluxDevice
			}
			fields := map[string]any{"value": normalizedValue}
			point, err := influx.NewPoint(measurement, tags, fields, pointTime)
			if err != nil {
				return nil, fmt.Errorf("create Influx point: %w", err)
			}
			points = append(points, point)
		}
		return points, nil
	}
	return nil, errors.New("register data object has no addr field")
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
			points, err := pointFromJSON(message.Payload(), c)
			if err != nil {
				log.Printf("drop message on %s: %v", message.Topic(), err)
				return
			}

			batch, err := influx.NewBatchPoints(influx.BatchPointsConfig{Database: c.InfluxDatabase, RetentionPolicy: c.InfluxRetentionPolicy})
			if err == nil {
				batch.AddPoints(points)
				err = influxClient.Write(batch)
			}
			if err != nil {
				log.Printf("write points: %+v", err)
			}
		})
		token.Wait()
		if token.Error() != nil {
			log.Printf("subscribe: %v", token.Error())
			return
		}
		log.Printf("subscribed to topic \"%s\"", c.MQTTTopic)
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
