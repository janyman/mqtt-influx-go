package main

import (
	"testing"
	"time"
)

func TestPointFromModbusJSON(t *testing.T) {
	points, err := pointFromJSON([]byte("{\"collection1_input1\":{\"time\":1786293291,\"local_time\":\"09/08/2026 19:34:51\",\"time_iso_8601\":\"2026-08-09T19:34:51+0300\",\"fw\":\"TRB1_R_00.07.23.7\\n\",\"name\":\"collection1\",\"id\":\"1\"},\"collection1_input2\":{\"addr\":\"4355\",\"data\":\"[217]\"}}"), Config{InfluxMeasurement: "temperature", InfluxDevice: "trb145"})
	if err != nil {
		t.Fatal(err)
	}
	if got := points[0].Tags()["location"]; got != "extract" {
		t.Fatalf("collection = %q", got)
	}
	if got := points[0].Tags()["device"]; got != "trb145" {
		t.Fatalf("device = %q", got)
	}
	fields, err := points[0].Fields()
	if err != nil {
		t.Fatal(err)
	}
	if got := fields["value"]; got != -270.98 {
		t.Fatalf("address = %v", got)
	}
	if len(points) != 1 {
		t.Fatalf("points = %d", len(points))
	}
	if got := points[0].UnixNano(); got != time.Date(2026, 8, 9, 16, 34, 51, 0, time.UTC).UnixNano() {
		t.Fatalf("timestamp = %d", got)
	}
}

func TestPointRejectsInvalidModbusJSON(t *testing.T) {
	if _, err := pointFromJSON([]byte("{}"), Config{InfluxMeasurement: "mqtt"}); err == nil {
		t.Fatal("expected error")
	}
}
