package main

import (
	"testing"
	"time"
)

func TestPointFromModbusJSON(t *testing.T) {
	p, err := pointFromJSON([]byte("{\"collection1_input1\":{\"time\":1786293291,\"local_time\":\"09/08/2026 19:34:51\",\"time_iso_8601\":\"2026-08-09T19:34:51+0300\",\"fw\":\"TRB1_R_00.07.23.7\\n\",\"name\":\"collection1\",\"id\":\"1\"},\"collection1_input2\":{\"addr\":\"4354\",\"data\":\"[55,29921,29911,29392,29478,29495,1,1,1245,1634,56,600]\"}}"), Config{InfluxMeasurement: "modbus"})
	if err != nil {
		t.Fatal(err)
	}
	if got := p.Tags()["collection"]; got != "collection1" {
		t.Fatalf("collection = %q", got)
	}
	if got := p.Tags()["input"]; got != "collection1" {
		t.Fatalf("input = %q", got)
	}
	if got := p.Tags()["device_id"]; got != "1" {
		t.Fatalf("device_id = %q", got)
	}
	fields, err := p.Fields()
	if err != nil {
		t.Fatal(err)
	}
	if got := fields["modbus_start_address"]; got != int64(4354) {
		t.Fatalf("address = %v", got)
	}
	if got := fields["register_1"]; got != 29921.0 {
		t.Fatalf("register_1 = %v", got)
	}
	if got := p.UnixNano(); got != time.Date(2026, 8, 9, 16, 34, 51, 0, time.UTC).UnixNano() {
		t.Fatalf("timestamp = %d", got)
	}
}

func TestPointRejectsInvalidModbusJSON(t *testing.T) {
	if _, err := pointFromJSON([]byte("{}"), Config{InfluxMeasurement: "mqtt"}); err == nil {
		t.Fatal("expected error")
	}
}
