package main

import "testing"

func TestPointFromJSON(t *testing.T) {
	c := Config{InfluxMeasurement: "temperature", InfluxTags: []string{"device"}, TimestampField: "time"}
	p, err := pointFromJSON([]byte(`{"device":"living-room","value":21.5,"ok":true,"time":"2024-01-02T03:04:05Z"}`), c)
	if err != nil {
		t.Fatal(err)
	}
	if got := p.Tags()["device"]; got != "living-room" {
		t.Fatalf("tag = %q", got)
	}
	fields, err := p.Fields()
	if err != nil {
		t.Fatal(err)
	}
	if got := fields["value"]; got != 21.5 {
		t.Fatalf("value = %v", got)
	}
}

func TestPointRejectsNonObject(t *testing.T) {
	if _, err := pointFromJSON([]byte("[]"), Config{InfluxMeasurement: "mqtt"}); err == nil {
		t.Fatal("expected error")
	}
}
