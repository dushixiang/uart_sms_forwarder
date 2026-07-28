package service

import (
	"bytes"
	"testing"

	"github.com/dushixiang/uart_sms_forwarder/config"
	"go.uber.org/zap"
)

func TestWriteAllHandlesShortWrites(t *testing.T) {
	writer := &shortWriter{limit: 3}
	want := []byte("complete frame")
	if err := writeAll(writer, want); err != nil {
		t.Fatalf("writeAll() error = %v", err)
	}
	if !bytes.Equal(writer.Bytes(), want) {
		t.Fatalf("writeAll() wrote %q, want %q", writer.Bytes(), want)
	}
}

func TestGetStatusReturnsCopy(t *testing.T) {
	service := NewSerialService(zap.NewNop(), config.SerialConfig{}, nil, nil, nil)
	cached := &StatusData{Version: "1.0.4", PortName: "cached"}
	service.deviceCache.Set(CacheKeyDeviceStatus, cached, CacheTTL)
	service.setPortName("active")
	service.setConnected(true)

	status, err := service.GetStatus()
	if err != nil {
		t.Fatal(err)
	}
	if status == cached {
		t.Fatal("GetStatus() returned the mutable cached pointer")
	}
	if status.PortName != "active" || !status.Connected {
		t.Fatalf("GetStatus() = %+v", status)
	}
	if cached.PortName != "cached" || cached.Connected {
		t.Fatalf("GetStatus() mutated cache: %+v", cached)
	}
}

type shortWriter struct {
	bytes.Buffer
	limit int
}

func (w *shortWriter) Write(data []byte) (int, error) {
	if len(data) > w.limit {
		data = data[:w.limit]
	}
	return w.Buffer.Write(data)
}
