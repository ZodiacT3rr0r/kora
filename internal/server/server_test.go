package server

import (
	"bytes"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"kora/internal/engine/core"
)

func TestRespParserSimpleString(t *testing.T) {
	data := "+OK\r\n"
	p := NewRespParser(bytes.NewReader([]byte(data)))
	val, err := p.Parse()
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if val.Type != RespSimpleString || val.Str != "OK" {
		t.Errorf("expected simple string 'OK', got type %c str %s", val.Type, val.Str)
	}
}

func TestRespParserBulkString(t *testing.T) {
	data := "$5\r\nhello\r\n"
	p := NewRespParser(bytes.NewReader([]byte(data)))
	val, err := p.Parse()
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if val.Type != RespBulkString || val.Str != "hello" {
		t.Errorf("expected bulk string 'hello', got type %c str %s", val.Type, val.Str)
	}
}

func TestRespParserArray(t *testing.T) {
	data := "*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n"
	p := NewRespParser(bytes.NewReader([]byte(data)))
	val, err := p.Parse()
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if val.Type != RespArray || len(val.Array) != 2 {
		t.Errorf("expected array of 2 elements, got type %c len %d", val.Type, len(val.Array))
	}
	if val.Array[0].Str != "GET" {
		t.Errorf("expected 'GET', got %s", val.Array[0].Str)
	}
	if val.Array[1].Str != "key1" {
		t.Errorf("expected 'key1', got %s", val.Array[1].Str)
	}
}

func TestRespWriterSimpleString(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewRespWriter(buf)
	w.WriteSimpleString("OK")
	expected := "+OK\r\n"
	if buf.String() != expected {
		t.Errorf("expected %q, got %q", expected, buf.String())
	}
}

func TestRespWriterBulkString(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewRespWriter(buf)
	w.WriteBulkString("hello")
	expected := "$5\r\nhello\r\n"
	if buf.String() != expected {
		t.Errorf("expected %q, got %q", expected, buf.String())
	}
}

func TestExtractCommand(t *testing.T) {
	val := &RespValue{
		Type: RespArray,
		Array: []*RespValue{
			{Type: RespBulkString, Str: "set"},
			{Type: RespBulkString, Str: "mykey"},
			{Type: RespBulkString, Str: "myvalue"},
		},
	}
	cmd, args, err := ExtractCommand(val)
	if err != nil {
		t.Fatalf("extract failed: %v", err)
	}
	if cmd != "SET" {
		t.Errorf("expected command 'SET', got %s", cmd)
	}
	if len(args) != 2 || args[0] != "mykey" || args[1] != "myvalue" {
		t.Errorf("expected args ['mykey', 'myvalue'], got %v", args)
	}
}

func TestServerIntegration(t *testing.T) {
	// Create temporary engine
	tmpDir := t.TempDir()
	engine, err := core.Open(core.Options{
		DataDir:         tmpDir,
		WALPath:         filepath.Join(tmpDir, "wal"),
		ManifestPath:    filepath.Join(tmpDir, "manifest.json"),
		MemTableMaxSize: 1024 * 100,
		SyncOnWrite:     false,
	})
	if err != nil {
		t.Fatalf("open engine failed: %v", err)
	}
	defer engine.Close()

	// Create server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	addr := listener.Addr().String()
	listener.Close()

	server := NewServer(addr, engine)
	go func() {
		_ = server.Listen()
	}()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Connect and test SET command
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	writer := NewRespWriter(conn)
	parser := NewRespParser(conn)

	// Send SET command
	if err := writer.WriteArray([]*RespValue{
		{Type: RespBulkString, Str: "SET"},
		{Type: RespBulkString, Str: "testkey"},
		{Type: RespBulkString, Str: "testvalue"},
	}); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	// Read response
	resp, err := parser.Parse()
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if resp.Type != RespSimpleString || resp.Str != "OK" {
		t.Errorf("expected 'OK', got %v", resp)
	}

	// Send GET command
	if err := writer.WriteArray([]*RespValue{
		{Type: RespBulkString, Str: "GET"},
		{Type: RespBulkString, Str: "testkey"},
	}); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	// Read response
	resp, err = parser.Parse()
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if resp.Type != RespBulkString || resp.Str != "testvalue" {
		t.Errorf("expected 'testvalue', got %v", resp)
	}

	// Send QUIT command
	if err := writer.WriteArray([]*RespValue{
		{Type: RespBulkString, Str: "QUIT"},
	}); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	resp, err = parser.Parse()
	if err != nil && err != io.EOF {
		t.Fatalf("parse failed: %v", err)
	}

	server.Stop()
}

func TestServerPingCommand(t *testing.T) {
	tmpDir := t.TempDir()
	engine, _ := core.Open(core.Options{
		DataDir:         tmpDir,
		WALPath:         filepath.Join(tmpDir, "wal"),
		ManifestPath:    filepath.Join(tmpDir, "manifest.json"),
		MemTableMaxSize: 1024 * 100,
		SyncOnWrite:     false,
	})
	defer engine.Close()

	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := listener.Addr().String()
	listener.Close()

	server := NewServer(addr, engine)
	go func() {
		_ = server.Listen()
	}()
	time.Sleep(100 * time.Millisecond)

	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()

	writer := NewRespWriter(conn)
	parser := NewRespParser(conn)

	// Send PING
	writer.WriteArray([]*RespValue{
		{Type: RespBulkString, Str: "PING"},
	})

	resp, _ := parser.Parse()
	if resp.Type != RespSimpleString || resp.Str != "PONG" {
		t.Errorf("expected 'PONG', got %v", resp)
	}

	server.Stop()
}

func TestServerSetGetDelete(t *testing.T) {
	tmpDir := t.TempDir()
	engine, _ := core.Open(core.Options{
		DataDir:         tmpDir,
		WALPath:         filepath.Join(tmpDir, "wal"),
		ManifestPath:    filepath.Join(tmpDir, "manifest.json"),
		MemTableMaxSize: 1024 * 100,
		SyncOnWrite:     false,
	})
	defer engine.Close()

	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := listener.Addr().String()
	listener.Close()

	server := NewServer(addr, engine)
	go func() {
		_ = server.Listen()
	}()
	time.Sleep(100 * time.Millisecond)

	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()

	writer := NewRespWriter(conn)
	parser := NewRespParser(conn)

	// SET key value
	writer.WriteArray([]*RespValue{
		{Type: RespBulkString, Str: "SET"},
		{Type: RespBulkString, Str: "key1"},
		{Type: RespBulkString, Str: "value1"},
	})
	resp, _ := parser.Parse()
	if resp.Str != "OK" {
		t.Errorf("SET failed: expected OK, got %v", resp)
	}

	// GET key
	writer.WriteArray([]*RespValue{
		{Type: RespBulkString, Str: "GET"},
		{Type: RespBulkString, Str: "key1"},
	})
	resp, _ = parser.Parse()
	if resp.Str != "value1" {
		t.Errorf("GET failed: expected value1, got %v", resp)
	}

	// DEL key
	writer.WriteArray([]*RespValue{
		{Type: RespBulkString, Str: "DEL"},
		{Type: RespBulkString, Str: "key1"},
	})
	resp, _ = parser.Parse()
	if resp.Int != 1 {
		t.Errorf("DEL failed: expected 1, got %v", resp)
	}

	// GET key (should be nil)
	writer.WriteArray([]*RespValue{
		{Type: RespBulkString, Str: "GET"},
		{Type: RespBulkString, Str: "key1"},
	})
	resp, _ = parser.Parse()
	if !resp.Null {
		t.Errorf("GET deleted key failed: expected nil, got %v", resp)
	}

	server.Stop()
}
