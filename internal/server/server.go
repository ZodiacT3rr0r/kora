package server

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"kora/internal/engine/core"
)

// Server implements a Redis-compatible TCP server.
type Server struct {
	addr   string
	engine *core.Engine
	mu     sync.Mutex
	ln     net.Listener
}

// NewServer creates a new server instance.
func NewServer(addr string, engine *core.Engine) *Server {
	return &Server{
		addr:   addr,
		engine: engine,
	}
}

// Listen starts the server and blocks until a fatal error or Stop() is called.
func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if s.ln == ln {
			s.ln = nil
		}
		s.mu.Unlock()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handleConnection(conn)
	}
}

// Stop stops the server.
func (s *Server) Stop() error {
	s.mu.Lock()
	ln := s.ln
	s.mu.Unlock()
	if ln != nil {
		return ln.Close()
	}
	return nil
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	parser := NewRespParser(conn)
	writer := NewRespWriter(conn)

	for {
		val, err := parser.Parse()
		if err != nil {
			// Connection closed or parse error
			return
		}

		// Extract command and arguments
		cmd, args, err := ExtractCommand(val)
		if err != nil {
			writer.WriteError("ERR " + err.Error())
			continue
		}

		// Dispatch command
		switch cmd {
		case "SET":
			s.cmdSet(writer, args)
		case "GET":
			s.cmdGet(writer, args)
		case "DEL":
			s.cmdDel(writer, args)
		case "PING":
			s.cmdPing(writer, args)
		case "FLUSH":
			s.cmdFlush(writer, args)
		case "COMPACT":
			s.cmdCompact(writer, args)
		case "QUIT":
			writer.WriteSimpleString("OK")
			return
		default:
			writer.WriteError(fmt.Sprintf("ERR unknown command '%s'", cmd))
		}
	}
}

// Command implementations

func (s *Server) cmdSet(w *RespWriter, args []string) {
	if len(args) < 2 {
		w.WriteError("ERR wrong number of arguments for 'set' command")
		return
	}
	key := args[0]
	value := args[1]
	var ttl time.Duration
	if len(args) >= 4 && strings.ToUpper(args[2]) == "EX" {
		ex, err := time.ParseDuration(args[3] + "s")
		if err != nil || ex <= 0 {
			w.WriteError("ERR invalid expire time")
			return
		}
		ttl = ex
	}
	if err := s.engine.Put([]byte(key), []byte(value), ttl); err != nil {
		w.WriteError("ERR " + err.Error())
		return
	}
	w.WriteSimpleString("OK")
}

func (s *Server) cmdGet(w *RespWriter, args []string) {
	if len(args) < 1 {
		w.WriteError("ERR wrong number of arguments for 'get' command")
		return
	}
	key := args[0]
	val, found, err := s.engine.Get([]byte(key))
	if err != nil {
		w.WriteError("ERR " + err.Error())
		return
	}
	if !found {
		w.WriteBulkStringBytes(nil) // nil bulk string
		return
	}
	w.WriteBulkStringBytes(val)
}

func (s *Server) cmdDel(w *RespWriter, args []string) {
	if len(args) < 1 {
		w.WriteError("ERR wrong number of arguments for 'del' command")
		return
	}
	key := args[0]
	if err := s.engine.Delete([]byte(key)); err != nil {
		w.WriteError("ERR " + err.Error())
		return
	}
	w.WriteInteger(1)
}

func (s *Server) cmdPing(w *RespWriter, args []string) {
	if len(args) == 0 {
		w.WriteSimpleString("PONG")
	} else {
		w.WriteBulkString(args[0])
	}
}

func (s *Server) cmdFlush(w *RespWriter, args []string) {
	if err := s.engine.FlushOnce(); err != nil {
		w.WriteError("ERR " + err.Error())
		return
	}
	w.WriteSimpleString("OK")
}

func (s *Server) cmdCompact(w *RespWriter, args []string) {
	if err := s.engine.Compact(); err != nil {
		w.WriteError("ERR " + err.Error())
		return
	}
	w.WriteSimpleString("OK")
}
