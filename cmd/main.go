package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/CodeWithBenji/a-simple-redis-clone/internals/handlers"
	"github.com/CodeWithBenji/a-simple-redis-clone/internals/resp"
	log "github.com/sirupsen/logrus"
)

type Server struct {
	wg         sync.WaitGroup
	listener   net.Listener
	shutdown   chan struct{}
	connection chan net.Conn
}

func NewServer() (*Server, error) {
	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		panic(err)
	}
	return &Server{
		listener:   listener,
		shutdown:   make(chan struct{}),
		connection: make(chan net.Conn),
	}, nil
}

func main() {
	log.Println("Server initalizing...")
	s, err := NewServer()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	s.Start()
	log.Println("Server listening on port 0.0.0.0:6379")
	// Wait for a SIGINT or SIGTERM signal to gracefully shut down the server
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("Shutting down server...")
	s.Stop()
	fmt.Println("Server stopped.")
}

func (s *Server) Start() {
	s.wg.Add(4)
	go s.acceptConnections()
	go s.handleConnections()
}

func (s *Server) Stop() {
	close(s.shutdown)
	s.listener.Close()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return
	case <-time.After(time.Second):
		fmt.Println("Timed out waiting for connections to finish.")
		return
	}
}
func (s *Server) acceptConnections() {
	defer s.wg.Done()
	for {
		select {
		case <-s.shutdown:
			return
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				continue
			}
			s.connection <- conn
		}
	}
}
func (s *Server) handleConnections() {
	defer s.wg.Done()
	for {
		select {
		case <-s.shutdown:
			return
		case conn := <-s.connection:
			go s.handleConnection(conn)
		}
	}
}
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		respone := resp.NewResponse(conn)
		value, err := respone.Read()
		if err != nil {
			fmt.Println(err)
			return
		}

		command := strings.ToUpper(value.Array[0].Bulk)
		args := value.Array[1:]

		writer := resp.NewWriter(conn)

		handler, ok := handlers.Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			writer.Write(resp.RespValue{Type: "string", String: ""})
			continue
		}

		result := handler(args)
		log.Println(result)
		writer.Write(result)

	}
}
