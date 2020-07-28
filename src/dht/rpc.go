package dht

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"time"
	"errors"
)

var TimeOutError error = errors.New("time out")
var InvalidAddressError error = errors.New("invalid address")

type Server struct {
	server *rpc.Server
	listener net.Listener
	node *ChordNode
}

func NewServer(nd *ChordNode) *Server {
	return &Server{node : nd}
}

func (s *Server) Launch() error {
	s.server = rpc.NewServer()
	if err := rpc.Register(s.node) ; err != nil {
		fmt.Println("Register fail: ", err)
		return err
	}

	lsn, err := net.Listen("tcp", s.node.address)
	if err != nil {
		fmt.Println("Listen fail: ", err)
		return err
	}

	s.listener = lsn
	s.node.Create()
	s.node.listening = true
	go s.server.Accept(s.listener)
	return nil
}

func (s *Server) Shutdown() {
	s.node.listening = false
	if err := s.listener.Close() ; err != nil {
		log.Println(err)
		return
	}
	fmt.Println(s.node.address , ": shutdown successfully.")
}

func (s *Server) CallFunc(client *rpc.Client, method string, args interface{}, reply interface{}) error {
	select {
	case call := <- client.Go(method, args, reply, make(chan *rpc.Call)).Done :
		return call.Error
	case <- time.After(500 * time.Millisecond) :
		return TimeOutError
	}
}

func (s *Server) CallFuncByAddress(address string, method string, args interface{}, reply interface{}) error {
	if address == "" {
		return InvalidAddressError
	}
	var client *rpc.Client
	dialError := make(chan error)
	go func() {
		var err error
		client, err = rpc.Dial("tcp", address)
		dialError <- err
	}()
	select {
		case err := <- dialError :
			if err != nil {
				return err
			}
		case <- time.After(500 * time.Millisecond) :
			return TimeOutError
	}
	return s.CallFunc(client, method, args, reply)
}