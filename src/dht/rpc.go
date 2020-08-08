package dht

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"net"
	"net/rpc"
	"time"
)

var TimeOutError error = errors.New("time out")
var InvalidAddressError error = errors.New("invalid address")

type Server struct {
	server *rpc.Server
	listener net.Listener
	node *RPCWrapper
}

func NewServer(nd *ChordNode) *Server {
	return &Server{node : &RPCWrapper{nd}}
}

func (s *Server) Launch() error {
	s.server = rpc.NewServer()
	if err := s.server.Register(s.node) ; err != nil {
		log.Errorln("Register fail: ", err)
		return err
	}

	lsn, err := net.Listen("tcp", s.node.node.address)
	if err != nil {
		log.Errorln("Listen fail: ", err)
		return err
	}

	s.listener = lsn
	s.node.node.Create()
	s.node.node.listening = true
	go s.server.Accept(s.listener)
	return nil
}

func (s *Server) Shutdown() {
	s.node.node.listening = false
	if err := s.listener.Close() ; err != nil {
		log.Errorln(err)
		return
	}
	log.Traceln(s.node.node.address , ": shutdown successfully.")
}

func CallFunc(client *rpc.Client, method string, args interface{}, reply interface{}) error {
	select {
	case call := <- client.Go(method, args, reply, make(chan *rpc.Call, 1)).Done :
		return call.Error
	case <- time.After(2 * maintainPeriod) :
		return TimeOutError
	}
}

func GetClient(address string) (client *rpc.Client, err error) {
	if address == "" {
		return nil, InvalidAddressError
	}
	dialError := make(chan error)
	defer close(dialError)
	for trial := 0 ; trial < 2 ; trial ++ {
		go func() {
			var err error
			client, err = rpc.Dial("tcp", address)
			defer func() {
				if r := recover(); r != nil {
					log.Errorln("GetClient: ", r)
				}
			}()
			dialError <- err
		}()
		select {
		case err := <- dialError:
			if err != nil {
				return nil, err
			} else {
				return client, nil
			}
		case <- time.After(2 * maintainPeriod):
			log.Errorln("GetClient: ", TimeOutError)
		}
	}
	return nil, TimeOutError
}

func CallFuncByAddress(address string, method string, args interface{}, reply interface{}) error {
	client, err := GetClient(address)
	if err != nil {
		return err
	}
	return CallFunc(client, method, args, reply)
}

func CheckValidRPC(address string) bool {
	if address == "" {
		return false
	}
	dialError := make(chan error)
	defer close(dialError)
	for trial := 0 ; trial < 2 ; trial ++ {
		go func() {
			var err error
			_, err = rpc.Dial("tcp", address)
			defer func() {
				if r := recover(); r != nil {
					log.Errorln(r)
				}
			}()
			dialError <- err
		}()
		select {
		case err := <- dialError:
			if err == nil {
				return true
			} else {
				log.Errorln(err)
			}
		case <- time.After(maintainPeriod):
			log.Errorln(TimeOutError)
		}
		time.Sleep(halfMaintainPeriod)
	}
	log.Warningf("Connection trial to %s fail.\n", address)
	return false
}