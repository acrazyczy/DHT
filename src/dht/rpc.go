package dht

import (
	"fmt"
	"log"
	"math/big"
	"net"
	"net/rpc"
	"time"
	"errors"
)

var TimeOutError error = errors.New("time out")
var InvalidAddressError error = errors.New("invalid address")

type RPCWrapper struct {
	node *ChordNode
}

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
		fmt.Println("Register fail: ", err)
		return err
	}

	lsn, err := net.Listen("tcp", s.node.node.address)
	if err != nil {
		fmt.Println("Listen fail: ", err)
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
		log.Println(err)
		return
	}
	fmt.Println(s.node.node.address , ": shutdown successfully.")
}

func CallFunc(client *rpc.Client, method string, args interface{}, reply interface{}) error {
	select {
	case call := <- client.Go(method, args, reply, make(chan *rpc.Call, 1)).Done :
		return call.Error
	case <- time.After(time.Second) :
		return TimeOutError
	}
}

func GetClient(address string) (client *rpc.Client, err error) {
	if address == "" {
		return nil, InvalidAddressError
	}
	dialError := make(chan error)
	defer close(dialError)
	go func() {
		var err error
		client, err = rpc.Dial("tcp", address)
		defer func() {
			if r := recover(); r != nil {
				log.Println(r)
			}
		}()
		dialError <- err
	}()
	select {
	case err := <- dialError :
		if err != nil {
			return nil, err
		}
	case <- time.After(500 * time.Millisecond) :
		return nil, TimeOutError
	}
	return client, err
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
	//fmt.Printf("Try to connect to %s.\n", address)
	for trial := 0 ; trial < 2 ; trial ++ {
		go func() {
			var err error
			_, err = rpc.Dial("tcp", address)
			defer func() {
				if r := recover(); r != nil {
					log.Println(r)
				}
			}()
			dialError <- err
		}()
		select {
		case err := <- dialError:
			if err == nil {
				//fmt.Printf("Connect trial to %s succeed.\n", address)
				return true
			} else {
				log.Println(err)
			}
		case <- time.After(100 * time.Millisecond):
			log.Println(TimeOutError)
		}
		time.Sleep(50 * time.Millisecond)
	}
	//fmt.Printf("Connection trial to %s fail.\n", address)
	return false
}

func (this *RPCWrapper) FindSuccessor(hashValue *big.Int, succaddr *string) error {
	return this.node.FindSuccessor(hashValue, succaddr)
}

func (this *RPCWrapper) ReceiveData(data map[string] string, count *int) error {
	return this.node.ReceiveData(data, count)
}

func (this *RPCWrapper) Put(kv KVPair, ok *bool) error {
	return this.node.Put(kv, ok)
}

func (this *RPCWrapper) Get(key string, value *string) error {
	return this.node.Get(key, value)
}

func (this *RPCWrapper) Delete(key string, value *string) error {
	return this.node.Delete(key, value)
}

func (this *RPCWrapper) GetPredecessor(_ int, addr *string) error {
	return this.node.GetPredecessor(0, addr)
}

func (this *RPCWrapper) Notify(addr string, ok *bool) error {
	return this.node.Notify(addr, ok)
}

func (this *RPCWrapper) QuitNotifyByPredecessor(addr string, _ *int) error {
	return this.node.QuitNotifyByPredecessor(addr, nil)
}

func (this *RPCWrapper) QuitNotifyBySuccessor(addr string, _ *int) error {
	return this.node.QuitNotifyBySuccessor(addr, nil)
}