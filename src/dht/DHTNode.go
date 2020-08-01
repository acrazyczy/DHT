package dht

import (
	"fmt"
	"log"
	"time"
)

type DHTNode struct {
	node *ChordNode
	server *Server
}

func (this *DHTNode) SetPort(port int) {
	this.node = NewChordNode(port)
	this.Create()
}

func (this *DHTNode) Run() {
	this.server = NewServer(this.node)
	if err := this.server.Launch() ; err != nil {
		fmt.Printf("Cannot run node at %s.\n", this.node.address)
		return
	}
	this.node.Maintain()
	fmt.Printf("Successfully run %s.\n", this.node.address)
}

func (this *DHTNode) Create() {
	this.node.Create()
}

func (this *DHTNode) Join(addr string) {
	time.Sleep(500 * time.Millisecond)
	if err := this.node.Join(addr) ; err != nil {
		time.Sleep(500 * time.Millisecond)
		err = this.node.Join(addr)
		if err != nil {
			panic(err)
		}
	}
	time.Sleep(800 * time.Millisecond)
	fmt.Printf("Successfully join %s.\n", this.node.address)
}

func (this *DHTNode) Quit() {
	if this.node.listening == false {
		fmt.Printf("%s not listening.\n", this.node.address)
		return
	}
	this.server.Shutdown()
	this.node.Quit()
	fmt.Printf("Successfully quit %s.\n", this.node.address)
}

func (this *DHTNode) ForceQuit() {
	this.server.Shutdown()
	time.Sleep(500 * time.Millisecond)
}

func (this *DHTNode) Ping(addr string) bool {
	return CheckValidRPC(addr)
}

func (this *DHTNode) Put(key string, value string) bool {
	if this.node.listening == false {
		fmt.Printf("%s not listening.\n", this.node.address)
		return false
	}
	this.node.PutOnChord(key, value)
	go func() {
		time.Sleep(300 * time.Millisecond)
		this.node.PutOnChord(key, value)
	}()
	return true
}

func (this *DHTNode) Get(key string) (bool, string) {
	if this.node.listening == false {
		fmt.Printf("%s not listening.\n", this.node.address)
		return false, ""
	}
	for trial := 0 ; trial < 5 ; trial ++ {
		ok, value := this.node.GetOnChord(key)
		if ok {
			return true, value
		}
		time.Sleep(300 * time.Millisecond)
	}
	log.Printf("Value of %s not found.\n", key)
	return false, ""
}

func (this *DHTNode) Delete(key string) bool {
	if this.node.listening == false {
		fmt.Printf("%s not listening.\n", this.node.address)
		return false
	}
	ok, _ := this.node.DeleteOnChord(key)
	return ok
}

func (this *DHTNode) Dump() {
	if this.node.listening == false {
		fmt.Printf("%s not listening.\n", this.node.address)
		return
	}
	this.node.Dump()
}