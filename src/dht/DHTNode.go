package dht

import (
	log "github.com/sirupsen/logrus"
	"os"
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
		log.Errorf("Cannot run node at %s.\n", this.node.address)
		return
	}
	this.node.Maintain()
	time.Sleep(maintainPeriod)
	log.Tracef("Successfully run %s.\n", this.node.address)
}

func (this *DHTNode) Create() {
	this.node.Create()
}

func (this *DHTNode) Join(addr string) bool {
	time.Sleep(halfMaintainPeriod)
	if err := this.node.Join(addr) ; err != nil {
		log.Errorln("First join attempt error.", err)
		time.Sleep(maintainPeriod)
		err = this.node.Join(addr)
		if err != nil {
			return false
		}
	}
	time.Sleep(maintainPeriod)
	return true
}

func (this *DHTNode) Quit() {
	this.ForceQuit()
}

func (this *DHTNode) ForceQuit() {
	this.server.Shutdown()
	this.node.Clear()
	log.Tracef("Force quit at node %s.\n", this.node.address)
	time.Sleep(maintainPeriod * 3)
}

func (this *DHTNode) Ping(addr string) bool {
	return CheckValidRPC(addr)
}

func (this *DHTNode) Put(key string, value string) bool {
	if this.node.listening == false {
		log.Errorf("%s not listening.\n", this.node.address)
		return false
	}
	var result bool
	if !this.node.PutOnChord(key, value) {
		time.Sleep(maintainPeriod)
		result = this.node.PutOnChord(key, value)
	} else {
		result = true
	}
	//time.Sleep(maintainPeriod)
	return result
}

func (this *DHTNode) Get(key string) (bool, string) {
	if this.node.listening == false {
		log.Errorf("%s not listening.\n", this.node.address)
		return false, ""
	}
	for trial := 0 ; trial < 3 ; trial ++ {
		ok, value := this.node.GetOnChord(key)
		if ok {
			return true, value
		}
		time.Sleep(maintainPeriod)
	}
	log.Warningf("Value of %s not found.\n", key)
	return false, ""
}

func (this *DHTNode) Delete(key string) bool {
	if this.node.listening == false {
		log.Errorf("%s not listening.\n", this.node.address)
		return false
	}
	ok, _ := this.node.DeleteOnChord(key)
	//time.Sleep(maintainPeriod)
	return ok
}

func (this *DHTNode) Dump(file *os.File) {
	if this.node.listening == false {
		log.Errorf("%s not listening.\n", this.node.address)
		return
	}
	this.node.Dump(file)
}