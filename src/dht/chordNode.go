package dht

import (
	"math/big"
	"strconv"
	"sync"
	"time"
)

type ChordNode struct {
	address string
	listening bool

	data map[string] string
	dataLock sync.RWMutex

	successor string
	sucLock sync.RWMutex
	predecessor string
}

func NewChordNode(port int) *ChordNode{
	localIP := getLocalAddress()
	return &ChordNode {
		address : localIP + ":" + strconv.Itoa(port),
		data : make(map[string] string),
	}
}

func (this *ChordNode) Maintain() {
	go func() {
		for this.listening {
			this.Stabilize()
			time.Sleep(300 * time.Millisecond)
		}
	}()
	go func() {
		for this.listening {
			this.CheckPredecessor()
			time.Sleep(300 * time.Millisecond)
		}
	}()
}

func (this *ChordNode) Create() {
	this.predecessor = ""
	this.successor = this.address
}

func (this *ChordNode) Join(addr string) error {
	this.predecessor = ""
	hashValue := hashString(addr)
	var err error
	err = CallFuncByAddress(addr, "ChordNode.FindSuccessor", hashValue, &this.successor)
	return err
}

func (this *ChordNode) FindSuccessor(hashValue *big.Int, succaddr *string) error {
	if between(hashString(this.address), hashValue, hashString(this.successor), true) {
		*succaddr = this.successor
		return nil
	}
	return CallFuncByAddress(this.successor, "ChordNode.FindSuccessor", hashValue, succaddr)
}

func (this *ChordNode) Quit() {
	this.dataLock.Lock()
	
	this.dataLock.Unlock()
}