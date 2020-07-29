package dht

import (
	"log"
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
	time.Sleep(500 * time.Millisecond)
	this.MergeToSuccessor()
	this.dataLock.Unlock()
}

func (this *ChordNode) MergeToSuccessor() {
	this.dataLock.Lock()
	var reply int = 0
	err := CallFuncByAddress(this.successor, "ChordNode.ReceiveData", this.data, &reply)
	if reply != len(this.data) || err != nil {
		log.Printf("Fail to merge %s to its successor.\n", this.address)
	} else {
		log.Printf("Successfully merge %s to its successor.\n", this.address)
	}
	this.dataLock.Unlock()
}

func (this *ChordNode) ReceiveData(data map[string] string, count *int) error {
	this.dataLock.Lock()
	for key, value := range data {
		this.data[key] = value
		*count ++
	}
	this.dataLock.Unlock()
	return nil
}

type KVPair struct {
	key, value string
}

func (this *ChordNode) PutOnChord(key string, value string) bool {
	var addr string
	err := this.FindSuccessor(hashString(key), &addr)
	if err != nil {
		return false
	}
	var ok bool
	err = CallFuncByAddress(addr, "ChordNode.Put", KVPair{key, value}, &ok)
	return err == nil && ok
}

func (this *ChordNode) Put(kv KVPair, ok *bool) error {
	this.dataLock.Lock()
	this.data[kv.key], *ok = kv.value, true
	this.dataLock.Unlock()
	return nil
}

func (this *ChordNode) GetOnChord(key string) (bool, string) {
	var addr string
	err := this.FindSuccessor(hashString(key), &addr)
	if err != nil {
		return false, ""
	}
	var value string
	err = CallFuncByAddress(addr, "ChordNode.Get", key, &value)
	return err == nil && value != "", value
}

func (this *ChordNode) Get(key string, value *string) error {
	this.dataLock.RLock()
	var ok bool
	*value, ok = this.data[key]
	if !ok {
		*value = ""
	}
	this.dataLock.RUnlock()
	return nil
}

func (this *ChordNode) DeleteOnChord(key string) (bool, string) {
	var addr string
	err := this.FindSuccessor(hashString(key), &addr)
	if err != nil {
		return false, ""
	}
	var value string
	err = CallFuncByAddress(addr, "ChordNode.Delete", key, &value)
	return err == nil && value != "", value
}

func (this *ChordNode) Delete(key string, value *string) error {
	this.dataLock.Lock()
	var ok bool
	*value, ok = this.data[key]
	if !ok {
		*value = ""
	} else {
		delete(this.data, key)
	}
	this.dataLock.Unlock()
	return nil
}