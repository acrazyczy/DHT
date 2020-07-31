package dht

import (
	"fmt"
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
	succLock sync.RWMutex
	predecessor string
}

func NewChordNode(port int) *ChordNode{
	localIP := GetLocalAddress()
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
	err = CallFuncByAddress(addr, "RPCWrapper.FindSuccessor", hashValue, &this.successor)
	return err
}

func (this *ChordNode) FindSuccessor(hashValue *big.Int, succaddr *string) error {
	//fmt.Printf("Try to findsuccessor at %s.\n", this.address)
	if between(hashString(this.address), hashValue, hashString(this.successor), true) {
		*succaddr = this.successor
		return nil
	}
	return CallFuncByAddress(this.successor, "RPCWrapper.FindSuccessor", hashValue, succaddr)
}

func (this *ChordNode) Quit() {
	time.Sleep(1000 * time.Millisecond)
	this.MergeToSuccessor()
	this.QuitNotify()
	time.Sleep(1000 * time.Millisecond)
}

func (this *ChordNode) MergeToSuccessor() {
	this.dataLock.Lock()
	var reply int = 0
	err := CallFuncByAddress(this.successor, "RPCWrapper.ReceiveData", this.data, &reply)
	if err != nil || reply != len(this.data) {
		log.Printf("Fail to merge %s to its successor.\n", this.address)
	} else {
		log.Printf("Successfully merge %s to its successor.\n", this.address)
	}
	this.dataLock.Unlock()
}

func (this *ChordNode) QuitNotify() {
	if this.successor == this.address {
		return
	}
	if err := CallFuncByAddress(this.successor, "RPCWrapper.QuitNotifyByPredecessor", this.predecessor, nil) ; err != nil {
		log.Println(err)
	}
	if err := CallFuncByAddress(this.predecessor, "RPCWrapper.QuitNotifyBySuccessor", this.successor, nil) ; err != nil {
		log.Println(err)
	}
}

func (this *ChordNode) QuitNotifyByPredecessor(addr string, _ *int) error {
	this.predecessor = addr
	return nil
}

func (this *ChordNode) QuitNotifyBySuccessor(addr string, _ *int) error {
	this.succLock.Lock()
	this.successor = addr
	this.succLock.Unlock()
	return nil
}

func (this *ChordNode) ReceiveData(data map[string] string, count *int) error {
	this.dataLock.Lock()
	*count = 0
	for key, value := range data {
		this.data[key] = value
		*count ++
	}
	this.dataLock.Unlock()
	return nil
}

type KVPair struct {
	Key, Value string
}

func (this *ChordNode) PutOnChord(key string, value string) bool {
	var addr string
	err := this.FindSuccessor(hashString(key), &addr)
	if err != nil {
		return false
	}
	var ok bool
	err = CallFuncByAddress(addr, "RPCWrapper.Put", KVPair{Key: key, Value: value}, &ok)
	return err == nil && ok
}

func (this *ChordNode) Put(kv KVPair, ok *bool) error {
	this.dataLock.Lock()
	this.data[kv.Key], *ok = kv.Value, true
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
	err = CallFuncByAddress(addr, "RPCWrapper.Get", key, &value)
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
	err = CallFuncByAddress(addr, "RPCWrapper.Delete", key, &value)
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

func (this *ChordNode) Stabilize() {
	this.succLock.Lock()
	client, err := GetClient(this.successor)
	if client == nil {
		return
	}
	var addr string
	err_ := CallFunc(client, "RPCWrapper.GetPredecessor", 0, &addr)
	if err_ != nil {
		err_ = CallFunc(client, "RPCWrapper.GetPredecessor", 0, &addr)
	}
	if err_ == nil {
		if addr != "" && between(hashString(this.address), hashString(addr), hashString(this.successor), false) {
			this.successor = addr
			client, err = GetClient(this.successor)
		}
	} else {
		log.Println(err_)
	}
	this.succLock.Unlock()
	if err == nil {
		var ok bool
		err_ = CallFunc(client, "RPCWrapper.Notify", this.address, &ok)
		if ok {
			log.Printf("Notify %s at %s.\n", this.address, this.successor)
		}
		if err_ != nil {
			log.Println(err_)
		}
	}
}

func (this *ChordNode) Notify(addr string, ok *bool) error {
	if this.predecessor == "" || between(hashString(this.predecessor), hashString(addr), hashString(this.address), false) {
		this.predecessor, *ok = addr, true
	} else {
		*ok = false
	}
	return nil
}

func (this *ChordNode) GetPredecessor(_ int, addr *string) error {
	*addr = this.predecessor
	return nil
}

func (this *ChordNode) CheckPredecessor() {
	if this.predecessor != "" && !CheckValidRPC(this.predecessor) {
		this.predecessor = ""
	}
}

func (this *ChordNode) Dump() {
	fmt.Printf("Dumping node at %s.\n", this.address)
	fmt.Printf("Predecessor: %s\n", this.predecessor)
	fmt.Printf("Successor: %s\n", this.successor)
	fmt.Print("Data: {")
	for key, value := range this.data {
		fmt.Printf("{%s: %s}, ", key, value)
	}
	fmt.Printf("}\n")
}