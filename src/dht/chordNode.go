package dht

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"math/big"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const fingerLen int = 160
const successorLen int = 5
const maintainPeriod time.Duration = 250 * time.Millisecond
const halfMaintainPeriod time.Duration = 125 * time.Millisecond

type ChordNode struct {
	address string
	listening bool

	data map[string] string
	dataLock sync.RWMutex

	backup map[string] string
	backupLock sync.Mutex

	successor [successorLen] string
	succLock sync.RWMutex
	predecessor string

	finger [fingerLen] string
	next int
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
			time.Sleep(maintainPeriod)
		}
	}()
	go func() {
		for this.listening {
			this.CheckPredecessor()
			time.Sleep(maintainPeriod)
		}
	}()
	go func() {
		for this.listening {
			this.FixFingers()
			time.Sleep(maintainPeriod)
		}
	}()
}

func (this *ChordNode) Create() {
	this.predecessor = ""
	this.successor[0] = this.address
}

func (this *ChordNode) Join(addr string) error {
	log.Tracef("Start to join %s.\n", this.address)

	this.predecessor = ""
	hashValue := hashString(this.address)
	client, _ := GetClient(addr)

	var err error
	var suc string

	if client == nil {
		log.Fatalln("Join 1: null pointer.")
	}

	err = CallFunc(client, "RPCWrapper.FindSuccessor", hashValue, &suc)
	if err != nil {
		client.Close()
		return err
	}

	this.succLock.Lock()
	this.successor[0] = suc
	defer this.succLock.Unlock()

	log.Tracef("Find successor of %s: %s.\n", this.address, this.successor[0])

	client.Close()
	client, _ = GetClient(this.successor[0])
	defer client.Close()

	var list [successorLen] string
	log.Tracef("Try to get successor list of %s from %s.\n", this.address, this.successor[0])
	if client == nil {
		log.Fatalln("Join 2: null pointer.")
	}
	err = CallFunc(client, "RPCWrapper.GetSuccessor", 0, &list)
	if err != nil {
		this.successor[0] = this.address
		return err
	}
	for i := 1 ; i < successorLen ; i ++ {
		this.successor[i] = list[i - 1]
	}

	log.Tracef("Get successor list of %s.\n", this.address)

	if client == nil {
		log.Fatalln("Join 3: null pointer.")
	}
	this.dataLock.Lock()
	log.Traceln("{")
	err = CallFunc(client, "RPCWrapper.SplitIntoPredecessor", this.address, &this.data)
	log.Traceln("}")
	this.dataLock.Unlock()
	log.Tracef("Split done: %s.\n", this.address)
	if err != nil {
		this.successor[0] = this.address
		return err
	}

	log.Tracef("Successfully join %s.\n", this.address)

	return nil
}

func (this *ChordNode) GetSuccessor(_ int, reply *[successorLen] string) error {
	this.succLock.Lock()
	*reply = this.successor
	this.succLock.Unlock()
	return nil
}

func (this *ChordNode) SplitIntoPredecessor(addr string, reply *map[string] string) error {
	hashValue := hashString(addr)
	this.predecessor = addr
	this.dataLock.Lock()
	for key, value := range this.data {
		if !between(hashValue, hashString(key), hashString(this.address), true) {
			(*reply)[key] = value
			delete(this.data, key)
		}
	}
	this.dataLock.Unlock()
	err := CallFuncByAddress(this.successor[0], "RPCWrapper.RemoveFromBackup", *reply, nil)
	this.backupLock.Lock()
	this.backup = *reply
	this.backupLock.Unlock()
	return err
}

func (this *ChordNode) RemoveFromBackup(backup map[string] string, _ *int) error {
	this.backupLock.Lock()
	for key, _ := range backup {
		delete(this.backup, key)
	}
	this.backupLock.Unlock()
	return nil
}

func (this *ChordNode) FindSuccessor(hashValue *big.Int, succaddr *string) error {
	if suc := this.FirstValidSuccessor() ; between(hashString(this.address), hashValue, hashString(suc), true) {
		*succaddr = suc
		return nil
	}
	jump := this.ClosestPrecedingNode(hashValue)
	if jump == nil {
		log.Fatalln("FindSuccessor: null pointer.")
	} else {
		defer jump.Close()
	}
	return CallFunc(jump, "RPCWrapper.FindSuccessor", hashValue, succaddr)
}

func (this *ChordNode) ClosestPrecedingNode(hashValue *big.Int) *rpc.Client {
	start := hashString(this.address)
	for i := fingerLen - 1 ; i >= 0 ; i -- {
		if this.finger[i] == "" || !between(start, hashString(this.finger[i]), hashValue, false) {
			continue
		}
		client, err := GetClient(this.finger[i])
		if err != nil {
			continue
		}
		return client
	}
	client, _ := GetClient(this.FirstValidSuccessor())
	return client
}

func (this *ChordNode) FirstValidSuccessor() string {
	for i := 0 ; i < successorLen ; i ++ {
		if CheckValidRPC(this.successor[i]) {
			return this.successor[i]
		}
	}
	return ""
}

func (this *ChordNode) SendBackup(backup map[string] string, _ *int) error {
	this.backupLock.Lock()
	for key, value := range backup {
		this.backup[key] = value
	}
	this.backupLock.Unlock()
	return nil
}

type KVPair struct {
	Key, Value string
}

func (this *ChordNode) PutOnChord(key string, value string) bool {
	log.Tracef("Try to put key %s on chord.\n", key)
	var addr string
	err := this.FindSuccessor(hashString(key), &addr)
	if err != nil {
		return false
	}
	var ok bool
	log.Tracef("Get put address : %s.\n", addr)
	err = CallFuncByAddress(addr, "RPCWrapper.Put", KVPair{Key: key, Value: value}, &ok)
	return err == nil && ok
}

func (this *ChordNode) Put(kv KVPair, ok *bool) error {
	err := CallFuncByAddress(this.successor[0], "RPCWrapper.PutOnBackup", kv, nil)
	if err != nil {
		*ok = false
		return err
	}
	this.dataLock.Lock()
	this.data[kv.Key], *ok = kv.Value, true
	this.dataLock.Unlock()
	return nil
}

func (this *ChordNode) PutOnBackup(kv KVPair, _ *int) error {
	this.backupLock.Lock()
	this.backup[kv.Key] = kv.Value
	this.backupLock.Unlock()
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
	log.Tracef("Try to delete key %s on chord.\n", key)
	var addr string
	err := this.FindSuccessor(hashString(key), &addr)
	if err != nil {
		return false, ""
	}
	var value string
	log.Tracef("Get delete address : %s.'n", addr)
	err = CallFuncByAddress(addr, "RPCWrapper.Delete", key, &value)
	return err == nil, value
}

var DeleteNonExistenceError error = errors.New("delete an element that doesn't exist")

func (this *ChordNode) Delete(key string, value *string) error {
	err := CallFuncByAddress(this.successor[0], "RPCWrapper.DeleteOnBackup", key, nil)
	if err != nil {
		return err
	}
	this.dataLock.Lock()
	var ok bool
	*value, ok = this.data[key]
	if !ok {
		return DeleteNonExistenceError
	} else {
		delete(this.data, key)
	}
	this.dataLock.Unlock()
	return nil
}

func (this *ChordNode) DeleteOnBackup(key string, _ *int) error {
	this.backupLock.Lock()
	defer this.backupLock.Unlock()
	if _, ok := this.backup[key] ; ok {
		delete(this.backup, key)
	} else {
		return DeleteNonExistenceError
	}
	return nil
}

func (this *ChordNode) Stabilize() {
	log.Tracef("Stabilize at %s.\n", this.address)
	defer log.Tracef("Stabilization at %s ends.\n", this.address)
	suc := this.FirstValidSuccessor()
	log.Tracef("first valid successor: %s.\n", suc)
	client, err := GetClient(suc)
	if err != nil {
		return
	}
	var addr string
	if client == nil {
		log.Fatalln("Stabilize 1: null pointer.")
	}
	err_ := CallFunc(client, "RPCWrapper.GetPredecessor", 0, &addr)
	log.Tracef("predecessor: %s.\n", addr)
	if err_ != nil {
		err_ = CallFunc(client, "RPCWrapper.GetPredecessor", 0, &addr)
	}
	if err_ == nil {
		if addr != "" && between(hashString(this.address), hashString(addr), hashString(suc), false) {
			client.Close()
			client, err = GetClient(addr)
			if err != nil {
				client, err = GetClient(suc)
				addr = ""
				if err != nil {
					return
				}
			}
		} else {
			addr = ""
		}
	} else {
		log.Errorln("Stabilize1: ", err_)
	}
	if client == nil {
		log.Fatalln("Stabilize 2: null pointer.")
	}
	var list [successorLen] string
	err = CallFunc(client, "RPCWrapper.GetSuccessor", 0, &list)
	if err != nil {
		log.Errorln("Stabilize2: ", err)
		client.Close()
		return
	}
	this.succLock.Lock()
	if addr != "" {
		this.successor[0] = addr
		log.Tracef("The successor of node %s has been changed to %s.\n", this.address, addr)
	} else {
		this.successor[0] = suc
		log.Tracef("The successor of node %s has been changed to %s.\n", this.address, suc)
	}
	for i := 1 ; i < successorLen ; i ++ {
		this.successor[i] = list[i - 1]
	}
	if client == nil {
		log.Fatalln("Stabilize 3: null pointer.")
	}
	err_ = CallFunc(client, "RPCWrapper.Notify", this.address, nil)
	if err_ != nil {
		log.Errorln("Stabilize3: ", err_)
	}
	this.succLock.Unlock()
	client.Close()
}

func (this *ChordNode) Notify(addr string, _ *int) error {
	if this.predecessor == "" || this.predecessor != addr && between(hashString(this.predecessor), hashString(addr), hashString(this.address), true) {
		log.Tracef("The predecessor of node %s has been changed from %s to %s.\n", this.address, this.predecessor, addr)
		this.predecessor = addr
		err := CallFuncByAddress(addr, "RPCWrapper.ReceiveData", 0, &this.backup)
		if err != nil {
			log.Errorln("Notify: ", err)
		}
	}
	return nil
}

func (this *ChordNode) ReceiveData(_ int, data *map[string] string) error {
	this.dataLock.Lock()
	*data = this.data
	this.dataLock.Unlock()
	return nil
}

func (this *ChordNode) GetPredecessor(_ int, addr *string) error {
	*addr = this.predecessor
	return nil
}

func (this *ChordNode) CheckPredecessor() {
	if this.predecessor != "" && !CheckValidRPC(this.predecessor) {
		log.Warningf("Node %s, predecessor of node %s, has failed.\n", this.predecessor, this.address)
		this.EnableBackup()
		this.predecessor = ""
	}
}

func (this *ChordNode) EnableBackup() {
	this.backupLock.Lock()
	this.dataLock.Lock()
	for key, value := range this.backup {
		this.data[key] = value
	}
	this.dataLock.Unlock()
	client, err := GetClient(this.FirstValidSuccessor())
	if err == nil {
		if client == nil {
			log.Fatalln("EnableBackup: null pointer.")
		}
		err = CallFunc(client, "RPCWrapper.SendBackup", this.backup, nil)
		client.Close()
	}
	if err != nil {
		log.Errorln("EnableBackup: ", err)
	}
	this.backup = make(map[string] string)
	this.backupLock.Unlock()
}

func (this *ChordNode) FixFingers() {
	err := this.FindSuccessor(jump(this.address, this.next), &this.finger[this.next])
	if err != nil {
		log.Errorln("FixFingers: ", err)
	}
	this.next = (this.next + 1) % fingerLen
}

func (this *ChordNode) Clear() {
	this.dataLock.Lock()
	this.data = make(map[string] string)
	this.dataLock.Unlock()
	this.backupLock.Lock()
	this.backup = make(map[string] string)
	this.backupLock.Unlock()
}

func (this *ChordNode) Dump(file *os.File) {
	this.dataLock.Lock()
	for key, value := range this.data {
		fmt.Fprintf(file, "%s %s\n", key, value)
	}
	this.dataLock.Unlock()
}