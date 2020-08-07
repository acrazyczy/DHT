package dht

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"math/big"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

const fingerLen int = 160
const successorLen int = 5
const maintainPeriod time.Duration = 300 * time.Millisecond
const halfMaintainPeriod time.Duration = 150 * time.Millisecond

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
	err = CallFunc(client, "RPCWrapper.GetSuccessor", 0, &list)
	if err != nil {
		return err
	}
	for i := 1 ; i < successorLen ; i ++ {
		this.successor[i] = list[i - 1]
	}

	log.Tracef("Get successor list of %s.\n", this.address)

	/*this.backupLock.Lock()
	err = CallFunc(client, "RPCWrapper.ReceiveAndDeleteBackup", 0, &this.backup)
	this.backupLock.Unlock()
	if err != nil {
		return err
	}*/

	this.dataLock.Lock()
	err = CallFunc(client, "RPCWrapper.SplitIntoPredecessor", hashValue, &this.data)
	this.dataLock.Unlock()
	log.Tracef("Split done: %s.\n", this.address)
	if err != nil {
		return err
	}
	log.Tracef("%s tries to notify %s.\n", this.address, this.successor[0])
	err = CallFunc(client, "RPCWrapper.Notify", this.address, nil)
	if err != nil {
		log.Errorln("Join: ", err)
	}

	log.Tracef("Successfully join %s.\n", this.address)

	return err
}

func (this *ChordNode) GetSuccessor(_ int, reply *[successorLen] string) error {
	this.succLock.Lock()
	*reply = this.successor
	this.succLock.Unlock()
	return nil
}

/*func (this *ChordNode) ReceiveAndDeleteBackup(_ int, backup *map[string] string) error {
	this.backupLock.Lock()
	*backup = this.backup
	this.backup = make(map[string] string)
	this.backupLock.Unlock()
	return nil
}*/

func (this *ChordNode) SplitIntoPredecessor(hashValue *big.Int, reply *map[string] string) error {
	this.dataLock.Lock()
	for key, value := range this.data {
		if between(hashValue, hashString(key), hashString(this.address), true) {
			(*reply)[key] = value
			delete(this.data, key)
		}
	}
	err := CallFuncByAddress(this.successor[0], "RPCWrapper.RemoveFromBackup", *reply, nil)
	this.dataLock.Unlock()
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
	//log.Tracef("FindSuccessor at %s.\n", this.address)
	if suc := this.FirstValidSuccessor() ; between(hashString(this.address), hashValue, hashString(suc), true) {
		*succaddr = suc
		return nil
	}
	jump := this.ClosestPrecedingNode(hashValue)
	return CallFunc(jump, "RPCWrapper.FindSuccessor", hashValue, succaddr)
}

func (this *ChordNode) ClosestPrecedingNode(hashValue *big.Int) *rpc.Client {
	/*start := hashString(this.address)
	for i := fingerLen - 1 ; i >= 0 ; i -- {
		if this.finger[i] == "" || !between(start, hashString(this.finger[i]), hashValue, false) {
			continue
		}
		client, err := GetClient(this.finger[i])
		if err != nil {
			continue
		}
		return client
	}*/
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

/*func (this *ChordNode) Quit() {
	time.Sleep(800 * time.Millisecond)
	this.MergeToSuccessor()
	this.QuitNotify()
	time.Sleep(800 * time.Millisecond)
}

func (this *ChordNode) MergeToSuccessor() {
	suc := this.FirstValidSuccessor()
	if suc == this.address {
		return
	}
	this.dataLock.Lock()
	err := CallFuncByAddress(suc, "RPCWrapper.SendData", this.data, nil)
	if err != nil {
		log.Println(err)
	}
	this.dataLock.Unlock()
	this.backupLock.Lock()
	err = CallFuncByAddress(suc, "RPCWrapper.ReplaceBackup", this.backup, nil)
	if err != nil {
		log.Println(err)
	}
	this.backupLock.Unlock()
}

func (this *ChordNode) QuitNotify() {
	suc := this.FirstValidSuccessor()
	if suc == this.address {
		return
	}

	if err := CallFuncByAddress(suc, "RPCWrapper.QuitNotifyByPredecessor", this.predecessor, nil) ; err != nil {
		log.Println(err)
	}
	if err := CallFuncByAddress(this.predecessor, "RPCWrapper.QuitNotifyBySuccessor", this.successor, nil) ; err != nil {
		log.Println(err)
	}
}

func (this *ChordNode) QuitNotifyByPredecessor(addr string, _ *int) error {
	//log.Printf("While quiting, change the predecessor of %s from %s to %s.\n", this.address, this.predecessor, addr)
	this.predecessor = addr
	return nil
}

func (this *ChordNode) QuitNotifyBySuccessor(addr string, _ *int) error {
	this.succLock.Lock()
	//log.Printf("While quiting, change the successor of %s from %s to %s.\n", this.address, this.successor, addr)
	this.successor[0] = addr
	this.succLock.Unlock()
	return nil
}

func (this *ChordNode) SendData(data map[string] string, _ *int) error {
	this.dataLock.Lock()
	for key, value := range data {
		this.data[key] = value
	}
	err := CallFuncByAddress(this.successor[0], "RPCWrapper.SendBackup", data, nil)
	this.dataLock.Unlock()
	return err
}

func (this *ChordNode) ReplaceBackup(backup map[string] string, _ *int) error {
	this.backupLock.Lock()
	this.backup = backup
	this.backupLock.Unlock()
}*/

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
	err := CallFuncByAddress(this.successor[0], "RPCWrapper.PutOnBackup", kv, nil)
	if err != nil {
		log.Errorln("Put: ", err)
	}
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
	err := CallFuncByAddress(this.successor[0], "RPCWrapper.DeleteOnBackup", key, nil)
	if err != nil {
		log.Errorln("Delete: ", err)
	}
	return nil
}

func (this *ChordNode) DeleteOnBackup(key string, _ *int) error {
	this.backupLock.Lock()
	delete(this.backup, key)
	this.backupLock.Unlock()
	return nil
}

func (this *ChordNode) Stabilize() {
	log.Tracef("Stabilize at %s.\n", this.address)
	defer log.Tracef("Stabilization at %s ends.\n", this.address)
	suc := this.FirstValidSuccessor()
	client, err := GetClient(suc)
	if client == nil {
		return
	}
	var addr string
	err_ := CallFunc(client, "RPCWrapper.GetPredecessor", 0, &addr)
	if err_ != nil {
		err_ = CallFunc(client, "RPCWrapper.GetPredecessor", 0, &addr)
	}
	if err_ == nil {
		if addr != "" && between(hashString(this.address), hashString(addr), hashString(suc), false) {
			client.Close()
			client, err = GetClient(addr)
			if err != nil {
				return
			}
		} else {
			addr = ""
		}
	} else {
		log.Errorln("Stabilize1: ", err_)
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
	}
	for i := 1 ; i < successorLen ; i ++ {
		this.successor[i] = list[i - 1]
	}
	if client != nil {
		log.Tracef("Try to notify %s.\n", this.successor[0])
		err_ = CallFunc(client, "RPCWrapper.Notify", this.address, nil)
		if err_ != nil {
			log.Errorln("Stabilize3: ", err_)
		}
	}
	this.succLock.Unlock()
	client.Close()
}

func (this *ChordNode) Notify(addr string, _ *int) error {
	if this.predecessor == "" || addr != this.address && between(hashString(this.predecessor), hashString(addr), hashString(this.address), false) {
		log.Tracef("The predecessor of node %s has been changed from %s to %s.\n", this.address, this.predecessor, addr)
		log.Traceln(this.predecessor, addr, this.address)
		log.Traceln(*hashString(this.predecessor), *hashString(addr), *hashString(this.address))
		this.predecessor = addr
		err := CallFuncByAddress(addr, "RPCWrapper.ReceiveData", 0, &this.backup)
		if err != nil {
			log.Errorln("Notify: ", err)
		}
	} else {
		log.Tracef("Notify failed.\n")
		if this.predecessor != " " {
			log.Traceln(this.predecessor, addr, this.address)
			log.Traceln(*hashString(this.predecessor), *hashString(addr), *hashString(this.address))
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
	client, err := GetClient(this.FirstValidSuccessor())
	defer client.Close()
	if err == nil {
		err = CallFunc(client, "RPCWrapper.SendBackup", this.backup, nil)
	}
	if err != nil {
		log.Errorln("EnableBackup: ", err)
	}
	this.backup = make(map[string] string)
	this.backupLock.Unlock()
	this.dataLock.Unlock()
}

func (this *ChordNode) FixFingers() {
	err := this.FindSuccessor(jump(this.address, this.next), &this.finger[this.next])
	if err != nil {
		log.Errorln("FixFingers: ", err)
	}
	this.next = (this.next + 1) % fingerLen
}

func (this *ChordNode) Dump() {
	fmt.Printf("Dumping node at %s.\n", this.address)
	fmt.Printf("Predecessor: %s\n", this.predecessor)
	fmt.Printf("Successor: %s\n", this.successor)
	/*fmt.Print("Data: {")
	for key, value := range this.data {
		fmt.Printf("{%s: %s}, ", key, value)
	}
	fmt.Printf("}\n")*/
}