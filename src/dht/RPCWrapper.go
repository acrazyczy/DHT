package dht

import "math/big"

type RPCWrapper struct {
	node *ChordNode
}

func (this *RPCWrapper) FindSuccessor(hashValue *big.Int, succaddr *string) error {
	return this.node.FindSuccessor(hashValue, succaddr)
}

func (this *RPCWrapper) GetSuccessor(_ int, list *[successorLen] string) error {
	return this.node.GetSuccessor(0, list)
}

func (this *RPCWrapper) SendBackup(backup map[string] string, _ *int) error {
	return this.node.SendBackup(backup, nil)
}

func (this *RPCWrapper) SplitIntoPredecessor(addr string, reply *map[string] string) error {
	return this.node.SplitIntoPredecessor(addr, reply)
}

func (this *RPCWrapper) RemoveFromBackup(backup map[string] string, _ *int) error {
	return this.node.RemoveFromBackup(backup, nil)
}

func (this *RPCWrapper) Put(kv KVPair, ok *bool) error {
	return this.node.Put(kv, ok)
}

func (this *RPCWrapper) PutOnBackup(kv KVPair, _ *int) error {
	return this.node.PutOnBackup(kv, nil)
}

func (this *RPCWrapper) Get(key string, value *string) error {
	return this.node.Get(key, value)
}

func (this *RPCWrapper) Delete(key string, value *string) error {
	return this.node.Delete(key, value)
}

func (this *RPCWrapper) DeleteOnBackup(key string, _ *int) error {
	return this.node.DeleteOnBackup(key, nil)
}

func (this *RPCWrapper) ReceiveData(_ int, data *map[string] string) error {
	return this.node.ReceiveData(0, data)
}

func (this *RPCWrapper) GetPredecessor(_ int, addr *string) error {
	return this.node.GetPredecessor(0, addr)
}

func (this *RPCWrapper) Notify(addr string, _ *int) error {
	return this.node.Notify(addr, nil)
}