package main

/* In this file, you should implement function "NewNode" and
 * a struct which implements the interface "dhtNode".
 */

import (
	"dht"
)

func NewNode(port int) dhtNode {
	// Todo: create a node and then return it.
	node := dht.DHTNode{}
	node.SetPort(port)
	return &node
}
// Todo: implement a struct which implements the interface "dhtNode".
