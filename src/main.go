package main

import (
	"dht"
	"fmt"
	"strconv"
)

func NaiveTest() {
	const NodeCount int = 3
	const PortStart int = 14025
	var network [NodeCount] dhtNode
	var ports [NodeCount] int
	for i := 0 ; i < NodeCount ; i ++ {
		ports[i], network[i] = PortStart + i, NewNode(PortStart + i)
		network[i].Run()
	}
	fmt.Println("Successfully run the whole network.")
	localIP := dht.GetLocalAddress()
	network[0].Create()
	for i := 1 ; i < NodeCount ; i ++ {
		network[i].Join(localIP + ":" + strconv.Itoa(ports[0]))
		/*time.Sleep(time.Second)
		network[i].Dump()
		network[0].Dump()*/
	}
	const DataCount int = 5
	var data [DataCount] dht.KVPair
	for i := 0 ; i < DataCount ; i ++ {
		data[i] = dht.KVPair{Key: strconv.Itoa(i), Value: strconv.Itoa(i * i)}
		network[i % NodeCount].Put(data[i].Key, data[i].Value)
	}
	var (
		SuccessCount int = 0
		MistakeCount int = 0
		FailureCount int = 0
	)
	for i := DataCount - 1 ; i >= 0 ; i -- {
		ok, value := network[(i * i) % NodeCount].Get(data[i].Key)
		if !ok {
			fmt.Printf("Data {key: %s, value: %s} not found.\n", data[i].Key, data[i].Value)
			FailureCount ++
		} else if value != data[i].Value {
			fmt.Printf("Data {key: %s, value: %s} is replaced by data {key: %s, value: %s}.\n", data[i].Key, data[i].Value, data[i].Key, value)
			MistakeCount ++
		} else {
			fmt.Printf("Data {key: %s, value: %s} found.\n", data[i].Key, value)
			SuccessCount ++
		}
	}
	fmt.Printf("Success: %d, Mistake: %d, Failure: %d.\n\n", SuccessCount, MistakeCount, FailureCount)
	for i := 0 ; i < NodeCount ; i ++ {
		network[i].Dump()
		network[i].Quit()
		fmt.Println()
	}
}

func main() {
	/*file, err := os.OpenFile("DHT.log", os.O_RDWR | os.O_CREATE | os.O_APPEND, 666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer file.Close()
	log.SetOutput(file)*/

	NaiveTest()
}