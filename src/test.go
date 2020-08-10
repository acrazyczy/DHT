package main

import (
	"dht"
	"fmt"
	log "github.com/sirupsen/logrus"
	easy_formatter "github.com/t-tomalak/logrus-easy-formatter"
	"os"
	"strconv"
	"strings"
	"time"
)

func NaiveTest() {
	log.SetFormatter(&easy_formatter.Formatter{
		TimestampFormat: "2006-01-02 15:04:05.000",
		LogFormat:       "[%lvl%]: %time% - %msg%\n",
	})
	log.SetLevel(log.TraceLevel)
	file, err := os.OpenFile("log/log_"+strings.ReplaceAll(time.Now().Format(time.Stamp)," ","-")+".log", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	log.SetOutput(file)
	const NodeCount int = 50
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
		if !network[i].Join(localIP + ":" + strconv.Itoa(ports[i - 1])) {
			log.Panicf("Fail to join %d.\n", ports[i])
		}
	}
	//time.Sleep(5 * time.Second)
	fmt.Println("Put test begins.")
	const DataCount int = 1000
	var data [DataCount] dht.KVPair
	for i := 0 ; i < DataCount ; i ++ {
		data[i] = dht.KVPair{Key: strconv.Itoa(i), Value: strconv.Itoa(i * i)}
		network[(i * i) % NodeCount].Put(data[i].Key, data[i].Value)
	}
	fmt.Println("Put test ends.")

	fmt.Println("Get test begins.")
	var (
		SuccessCount int = 0
		MistakeCount int = 0
		FailureCount int = 0
	)
	for i := DataCount - 1 ; i >= 0 ; i -- {
		ok, value := network[(i * i * i) % NodeCount].Get(data[i].Key)
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
	fmt.Println("Get test ends.")

	for i := DataCount / 5 ; i < DataCount ; i ++ {
		network[(i * i * i / 2) % NodeCount].Delete(data[i].Key)
	}

	fmt.Println("Get test begins.")
	SuccessCount, MistakeCount, FailureCount = 0, 0, 0
	for i := 0 ; i < DataCount / 5 ; i ++ {
		ok, value := network[(i * i * i / 3) % NodeCount].Get(data[i].Key)
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
	fmt.Println("Get test ends.")

	for i := 0 ; i < NodeCount ; i ++ {
		network[i].Quit()
	}
}

func main_() {
	/*file, err := os.OpenFile("DHT.log", os.O_RDWR | os.O_CREATE | os.O_APPEND, 666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer file.Close()
	log.SetOutput(file)*/

	NaiveTest()
}
