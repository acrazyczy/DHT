package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	easy_formatter "github.com/t-tomalak/logrus-easy-formatter"
	"os"
	"strings"
	"time"
)

var (
	currentPort int
	isRunning bool
	exitCode bool
	runningList map[int] dhtNode = make(map[int] dhtNode)
	backupList map[int] string = make(map[int] string)
	currentIndex int
)

func helpInfo() {
	_, _ = cyan.Println("\nWelcome to chord protocol DHT guide.")
	_, _ = cyan.Println("Run: launch your node.")
	_, _ = cyan.Println("Join: join your node into a network.")
	_, _ = cyan.Println("Create: create a network based on your node.")
	_, _ = cyan.Println("Put: put a {key, value} pair on the network.")
	_, _ = cyan.Println("Get: get the value of key in the network.")
	_, _ = cyan.Println("Delete: remove a key from the network.")
	_, _ = cyan.Println("Backup: save your data to disk.")
	_, _ = cyan.Println("Recover: choose a backup to put on the network.")
	_, _ = cyan.Println("SetPort: change your port number.")
	_, _ = cyan.Println("Help: display help information.")
	_, _ = cyan.Println("Info: display current network information.")
	_, _ = cyan.Println("Quit: quit your node.")
	_, _ = cyan.Println("Exit: quit this application.\n")
}

func main() {
	log.SetFormatter(&easy_formatter.Formatter{
		TimestampFormat: "2006-01-02 15:04:05.000",
		LogFormat:       "[%lvl%]: %time% - %msg%\n",
	})
	log.SetLevel(log.ErrorLevel)
	file, err := os.OpenFile("log/log_"+strings.ReplaceAll(time.Now().Format(time.Stamp)," ","-")+".log", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	log.SetOutput(file)
	_, _ = cyan.Println("Welcome to chord protocol DHT!\n")
	_, _ = cyan.Printf("Your current IP address is %s.\n", GetLocalAddress())
	currentPort = -1
	for currentPort == -1 {
		_, _ = yellow.Println("Please input your port number:")
		if _, err := fmt.Scanf("%d", &currentPort) ; err != nil {
			currentPort = -1
			_, _ = red.Println("Please input a number.")
		}
	}
	_, _ = green.Printf("Successfully set current port number to %d.\n", currentPort)
	helpInfo()
	for !exitCode {
		_, _ = yellow.Println("Please input your operation code.")
		var operationCode string
		_, _ = fmt.Scanf("%s", &operationCode)
		switch operationCode {
		case "Run":
			if isRunning {
				red.Println("Error: this node is already running.")
			} else {
				runningList[currentPort], isRunning = NewNode(currentPort), true
				runningList[currentPort].Run()
				green.Println("Successfully run node.")
			}
		case "Join":
			if !isRunning {
				red.Println("Error: this node isn't running.")
			} else {
				var joinAddress string
				yellow.Println("Please input the address of the join node to join.")
				yellow.Println("For example, 192.168.0.104:9353.")
				_, _ = fmt.Scanf("%s", &joinAddress)
				if !runningList[currentPort].Join(joinAddress) {
					_, _ = red.Println("Error: failed to join.")
				} else {
					_, _ = green.Println("Successfully join your node into the network.")
				}
			}
		case "Create":
			if !isRunning {
				red.Println("Error: this node isn't running.")
			} else {
				runningList[currentPort].Create()
				_, _ = green.Println("Successfully creat a network from your node.")
			}
		case "Put":
			if !isRunning {
				red.Println("Error: this node isn't running.")
			} else {
				var key, value string
				_, _ = yellow.Println("Please input the key to put:")
				fmt.Scanf("%s", &key)
				_, _ = yellow.Println("Please input the value to put:")
				fmt.Scanf("%s", &value)
				if !runningList[currentPort].Put(key, value) {
					_, _ = red.Println("Error: failed to put")
				} else {
					_, _ = green.Printf("Successfully put {key: %s, value: %s} into the network.\n", key, value)
				}
			}
		case "Get":
			if !isRunning {
				red.Println("Error: this node isn't running.")
			} else {
				var key string
				_, _ = yellow.Println("Please input the key to get:")
				fmt.Scanf("%s", &key)
				if ok, value := runningList[currentPort].Get(key) ; ok {
					_, _ = green.Printf("The value of key %s is: %s.\n", key, value)
				} else {
					_, _ = red.Printf("Error: failed to get the value of key %s.\n", key)
				}
			}
		case "Delete":
			if !isRunning {
				red.Println("Error: this node isn't running.")
			} else {
				var key string
				_, _ = yellow.Println("Please input the key to delete:")
				fmt.Scanf("%s", &key)
				if ok := runningList[currentPort].Delete(key) ; ok {
					_, _ = green.Printf("Successfully delete key %s.\n", key)
				} else {
					_, _ = red.Printf("Error: failed to delete key %s.\n", key)
				}
			}
		case "Backup":
			if !isRunning {
				red.Println("Error: this node isn't running.")
			} else {
				if _, err := os.Stat("backup"); os.IsNotExist(err) {
					os.Mkdir("backup", os.ModePerm)
				}
				filename := strings.ReplaceAll(time.Now().Format(time.Stamp), " ", "-")
				file, err := os.OpenFile("backup/"+ filename +".dat", os.O_WRONLY|os.O_CREATE, 0644)
				if err != nil {
					_, _ = red.Println("Error: failed to backup.")
				} else {
					runningList[currentPort].Dump(file)
					file.Close()
					currentIndex ++
					backupList[currentIndex] = filename
					_, _ = green.Println("Successfully save your data to disk.")
				}
			}
		case "Recover":
			if !isRunning {
				red.Println("Error: this node isn't running.")
			} else {
				_, _ = cyan.Println("Local backup list:")
				for key, value := range backupList {
					_, _ = cyan.Printf("%d: %s\n", key, value)
				}
				_, _ = yellow.Println("Please input the number of the backup to recover.")
				var id int
				_, err = fmt.Scanf("%d", &id)
				if err != nil {
					_, _ = red.Println("Error: not a valid number.")
				} else if filename, ok := backupList[id] ; !ok {
					_, _ = red.Println("Error: not a valid number.")
				} else {
					file, err := os.Open("backup/"+ filename +".dat")
					if err != nil {
						_, _ = red.Println("Error: failed to open backup file.")
					} else {
						for {
							var key, value string
							_, err = fmt.Fscanf(file, "%s %s", &key, &value)
							if err != nil {
								break
							}
							runningList[currentPort].Put(key, value)
						}
						_ = file.Close()
						_ = os.Remove("backup/" + filename + ".dat")
						delete(backupList, id)
						_, _ = green.Printf("Successfully recover %s to the network.\n", "backup/"+ filename +".dat")
					}
				}
			}
		case "SetPort":
			_, _ = yellow.Println("Please input your port number:")
			var newPort int = -1
			if _, err := fmt.Scanf("%d", &newPort) ; err != nil {
				_, _ = red.Println("Error: not a valid port number.")
			} else {
				currentPort = newPort
				_, isRunning = runningList[currentPort]
				_, _ = green.Printf("Successfully set current port number to %d.\n", currentPort)
			}
		case "Help": helpInfo()
		case "Info":
			_, _ = cyan.Printf("Your current IP address is %s.\n", GetLocalAddress())
			_, _ = cyan.Printf("Current port %d.\n", currentPort)
			_, _ = cyan.Println("All online port numbers on your computer:")
			for key, _ := range runningList {
				_, _ = cyan.Printf("%d ", key)
			}
			_, _ = cyan.Println()
		case "Quit":
			runningList[currentPort].Quit()
			isRunning = false
			delete(runningList, currentPort)
			_, _ = green.Println("Successfully quit.")
		case "Exit":
			_, _ = cyan.Println("Bye!")
			exitCode = true
			break
		default: _, _ = red.Println("Please input a valid operation code.")
		}
	}
}