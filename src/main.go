package main

import (
	"flag"
	log "github.com/sirupsen/logrus"
	easy_formatter "github.com/t-tomalak/logrus-easy-formatter"
	"math/rand"
	"os"
	"strings"
	"time"
)

var (
	help     bool
	testName string
)

func init() {
	flag.BoolVar(&help, "help", false, "help")
	flag.StringVar(&testName, "test", "", "which test(s) do you want to run: basic/advance/all")

	flag.Usage = usage
	//flag.Parse()
	testName = "basic"

	if help || (testName != "basic" && testName != "advance" && testName != "all") {
		flag.Usage()
		os.Exit(0)
	}

	rand.Seed(time.Now().UnixNano())
}

func main_() {
	log.SetFormatter(&easy_formatter.Formatter{
		TimestampFormat: "2006-01-02 15:04:05.000",
		LogFormat:       "[%lvl%]: %time% - %msg%\n",
	})
	log.SetLevel(log.InfoLevel)
	file, err := os.OpenFile("log/log_"+strings.ReplaceAll(time.Now().Format(time.Stamp)," ","-")+".log", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	log.SetOutput(file)

	_, _ = yellow.Println("Welcome to DHT-2020 Test Program!\n")

	var basicFailRate float64
	//var advanceFailRate float64

	switch testName {
	case "all":
		fallthrough
	case "basic":
		_, _ = yellow.Println("Basic Test Begins:")
		basicFailedCnt, basicTotalCnt := basicTest()
		basicFailRate = float64(basicFailedCnt) / float64(basicTotalCnt)
		if basicFailRate > basicTestMaxFailRate {
			_, _ = red.Printf("Basic test failed with fail rate %.4f\n", basicFailRate)
		} else {
			_, _ = green.Printf("Basic test passed with fail rate %.4f\n", basicFailRate)
		}

		if testName == "basic" {
			break
		}
		fallthrough
	case "advance":
		_, _ = cyan.Println("Advance Test Begins:")
		_, _ = red.Println("To be added...")
	}
	/*
		switch 1 {
		case 1:
			blue.Println("Start Advanced Tests")
			if advancedTest(); basicTestMaxFailRate > failrate() {
				green.Println("Passed Advanced Tests with", failrate())
			} else {
				red.Println("Failed Advanced Tests")
				// os.Exit(0)
			}

			totalCnt = 0
			totalFail = 0
			blue.Println("Start Force Quit Tests")
			if testForceQuit(2); basicTestMaxFailRate > failrate()/50 {
				green.Println("Passed Force Quit with", failrate())
			} else {
				red.Println("Failed Advanced Tests")
				os.Exit(0)
			}
			finalScore += failrate()
		default:
			red.Print("Select error, ask -h for help")
			os.Exit(0)
		}

		green.Printf("\nNot necessary, but tell finall score: %.2f\n", 1-finalScore)
	*/
	_, _ = cyan.Println("\nFinal print:")
	if basicFailRate > basicTestMaxFailRate {
		_, _ = red.Printf("Basic test failed with fail rate %.4f\n", basicFailRate)
	} else {
		_, _ = green.Printf("Basic test passed with fail rate %.4f\n", basicFailRate)
	}
}

func usage() {
	flag.PrintDefaults()
}
