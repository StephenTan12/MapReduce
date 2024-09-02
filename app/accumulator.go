package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const ACCUMULATOR_SERVER_PORT = "9000"
const OUTPUT_FILE = "output.txt"

type AccumulatorInternalData struct {
	numWorkers         int
	workersDoneSending []bool
	workerResults      [][]string
	workerResultsMutex sync.Mutex
	masterConn         net.Conn
	serverIp           string
}

func createAccumulatorServer() {
	accumulatorServerIpAddr := LOCAL_HOST_IP_ADDR + ":" + ACCUMULATOR_SERVER_PORT
	masterIpAddr := LOCAL_HOST_IP_ADDR + ":" + MASTER_SERVER_PORT_STR

	registrationMessage := "accumulator " + LOCAL_HOST_IP_ADDR + " " + ACCUMULATOR_SERVER_PORT + "\n"
	response, masterConn := writeToServer(masterIpAddr, registrationMessage)
	defer masterConn.Close()

	formattedResponse := strings.Split(strings.TrimSpace(response), " ")

	if formattedResponse[0] != "numWorkers" {
		fmt.Println("failed to receive")
		return
	}

	numWorkers, err := strconv.Atoi(formattedResponse[1])
	if err != nil {
		fmt.Println("error in numWorkers")
		return
	}

	internalData := AccumulatorInternalData{
		numWorkers:         numWorkers,
		workersDoneSending: make([]bool, numWorkers),
		workerResults:      make([][]string, numWorkers),
		workerResultsMutex: sync.Mutex{},
		masterConn:         masterConn,
		serverIp:           accumulatorServerIpAddr,
	}

	fmt.Printf("accumulator %s is ready to start reading responses\n", accumulatorServerIpAddr)
	listenForWorkerResponse(&internalData)

	fmt.Printf("accumulator %s is aggregating worker results\n", accumulatorServerIpAddr)
	aggregateWorkerResults(&internalData)

	masterConn.Write([]byte("done\n"))
}

func listenForWorkerResponse(internalData *AccumulatorInternalData) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", internalData.serverIp)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for range internalData.numWorkers {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
		}

		go handleWorkerConnection(conn, internalData)
	}

	for {
		time.Sleep(10 * time.Millisecond)
		didFinish := true
		for _, doneSending := range internalData.workersDoneSending {
			if !doneSending {
				didFinish = false
				break
			}
		}

		if didFinish {
			break
		}
	}

	fmt.Println("Accumulator has finished accepting responses")
}

func handleWorkerConnection(conn net.Conn, internalData *AccumulatorInternalData) {
	currWorkerNum := 0
	words := make([]string, 0)

	for {
		data, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}

		formattedData := strings.Split(strings.TrimSpace(data), " ")

		if formattedData[0] == "done" {
			fmt.Printf("Accumulator has read %d words from worker %d\n", len(words), currWorkerNum)
			internalData.workersDoneSending[currWorkerNum-1] = true
			break
		}

		if formattedData[0] != "add" {
			fmt.Println(err)
			return
		}

		workerNum, _ := strconv.Atoi(formattedData[1])
		currWorkerNum = workerNum
		words = append(words, formattedData[2])
	}

	internalData.workerResultsMutex.Lock()
	internalData.workerResults[currWorkerNum-1] = append(internalData.workerResults[currWorkerNum-1], words...)
	internalData.workerResultsMutex.Unlock()
}

func aggregateWorkerResults(internalData *AccumulatorInternalData) {
	output, err := os.Create(OUTPUT_FILE)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := output.Close(); err != nil {
			panic(err)
		}
	}()

	for _, words := range internalData.workerResults {
		for _, word := range words {
			if _, err := output.Write([]byte(word + "\n")); err != nil {
				panic(err)
			}
		}
	}
	fmt.Printf("Accumulator has aggregated worker results and output to file %s\n", output.Name())
}
