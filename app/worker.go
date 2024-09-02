package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type WorkerInternalData struct {
	words              []string
	mutex              sync.Mutex
	doneReceivingWords bool
	masterConn         net.Conn
	workerNum          int
}

func createWorkerServer(workerPort string) {
	workerServerIpAddr := LOCAL_HOST_IP_ADDR + ":" + workerPort

	registrationMessage := "worker " + LOCAL_HOST_IP_ADDR + " " + workerPort + "\n"
	masterIpAddr := LOCAL_HOST_IP_ADDR + ":" + MASTER_SERVER_PORT_STR
	response, masterConn := writeToServer(masterIpAddr, registrationMessage)

	formattedResponse := strings.Split(strings.TrimSpace(response), " ")

	if formattedResponse[0] != "registered" {
		fmt.Println("failed to register worker")
		return
	}

	workerNum, err := strconv.Atoi(formattedResponse[1])
	if err != nil {
		fmt.Println("failed to read worker number")
		return
	}

	internalData := WorkerInternalData{
		words:              make([]string, 0),
		mutex:              sync.Mutex{},
		doneReceivingWords: false,
		masterConn:         masterConn,
		workerNum:          workerNum,
	}

	fmt.Printf("worker %s is ready to start accepting\n", workerServerIpAddr)

	for {
		data, err := bufio.NewReader(internalData.masterConn).ReadString('\n')
		if err != nil {
			masterConn.Close()
			fmt.Println(err)
			return
		}

		formattedData := strings.Split(strings.TrimSpace(data), " ")

		if formattedData[0] == "add" {
			internalData.words = append(internalData.words, formattedData[1])

		} else if formattedData[0] == "@" {
			internalData.doneReceivingWords = true
			break
		}
	}

	fmt.Printf("worker %d has received %d words\n", internalData.workerNum, len(internalData.words))

	fmt.Printf("worker %d is sorting words\n", internalData.workerNum)
	sort.Strings(internalData.words)

	fmt.Printf("worker %d is sending words to accumulator\n", internalData.workerNum)
	sendToAccumulator(&internalData)
	fmt.Printf("worker %d has sent %d words to accumulator\n", internalData.workerNum, len(internalData.words))
	masterConn.Close()
}

func writeToServer(ipAddr string, messageContent string) (string, net.Conn) {
	addr, err := net.ResolveTCPAddr("tcp4", ipAddr)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	_, err = conn.Write([]byte(messageContent))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		fmt.Println(err)
		return "", nil
	}

	return response, conn
}

func sendToAccumulator(internalData *WorkerInternalData) {
	accumulatorServerIpAddr := LOCAL_HOST_IP_ADDR + ":" + ACCUMULATOR_SERVER_PORT

	addr, err := net.ResolveTCPAddr("tcp4", accumulatorServerIpAddr)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	conn, err := net.DialTCP("tcp", nil, addr)
	defer conn.Close()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for _, word := range internalData.words {
		messageContent := "add " + strconv.Itoa(internalData.workerNum) + " " + word + "\n"
		time.Sleep(3 * time.Millisecond)
		_, err = conn.Write([]byte(messageContent))
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	time.Sleep(3 * time.Millisecond)
	_, err = conn.Write([]byte("done\n"))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
