package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const LOCAL_HOST_IP_ADDR = "127.0.0.1"
const MASTER_SERVER_PORT = 8000
const MASTER_SERVER_PORT_STR = "8000"
const REGISTER_WORKER = "worker"
const REGISTER_ACCUMULATOR = "accumulator"

type IpAddr struct {
	ip   string
	port string
}

func (ipAddr IpAddr) String() string {
	return ipAddr.ip + ":" + ipAddr.port
}

type MasterClientData struct {
	workToWorkerMap      map[int](net.Conn)
	accumlatorServerConn net.Conn
	numWorkers           int
	maxWorkers           int
	mutex                sync.Mutex
	regStageCompleted    bool
	createdAccumulator   bool
	inputFilepath        string
}

func createMasterClient(inputFilepath string) {
	fmt.Println("created master")

	masterServerIPAddr := LOCAL_HOST_IP_ADDR + ":" + MASTER_SERVER_PORT_STR

	tcpAddr, err := net.ResolveTCPAddr("tcp", masterServerIPAddr)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	workerData := MasterClientData{
		workToWorkerMap:      make(map[int]net.Conn),
		accumlatorServerConn: nil,
		numWorkers:           0,
		maxWorkers:           3,
		mutex:                sync.Mutex{},
		regStageCompleted:    false,
		inputFilepath:        inputFilepath,
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
		}

		handleServerRegistration(conn, &workerData)

		if workerData.regStageCompleted && workerData.createdAccumulator {
			break
		}
	}

	message := "numWorkers " + strconv.Itoa(workerData.numWorkers) + "\n"
	_, err = workerData.accumlatorServerConn.Write([]byte(message))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	sendWordDataToWorkers(&workerData)

	data, err := bufio.NewReader(workerData.accumlatorServerConn).ReadString('\n')
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	stringData := strings.TrimSpace(data)

	if stringData != "done" {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println("the sorting of file has completed")

	workerData.accumlatorServerConn.Close()

	for _, conn := range workerData.workToWorkerMap {
		conn.Close()
	}
}

func handleServerRegistration(conn net.Conn, workerData *MasterClientData) {
	data, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		fmt.Println(err)
		return
	}

	stringData := strings.TrimSpace(data)
	fmt.Println("received: " + stringData)

	splittedStringData := strings.Split(stringData, " ")

	if workerData.maxWorkers > workerData.numWorkers && splittedStringData[0] == REGISTER_WORKER {
		ipAddr := IpAddr{
			ip:   splittedStringData[1],
			port: splittedStringData[2],
		}

		workerData.mutex.Lock()
		workerData.workToWorkerMap[workerData.numWorkers] = conn
		workerData.numWorkers += 1
		workerData.mutex.Unlock()

		message := "registered " + strconv.Itoa(workerData.numWorkers) + "\n"
		conn.Write([]byte(message))

		fmt.Printf("registered worker from %s:%s\n", ipAddr.ip, ipAddr.port)

		if workerData.numWorkers == workerData.maxWorkers {
			workerData.regStageCompleted = true
		}
	} else if splittedStringData[0] == REGISTER_ACCUMULATOR {
		workerData.accumlatorServerConn = conn

		ipAddr := IpAddr{
			ip:   splittedStringData[1],
			port: splittedStringData[2],
		}

		fmt.Printf("registered accumulator from %s:%s\n", ipAddr.ip, ipAddr.port)

		workerData.createdAccumulator = true
	}
}

func sendWordDataToWorkers(workerData *MasterClientData) {
	f, err := os.Open(workerData.inputFilepath)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	scanner := bufio.NewScanner(f)
	wordPerServer := make([][]string, workerData.numWorkers)
	currentWordNum := 0

	for scanner.Scan() {
		word := scanner.Text()

		hashWordKey := hashWord(word)
		workerNum := getWorkerNum(hashWordKey, workerData.numWorkers)

		wordPerServer[workerNum] = append(wordPerServer[workerNum], word)
		currentWordNum += 1
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Num of words for server 0: ", len(wordPerServer[0]))
	fmt.Println("Num of words for server 1: ", len(wordPerServer[1]))
	fmt.Println("Num of words for server 2: ", len(wordPerServer[2]))

	var wg sync.WaitGroup

	for workerNum, workerWords := range wordPerServer {
		wg.Add(1)
		workerConn := workerData.workToWorkerMap[workerNum]
		go sendDataToWorker(workerConn, workerWords, &wg)
	}

	wg.Wait()
}

func hashWord(word string) int {
	return int(word[0]) - int('a')
}

func getWorkerNum(hashWordKey int, numWorkers int) int {
	intervals := 26 / numWorkers

	for numWorker := range numWorkers {
		if hashWordKey < intervals*(numWorker+1) {
			return numWorker
		}
	}
	return numWorkers - 1
}

func sendDataToWorker(workerConn net.Conn, wordsToSend []string, wg *sync.WaitGroup) {
	defer wg.Done()

	for _, word := range wordsToSend {
		messageContent := "add " + word + "\n"
		time.Sleep(3 * time.Millisecond)
		_, err := workerConn.Write([]byte(messageContent))
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
	time.Sleep(3 * time.Millisecond)
	messageContent := "@ \n"
	_, err := workerConn.Write([]byte(messageContent))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
