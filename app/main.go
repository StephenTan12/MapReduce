package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
)

const MASTER_ARG = "master"
const WORKER_SERVER_ARG = "worker"

const LOCAL_HOST_IP_ADDR = "127.0.0.1"
const MASTER_SERVER_PORT = 8000
const MASTER_SERVER_PORT_STR = "8000"

const REGISTER_WORKER = "register"
const SEND_DATA_WORKER = "data"

type IpAddr struct {
	ip   string
	port string
}

func (ipAddr IpAddr) String() string {
	return ipAddr.ip + ":" + ipAddr.port
}

type MasterClientData struct {
	workToWorkerMap   map[int](*IpAddr)
	numWorkers        int
	maxWorkers        int
	mutex             sync.Mutex
	regStageCompleted bool
}

type WorkerInternalData struct {
	words              []string
	mutex              sync.Mutex
	doneReceivingWords bool
}

/*
 Maintain one connetion instead of closing and opening
 have a map

 keywords:
	register ip port\n
	add word\n
	done\n

*/

func main() {
	comandLineArgs := os.Args
	serverType := comandLineArgs[1]

	if serverType == MASTER_ARG {
		createMasterClient()
	} else {
		createWorkerServer(comandLineArgs[2])
	}
}

func createWorkerServer(workerPort string) {
	workerServerIpAddr := LOCAL_HOST_IP_ADDR + ":" + workerPort

	workerAddr, err := net.ResolveTCPAddr("tcp", workerServerIpAddr)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	listener, err := net.ListenTCP("tcp", workerAddr)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	registrationMessage := "register " + LOCAL_HOST_IP_ADDR + " " + workerPort + "\n"
	masterIpAddr := LOCAL_HOST_IP_ADDR + ":" + MASTER_SERVER_PORT_STR
	response := writeToServer(masterIpAddr, registrationMessage)

	if strings.TrimSpace(response) != "registered" {
		fmt.Println("failed to register worker")
		return
	}

	internalData := WorkerInternalData{
		words:              make([]string, 0),
		mutex:              sync.Mutex{},
		doneReceivingWords: false,
	}

	fmt.Printf("worker %s is ready to start accepting\n", workerServerIpAddr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
		}

		handleWorkerConnection(conn, &internalData)

		if internalData.doneReceivingWords {
			break
		}
	}

	fmt.Printf("worker %s has received %d words\n", workerServerIpAddr, len(internalData.words))
}

func writeToServer(ipAddr string, messageContent string) string {
	addr, err := net.ResolveTCPAddr("tcp4", ipAddr)
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

	_, err = conn.Write([]byte(messageContent))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		fmt.Println(err)
		return ""
	}

	return response
}

func handleWorkerConnection(conn net.Conn, internalData *WorkerInternalData) {
	defer conn.Close()
	for {
		fmt.Println("trying to read")
		data, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Println("read: " + data)

		word := strings.TrimSpace(data)

		if word == "" {
			internalData.doneReceivingWords = true
			conn.Write([]byte("done\n"))
			return
		}

		internalData.words = append(internalData.words, word)
	}
}

func createMasterClient() {
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
		workToWorkerMap:   make(map[int]*IpAddr),
		numWorkers:        0,
		maxWorkers:        3,
		mutex:             sync.Mutex{},
		regStageCompleted: false,
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
		}

		handleWorkerRegistration(conn, &workerData)

		if workerData.regStageCompleted {
			break
		}
	}

	sendDataToWorkers(&workerData)
}

func handleWorkerRegistration(conn net.Conn, workerData *MasterClientData) {
	defer conn.Close()

	data, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		fmt.Println(err)
		return
	}

	stringData := strings.TrimSpace(data)
	fmt.Print("received: " + stringData)

	splittedStringData := strings.Split(stringData, " ")

	if workerData.maxWorkers > workerData.numWorkers && splittedStringData[0] == REGISTER_WORKER {
		ipAddr := IpAddr{
			ip:   splittedStringData[1],
			port: splittedStringData[2],
		}

		workerData.mutex.Lock()
		workerData.workToWorkerMap[workerData.numWorkers] = &ipAddr
		workerData.numWorkers += 1
		workerData.mutex.Unlock()

		conn.Write([]byte("registered\n"))

		fmt.Printf("registered worker from %s:%s\n", ipAddr.ip, ipAddr.port)

		if workerData.numWorkers == workerData.maxWorkers {
			workerData.regStageCompleted = true
		}
	}
}

func sendDataToWorkers(workerData *MasterClientData) {
	f, err := os.Open("../test-files/1000-common-words.txt")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	scanner := bufio.NewScanner(f)
	wordPerServer := make([][]string, workerData.numWorkers)
	currentWordNum := 0

	for scanner.Scan() {
		word := scanner.Text()
		workerNum := currentWordNum % workerData.numWorkers
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
		workerIpAddr := workerData.workToWorkerMap[workerNum]
		go sendDataToWorker(workerIpAddr, workerWords, &wg)
	}

	wg.Wait()
}

func sendDataToWorker(ipAddr *IpAddr, wordsToSend []string, wg *sync.WaitGroup) {
	defer wg.Done()
	addr, err := net.ResolveTCPAddr("tcp4", ipAddr.String())
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

	for _, word := range wordsToSend {
		messageContent := word + "\n"

		_, err = conn.Write([]byte(messageContent))
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	messageContent := "\n"
	_, err = conn.Write([]byte(messageContent))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	data, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(strings.TrimSpace((data)))
}
