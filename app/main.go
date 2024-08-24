package main

import (
	"bufio"
	"fmt"
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

	masterAddr, err := net.ResolveTCPAddr("tcp4", LOCAL_HOST_IP_ADDR+":"+MASTER_SERVER_PORT_STR)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	masterConn, err := net.DialTCP("tcp", nil, masterAddr)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	registrationMessage := "register " + LOCAL_HOST_IP_ADDR + " " + workerPort + "\n"
	_, err = masterConn.Write([]byte(registrationMessage))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	data, err := bufio.NewReader(masterConn).ReadString('\n')
	if err != nil {
		fmt.Println(err)
		return
	}
	if string(data) != "registered" {
		fmt.Println("failed to register")
		return
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
		}

		go handleWorkerConnection(conn)
	}
}

func handleWorkerConnection(conn net.Conn) {

}

type IpAddr struct {
	ip   string
	port string
}

func (ipAddr IpAddr) String() string {
	return ipAddr.ip + ":" + ipAddr.port
}

type MasterClientData struct {
	workToWorkerMap map[int](*IpAddr)
	numWorkers      int
	doneRegistering bool
	mutex           sync.Mutex
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
		workToWorkerMap: make(map[int]*IpAddr),
		numWorkers:      0,
		doneRegistering: false,
		mutex:           sync.Mutex{},
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
		}

		go handleMasterConnection(conn, &workerData)
	}
}

func handleMasterConnection(conn net.Conn, workerData *MasterClientData) {
	defer conn.Close()

	for {
		data, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}

		stringData := string(data)
		fmt.Print("received: " + stringData)

		splittedStringData := strings.Split(stringData, " ")

		if !workerData.doneRegistering && splittedStringData[0] == REGISTER_WORKER {
			ipAddr := IpAddr{
				ip:   splittedStringData[1],
				port: splittedStringData[2],
			}

			workerData.mutex.Lock()
			workerData.workToWorkerMap[workerData.numWorkers] = &ipAddr
			workerData.numWorkers += 1
			workerData.mutex.Unlock()

			conn.Write([]byte("registered"))

			// workedAddr, err := net.ResolveTCPAddr("tcp", ipAddr.String())
			// if err != nil {
			// 	fmt.Println(err)
			// 	os.Exit(1)
			// }
			// workedConn, err := net.DialTCP("tcp", nil, workedAddr)
			// if err != nil {
			// 	fmt.Println(err)
			// 	os.Exit(1)
			// }
			// _, err = workedConn.Write([]byte("registered\n"))
			// if err != nil {
			// 	fmt.Println(err)
			// 	os.Exit(1)
			// }

			fmt.Printf("registered worker from %s:%s\n", ipAddr.ip, ipAddr.port)
		}
	}
}
