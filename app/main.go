package main

import (
	"os"
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

func main() {
	comandLineArgs := os.Args
	serverType := comandLineArgs[1]

	if serverType == MASTER_ARG {
		createMasterClient()
	} else {
		createWorkerServer(comandLineArgs[2])
	}
}

/*
	Have an accumlator that combines the results of the workers which then sends back to master
*/
