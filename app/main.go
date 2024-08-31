package main

import (
	"os"
)

const MASTER_ARG = "master"
const WORKER_SERVER_ARG = "worker"

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
