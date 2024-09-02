package main

import (
	"os"
)

const MASTER_ARG = "master"
const WORKER_SERVER_ARG = "worker"

func main() {
	commandLineArgs := os.Args
	serverType := commandLineArgs[1]

	if serverType == MASTER_ARG {
		inputFilepath := "../test-files/big.txt"
		if len(commandLineArgs) == 3 {
			inputFilepath = commandLineArgs[2]
		}
		createMasterClient(inputFilepath)
	} else if serverType == WORKER_SERVER_ARG {
		createWorkerServer(commandLineArgs[2])
	} else {
		createAccumulatorServer()
	}
}
