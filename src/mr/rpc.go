package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Args struct { // for job request
	Who int
}

type Reply struct {
	Filename string
	TaskNum  int // worker 번호
	Nreduce  int // reduce 최대 크기
	Nmap     int
	JobType  int // map ? reduce?
}

type Cargs struct {
	TaskName string
	TaskType string
	TaskNum  int
}

type Creply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
