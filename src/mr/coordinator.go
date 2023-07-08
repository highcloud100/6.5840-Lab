package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	Files         []string
	Checker       map[string]int // 0 1 2 (before, processing, completed)
	RChecker      []int          // 0 1 2
	MapFlag       bool
	NReduce       int
	Mut           sync.Mutex
	CompleteFlag  bool
	CompleteRFlag bool
	RtaskCnt      int // allocate r num
	DoneFlag      bool
}

// Your code here -- RPC handlers for the worker to call.
// concurrent
// func (c *Coordinator) ReduceComplete(args *Rargs, reply *Rreply) error {

// }

func (c *Coordinator) Complete(args *Cargs, reply *Creply) error { // map 작업이 완료되었는지 신호보냄
	c.Mut.Lock()
	defer c.Mut.Unlock()

	//fmt.Printf("complete : %v\n", args.TaskName)

	if args.TaskType == "map" {
		c.Checker[args.TaskName] = 2

		for _, val := range c.Checker { // 모든 작업이 완료되었는가?
			if val != 2 {
				return nil
			}
		}
		//fmt.Println("all completed")
		c.CompleteFlag = true
		return nil
	} else if args.TaskType == "reduce" {
		c.RChecker[args.TaskNum] = 2

		for _, val := range c.RChecker {
			if val != 2 {
				return nil
			}
		}
		c.CompleteRFlag = true
		return nil
	}
	return nil
}

func (c *Coordinator) timeCheck(taskType string, taskNum int, fileName string) {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if taskType == "map" {
				c.Mut.Lock()
				if c.Checker[fileName] != 2 {
					c.Checker[fileName] = 0
				}
				c.Mut.Unlock()
			} else if taskType == "reduce" {
				c.Mut.Lock()

				if c.RChecker[taskNum] != 2 {
					c.RChecker[taskNum] = 0
				}

				c.Mut.Unlock()
			}
		}
	}
}

func (c *Coordinator) JobRequest(args *Args, reply *Reply) error {
	c.Mut.Lock()
	defer c.Mut.Unlock()

	reply.Nreduce = c.NReduce
	if c.MapFlag == false { // 모든 map 작업이 할당되었는가?

		for idx, i := range c.Files {

			if c.Checker[i] == 0 {
				reply.Filename = i
				reply.TaskNum = idx
				reply.JobType = 0
				c.Checker[i] = 1
				go c.timeCheck("map", 0, i)

				//fmt.Printf("res : %v\n", i)

				if idx == len(c.Checker)-1 {
					c.MapFlag = true
				}
				return nil
			}
		}
	} else {
		reply.JobType = 1
		//fmt.Println("reduce")
		if c.CompleteFlag == false { // wait mapping
			reply.TaskNum = -1
			//fmt.Print("wait map\n")
			return nil

		} else if c.CompleteRFlag == false {
			//fmt.Println("assign reduce")
			for idx, i := range c.RChecker {
				if i == 0 {
					reply.TaskNum = idx
					reply.Nmap = len(c.Files)
					c.RChecker[idx] = 1
					go c.timeCheck("reduce", idx, "")
					//fmt.Printf("res : %v\n", idx)
					return nil
				}
			}

		} else {
			reply.TaskNum = -2
			c.DoneFlag = true
		}
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := c.DoneFlag

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.Files = files
	c.Checker = make(map[string]int)
	c.RChecker = make([]int, nReduce)
	c.MapFlag = false
	c.NReduce = nReduce
	c.RtaskCnt = 0
	c.CompleteFlag = false
	c.CompleteRFlag = false

	for _, i := range files {
		c.Checker[i] = 0
	}
	for i := 0; i < nReduce; i++ {
		c.RChecker[i] = 0
	}
	// Your code here.

	c.server()
	return &c
}
