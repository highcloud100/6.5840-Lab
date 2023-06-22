package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"


type Coordinator struct {
	// Your definitions here.
	Files[] string
	Checker map[string]bool
	MapFlag bool
	NReduce int
}

// Your code here -- RPC handlers for the worker to call.
func (c* Coordinator) JobRequest(args*Args, reply*Reply) error {
	reply.Nreduce = c.NReduce
	if c.MapFlag==false{
		for idx,i := range c.Files{
			if c.Checker[i] == false {
				reply.Filename = i
				reply.TaskNum = idx
				reply.JobType = 0
				c.Checker[i] = true
				

				fmt.Println("res : %v",  i)

				if idx == len(c.Checker)-1 {
					c.MapFlag = true
				}

				return nil
			}
		}
	}
	
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.Files = files
	c.Checker = make(map[string]bool)
	c.MapFlag = false
	c.NReduce = nReduce

	for _, i := range files{
		c.Checker[i] = false
	}
	// Your code here.


	c.server()
	return &c
}
