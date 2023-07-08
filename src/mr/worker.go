package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func mapping(mapf func(string, string) []KeyValue, reply *Reply) {

	// read data
	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	file.Close()
	kva := mapf(reply.Filename, string(content))

	// partitioning
	dkva := make([][]KeyValue, reply.Nreduce)

	for _, temp := range kva {
		rTaskNum := ihash(temp.Key) % reply.Nreduce
		dkva[rTaskNum] = append(dkva[rTaskNum], temp)
	}

	// write
	for idx, temp := range dkva {
		if len(temp) != 0 {
			oname := "mr-" + strconv.Itoa(reply.TaskNum) + "-" + strconv.Itoa(idx)
			ofile, _ := os.Create(oname)

			enc := json.NewEncoder(ofile)
			for _, kv := range temp {
				_ = enc.Encode(&kv)

			}
			ofile.Close()
		}
	}
	cargs := Cargs{reply.Filename, "map", 0}
	creply := Creply{}
	ok := call("Coordinator.Complete", &cargs, &creply)
	if !ok {
		//fmt.Printf("%v", ok)
	}
}

func reducing(reducef func(string, []string) string, reply *Reply) {
	var kva []KeyValue

	//fmt.Printf("reduce %v\n", reply.TaskNum)

	for i := 0; i < reply.Nmap; i++ {
		name := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskNum)
		file, _ := os.Open(name)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))

	oname := "mr-out-" + strconv.Itoa(reply.TaskNum)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in kva[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
	cargs := Cargs{"", "reduce", reply.TaskNum}
	creply := Creply{}
	ok := call("Coordinator.Complete", &cargs, &creply)
	if !ok {
		//fmt.Printf("%v", ok)
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	//CallExample()

	for {
		args := Args{1}
		reply := Reply{}

		ok := call("Coordinator.JobRequest", &args, &reply)
		if !ok {
			//fmt.Println("return")
			//fmt.Println(ok)
			return
		}

		if reply.JobType == 0 {
			mapping(mapf, &reply)
		} else {
			if reply.TaskNum == -1 {
				time.Sleep(1 * time.Second)
			} else if reply.TaskNum == -2 {
				return
			} else {
				//fmt.Println("reducing")
				reducing(reducef, &reply)
			}
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
