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

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const waitTime = time.Second * 2

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		reply := GetJob()

		switch reply.Jtype {
		case QUIT:
			fmt.Println("Quiting...")
			return
		case WAIT:
			fmt.Println("Waiting...")
			time.Sleep(waitTime)
		case MAP:
			fmt.Printf("Get a map job #%v. Executing...\n", reply.Jid)
			if execMap(reply, mapf) {
				finishJob(reply)
			} else {
				fmt.Println("Exec map failed.")
				return
			}
		case REDUCE:
			fmt.Printf("Get a reduce job #%v. Executing...\n", reply.Jid)
			if execReduce(reply, reducef) {
				finishJob(reply)
			} else {
				fmt.Println("Exec reduce failed.")
				return
			}
		}
		// time.Sleep(waitTime)
	}

}

// get job from master
func GetJob() JobReply {
	args := JobArgs{}
	reply := JobReply{}
	// reply.Jfiles = []string{}
	if !call("Master.GetJob", &args, &reply) {
		reply.Jtype = QUIT
	}
	return reply
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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

func execMap(reply JobReply, mapf func(string, string) []KeyValue) bool {
	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	groups := make([][]KeyValue, reply.Jnum)
	for _, filename := range reply.Jfiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return false
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
			return false
		}
		file.Close()
		kva := mapf(filename, string(content))
		for _, kv := range kva {
			i := ihash(kv.Key) % reply.Jnum
			groups[i] = append(groups[i], kv)
		}
	}

	for i, group := range groups {
		file, _ := ioutil.TempFile("", "map-tmp-")

		tmpPath := file.Name()
		targetPath := "./mr-" + strconv.Itoa(reply.Jid) + "-" + strconv.Itoa(i) + ".tmp"
		enc := json.NewEncoder(file)
		for _, kv := range group {
			enc.Encode(&kv)
		}
		file.Close()
		os.Rename(tmpPath, targetPath)
	}
	return true
}

func finishJob(reply JobReply) {
	emptyReply := EmptyReply{}
	call("Master.FinishJob", &reply, &emptyReply)
}

func execReduce(reply JobReply, reducef func(string, []string) string) bool {
	intermediate := []KeyValue{}
	for _, fileName := range reply.Jfiles {
		file, err := os.Open(fileName)
		if err != nil {
			return false
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(reply.Jid) + ".tmp"
	ofile, err := os.Create(oname)
	if err != nil {
		return false
	}
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	return true
}
