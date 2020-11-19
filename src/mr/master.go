package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	MAPPING = iota
	REDUCING
	DONE
	MAXJOB    = 2
	REJOBTIME = 10 * time.Second
)

type Master struct {
	// Your definitions here.
	mu             sync.Mutex
	status         int // 0 mapping; 1 reducing; 2 done
	jid            int
	nReduce        int
	needMapFile    map[string]bool
	mappingFile    map[string]bool
	needReduceFile map[int][]string
	reducingFile   map[int][]string
	channels       map[int]chan bool
}

// Your code here -- RPC handlers for the worker to call.

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// worker.go calls GetJob to get a current job from master
//
func (m *Master) GetJob(args *JobArgs, reply *JobReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch m.status {
	case MAPPING:
		if len(m.needMapFile) > 0 {
			var i = 0
			for key := range m.needMapFile {
				i++
				if i > MAXJOB {
					break
				}
				reply.Jfiles = append(reply.Jfiles, key)
				delete(m.needMapFile, key)
				m.mappingFile[key] = true
			}
			reply.Jtype = MAP
			reply.Jid = m.jid
			reply.Jnum = m.nReduce
			m.channels[m.jid] = make(chan bool)
			go m.waitFinish(m.channels[m.jid], reply)
			m.jid++
			// go wait for resopnse
		} else {
			reply.Jtype = WAIT
		}
	case REDUCING:
		if len(m.needReduceFile) > 0 {
			for key, value := range m.needReduceFile {
				reply.Jfiles = value
				reply.Jid = key
				delete(m.needReduceFile, key)
				m.reducingFile[key] = value
				break
			}
			reply.Jnum = m.nReduce
			reply.Jtype = REDUCE
			m.channels[reply.Jid] = make(chan bool)
			go m.waitFinish(m.channels[reply.Jid], reply)
		} else {
			reply.Jtype = WAIT
		}
	case DONE:
		reply.Jtype = QUIT
	}
	return nil
}

func (m *Master) waitFinish(ch chan bool, reply *JobReply) {
	select {
	case <-ch:
		// finished
		m.mu.Lock()
		if reply.Jtype == MAP && m.status == MAPPING {
			fmt.Printf("Finish map job #%v\n", reply.Jid)
			for _, file := range reply.Jfiles {
				delete(m.mappingFile, file)
				delete(m.needMapFile, file)
			}
			for i := 0; i < m.nReduce; i++ {
				fileName := "mr-" + strconv.Itoa(reply.Jid) + "-" + strconv.Itoa(i) + ".tmp"
				m.needReduceFile[i] = append(m.needReduceFile[i], fileName)
			}

			fmt.Printf("Last %v files\n", len(m.needMapFile)+len(m.mappingFile))

			if len(m.needMapFile) == 0 && len(m.mappingFile) == 0 {
				fmt.Printf("Swich status mapping to reducing\n")
				m.status = REDUCING
			}

		} else if reply.Jtype == REDUCE && m.status == REDUCING {
			fmt.Printf("Finish reduce job #%v\n", reply.Jid)
			delete(m.reducingFile, reply.Jid)
			delete(m.needReduceFile, reply.Jid)
			if len(m.needReduceFile) == 0 && len(m.reducingFile) == 0 {
				fmt.Printf("Swich status reducing to done\n")
				m.status = DONE
			}
		} else {
			fmt.Printf("Job #%v finished, but is dropped.\n", reply.Jid)
		}

		delete(m.channels, reply.Jid)
		m.mu.Unlock()
		return
	case <-time.After(REJOBTIME):
		m.mu.Lock()
		if reply.Jtype == MAP {
			fmt.Printf("Map job #%v timeout!\n", reply.Jid)
			for _, file := range reply.Jfiles {
				delete(m.mappingFile, file)
				m.needMapFile[file] = true
			}
			delete(m.channels, reply.Jid)
		} else if reply.Jtype == REDUCE {
			fmt.Printf("Reduce job #%v timeout!\n", reply.Jid)
			delete(m.reducingFile, reply.Jid)
			m.needReduceFile[reply.Jid] = reply.Jfiles
		}
		m.mu.Unlock()
	}
}

//
// worker.go calls FinishJob to indicate it has finish the job
//
func (m *Master) FinishJob(args *JobReply, reply *EmptyReply) error {
	m.channels[args.Jid] <- true
	return nil
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.status == 2
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.status = MAPPING
	m.jid = 0
	m.nReduce = nReduce
	m.needMapFile = make(map[string]bool)
	m.mappingFile = make(map[string]bool)
	m.needReduceFile = make(map[int][]string)
	m.reducingFile = make(map[int][]string)
	m.channels = make(map[int]chan bool)

	for _, file := range files {
		m.needMapFile[file] = true
	}
	// Your code here.

	m.server()
	return &m
}
