package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type _Worker struct {
	id       int32
	nReduce  int32
	connFail int32

	jobIds []int

	job  Job
	mapf func(string, string) []KeyValue
	//reduceIdx int32
	reducef func(string, []string) string
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (w *_Worker) getBucketNo(key string) int {
	return ihash(key) % int(w.nReduce)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	worker := _Worker{}
	worker.mapf = mapf
	worker.reducef = reducef
	worker.init()
	for {
		worker.getJob()

		if worker.job.JobType == Wait {
			log.Printf("[Worker %d]Job Type Is Wait, Sleep 1 Seconds To Request Job.\n", worker.id)
			time.Sleep(1 * time.Second)
			continue
		}

		worker.handleJob()
		worker.finishJob()
	}
}

// initialize Worker, set Worker number & nReduce
func (w *_Worker) init() {
	args := NilArg{}
	reply := WorkerInitReply{}
	ok := call("Coordinator.GetWorkerConf", &args, &reply)
	if !ok {
		log.Fatalf("Worker/init() failed!\n")
	}
	w.id = reply.Number
	w.nReduce = reply.NReduce

	log.Printf("[Worker %d]Worker Start.\n", w.id)
}

func (w *_Worker) getWorkerList() {
	args := WorkerIdArg{}
	reply := JobIdListReply{}
	ok := call("Coordinator.GetJobIdList", &args, &reply)
	if !ok {
		log.Fatalf("[Worker %d]Worker/init() failed!\n", w.id)
	}
	w.jobIds = reply.JobIds

	log.Printf("[Worker %d]Worker Start.\n", w.id)
}

// Get Job From Coordinator
func (w *_Worker) getJob() {
	args := WorkerIdArg{WorkerId: w.id}
	job := Job{}

	ok := call("Coordinator.GetJob", &args, &job)
	if !ok {
		fmt.Printf("[Worker %d]Worker/getJob() failed!\n", w.id)
		w.connFail++
		if w.connFail == 5 {
			log.Fatalf("[Worker %d]Can't Connect To Coordinator!\n", w.id)
		}
	}
	w.connFail = 0

	job.WorkerId = w.id
	w.job = job
	if w.job.JobType == Exist {
		log.Printf("[Worker %d]All Job Are Complete, Worker will exist", w.id)
		os.Exit(0)
	}
	//log.Printf("[Worker %d]Get Job:%v.\n", w.id, w.job)
}

func (w *_Worker) handleJob() {
	if w.job.JobType == Map {
		w.handleMap()
	} else {
		if len(w.jobIds) == 0 {
			w.getWorkerList()
		}
		w.handleReduce()
	}
}

func (w *_Worker) loadFile() string {
	var filenameList []string
	if w.job.JobType == Map {
		//log.Printf("[Worker %d]loadFile: Load File ：%v", w.id, w.job.JobName)
		filenameList = append(filenameList, w.job.JobName)
	} else {
		for _, jobId := range w.jobIds {
			filename := fmt.Sprintf("mr-%d-%v", jobId, w.job.JobName)
			filenameList = append(filenameList, filename)
			//log.Printf("[Worker %d]loadFile: Load File ：%v", w.id, filename)
		}
	}

	var result string
	for _, filename := range filenameList {
		file, err := os.Open(filename)

		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}

		file.Close()
		result += string(content)
	}
	return result
}

func (w *_Worker) handleMap() {
	//log.Printf("[Worker %d]handleMap:Start Handle Map Job:%v.\n", w.id, w.job)

	content := w.loadFile()
	var kva []KeyValue
	if w.job.JobType == Map {
		kva = w.mapf(w.job.JobName, content)
	}
	w.storeMapResult(kva)

	//log.Printf("[Worker %d]handleMap:Map Job %v Complete.\n", w.id, w.job)
}

func (w *_Worker) storeMapResult(kva []KeyValue) {
	buckets := make([][]KeyValue, w.nReduce)
	for i := 0; i < len(kva); i++ {
		elem := kva[i]
		idx := w.getBucketNo(elem.Key)
		buckets[idx] = append(buckets[idx], elem)
	}

	var filenames []string
	for i := 0; i < int(w.nReduce); i++ {
		bucket := buckets[i]
		filename := fmt.Sprintf("mr-%d-%d_", w.job.JobId, i)
		file, _ := os.CreateTemp(".", filename)
		//file, _ := os.Create(filename)
		for _, elem := range bucket {
			fmt.Fprintf(file, "%v %v\n", elem.Key, elem.Value)
		}
		filenames = append(filenames, file.Name()[2:])
		file.Close()
	}

	for idx := range filenames {
		filename := filenames[idx]
		newName := strings.Split(filename, "_")[0]
		os.Rename(filename, newName)
	}

	w.job.ResultFile = filenames
}

func (w *_Worker) handleReduce() {
	log.Printf("[Worker %d]handleReduce:Start Handle Reduce Job:%v.\n", w.id, w.job)

	content := w.loadFile()
	intermediate := handleReduceLoadFile(&content)
	sort.Sort(ByKey(intermediate))

	filename := fmt.Sprintf("mr-out-%v_", w.job.JobName)
	resultFile, _ := os.CreateTemp(".", filename)
	w.job.ResultFile = append(w.job.ResultFile, resultFile.Name())
	//resultFile, _ := os.Create(filename)
	defer resultFile.Close()

	var result []KeyValue
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.reducef(intermediate[i].Key, values)
		result = append(result, KeyValue{Key: intermediate[i].Key, Value: output})
		fmt.Fprintf(resultFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	filename = resultFile.Name()[2:]
	newName := strings.Split(filename, "_")[0]
	os.Rename(filename, newName)
	w.job.ResultFile = append(w.job.ResultFile, newName)

	//log.Printf("[Worker %d]handleReduce:Reduce Job %v Complete.\n", w.id, w.job)
}

func handleReduceLoadFile(content *string) []KeyValue {
	var result []KeyValue
	lines := strings.Split(*content, "\n")
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		kv := strings.Split(line, " ")
		result = append(result, KeyValue{Key: kv[0], Value: kv[1]})
	}
	return result
}

func (w *_Worker) finishJob() {
	job := w.job
	reply := WorkerInitReply{}
	ok := call("Coordinator.FinishJob", &job, &reply)
	if !ok {
		log.Fatalf("[Worker %d]Worker/FinishJob() failed!\n", w.id)
	}
	log.Printf("[Worker %d]Job %v Finished.\n", w.id, job)
}

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
