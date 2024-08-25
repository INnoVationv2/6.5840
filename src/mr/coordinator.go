package mr

import (
	"fmt"
	"log"
	"strconv"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	nReduce int32
	stage   int32

	workerCnt int32

	reduceJobIds []int

	mapJobPendingList    *HashSet
	reduceJobPendingList *HashSet
	jobProcessingList    *HashSet
}

func (c *Coordinator) GetWorkerConf(arg *WorkerIdArg, reply *WorkerInitReply) error {
	reply.NReduce = c.nReduce
	reply.Number = atomic.AddInt32(&c.workerCnt, 1)
	//log.Printf("[Coordinator]GetWorkerConf:New Worker %d Connect.\n", reply.Number)
	return nil
}

func (c *Coordinator) GetJobIdList(arg *WorkerIdArg, reply *JobIdListReply) error {
	//log.Printf("[Coordinator]GetWorkerList:Worker %d request Worker List.\n", arg.WorkerId)
	reply.JobIds = c.reduceJobIds
	return nil
}

func (c *Coordinator) GetJob(arg *WorkerIdArg, jobReply *Job) error {
	jobPendingList := c.getPendingJobList()
	jobProcessingList := c.getProcessingJobList()

	if c.Done() {
		jobReply.JobType = Exist
		//log.Println("[Coordinator]All Job Are Complete, Ask All Worker Exit.")
		return nil
	}

	job := jobPendingList.GetFirstElem()
	if job.JobType == 0 {
		//log.Printf("[Coordinator]GetJob:Worker %d Request Job, But All Map Job Is Complete.\n", arg.WorkerId)
		return nil
	}

	jobReply.JobId = job.JobId
	jobReply.JobType = job.JobType
	jobReply.JobName = job.JobName
	jobReply.ResultFile = []string{}
	jobPendingList.Remove(jobReply)
	jobProcessingList.Add(*jobReply)

	go c.timeoutMonitor(jobReply)
	//log.Printf("[Coordinator]GetJob:Worker %d Request Job, Dispatch Job:%v.\n", arg.WorkerId, jobReply)
	//log.Printf(c.getCoordinatorStatus())
	return nil
}

func (c *Coordinator) FinishJob(job *Job, reply *NilArg) error {
	//log.Printf("[Coordinator]FinishJob:Worker %d Finish Job:%v.\n", job.WorkerId, job.JobName)

	jobProcessingList := c.getProcessingJobList()
	jobPendingList := c.getPendingJobList()

	if !jobPendingList.Contains(job) && !jobProcessingList.Contains(job) {
		return nil
	}
	jobProcessingList.Remove(job)
	jobPendingList.Remove(job)

	// 当所有任务完成，切换成Reduce阶段
	if job.JobType == Map && jobPendingList.IsEmpty() && jobProcessingList.IsEmpty() {
		//log.Printf("[Coordinator]All Map Job Complete, Switch To Reduce Stage\n")
		atomic.CompareAndSwapInt32(&c.stage, Map, Reduce)
	}

	//log.Printf(c.getCoordinatorStatus())
	return nil
}

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
	//log.Printf("[Coordinator]Coordinator start, Waiting for Worker.\n")
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	ret := c.stage == Reduce &&
		c.getPendingJobList().IsEmpty() &&
		c.getProcessingJobList().IsEmpty()
	return ret
}

func (c *Coordinator) getPendingJobList() *HashSet {
	if c.stage == Map {
		return c.mapJobPendingList
	} else {
		return c.reduceJobPendingList
	}
}

func (c *Coordinator) getProcessingJobList() *HashSet {
	return c.jobProcessingList
}

func (c *Coordinator) timeoutMonitor(job *Job) {
	time.Sleep(10 * time.Second)

	jobProcessingList := c.getProcessingJobList()
	if jobProcessingList.Contains(job) {
		pendingJobList := c.getPendingJobList()
		pendingJobList.Add(*job)
		jobProcessingList.Remove(job)
		//log.Printf("[Coordinator]Job <%v> is timeout, Re-add to pendingJobList\n", job)
	}
}

func (c *Coordinator) monitorCoordinatorStatus() {
	for {
		//log.Printf(c.getCoordinatorStatus())
		time.Sleep(5 * time.Second)
	}
}

func (c *Coordinator) getCoordinatorStatus() string {
	stage := GetJobType(int(c.stage))
	pendingJobCount := c.getPendingJobList().Size()
	processingJobCount := c.getProcessingJobList().Size()
	return fmt.Sprintf("[Coordinator]{Stage:%v, "+
		"PendingJobCount:%d, "+
		"ProcessingJobCount:%d}\n",
		stage, pendingJobCount, processingJobCount)
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.stage = Map
	c.nReduce = int32(nReduce)
	c.workerCnt = 0

	c.mapJobPendingList = NewHashSet()
	c.reduceJobPendingList = NewHashSet()
	c.jobProcessingList = NewHashSet()

	var job Job
	for i := 0; i < len(files); i++ {
		c.reduceJobIds = append(c.reduceJobIds, i)
		job = Job{JobId: i, JobType: Map, JobName: files[i]}
		c.mapJobPendingList.Add(job)
	}

	for i := 0; i < nReduce; i++ {
		job = Job{JobId: i, JobType: Reduce, JobName: strconv.Itoa(i)}
		c.reduceJobPendingList.Add(job)
	}

	c.server()
	go c.monitorCoordinatorStatus()
	return &c
}
