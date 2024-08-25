package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
)
import "strconv"

type NilArg struct{}

type WorkerIdArg struct {
	WorkerId int32
}

type WorkerInitReply struct {
	NReduce int32
	Number  int32
}

type JobIdListReply struct {
	JobIds []int
}

const (
	Wait = iota
	Map
	Reduce
	Exist
)

type Job struct {
	JobId      int
	JobType    int
	JobName    string
	WorkerId   int32
	ResultFile []string
}

func (job Job) String() string {
	return fmt.Sprintf("{%v, %v}", GetJobType(job.JobType), job.JobName)
}

func GetJobType(jobType int) string {
	if jobType == Map {
		return "Map"
	}
	return "Reduce"
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
