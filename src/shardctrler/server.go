package shardctrler

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	JOIN = iota
	LEAVE
	MOVE
	QUERY
)

type Command struct {
	ClientId int64
	CmdId    int32

	Type      int
	QueryArgs QueryArgs
	JoinArgs  JoinArgs
	LeaveArgs LeaveArgs
	MoveArgs  MoveArgs

	Status Err
	Conf   Config
}

func (cmd *Command) String() string {
	return fmt.Sprintf("{ClientId:%d, CmdId:%d, Type:%d}", cmd.ClientId, cmd.CmdId, cmd.Type)
}

func buildCmd(opType int, clientId int64, cmdId int32, args Args) *Command {
	return &Command{
		Type:     opType,
		ClientId: clientId,
		CmdId:    cmdId,
		Status:   OK,
	}
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	killCh   chan bool
	raftTerm int32
	//记录已经执行的最大的LogEntry的Index
	appliedLogIdx int32
	prevCmd       map[int64]*Command
	submitCmd     map[int]*Command

	history map[int64]map[int32]*Config
	//记录Server已经执行的最大的Command Index
	matchIndex map[int64]int32

	configCnt int32
	configs   []Config // indexed by config num
}

func (sc *ShardCtrler) getConfigNo() int {
	return int(atomic.AddInt32(&sc.configCnt, 1))
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	DPrintf("[%v]Received Join RPC:%v From Client", sc.getServerDetail(), args)
	cmd := buildCmd(JOIN, args.ClientId, args.CommandId, args)
	cmd.JoinArgs = *args
	reply.Err = sc.submit(cmd)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	DPrintf("[%v]Received Leave RPC:%v From Client", sc.getServerDetail(), args)
	cmd := buildCmd(LEAVE, args.ClientId, args.CommandId, args)
	cmd.LeaveArgs = *args
	reply.Err = sc.submit(cmd)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	DPrintf("[%v]Received Move RPC:%v From Client", sc.getServerDetail(), args)
	cmd := buildCmd(MOVE, args.ClientId, args.CommandId, args)
	cmd.MoveArgs = *args
	reply.Err = sc.submit(cmd)
}

func (sc *ShardCtrler) submit(cmd *Command) (err Err) {
	// 检查是否重复请求
	sc.mu.Lock()
	if sc.checkIfCommandAlreadyExecuted(cmd.ClientId, cmd.CmdId) {
		DPrintf("[%s]Dupliacte %d Reuqest %v", sc.getServerDetail(), cmd.Type, cmd)
		sc.mu.Unlock()
		return OK
	}
	sc.mu.Unlock()

	sc.submitCommand(cmd)
	err = cmd.Status
	if err != OK {
		DPrintf("[%v]Command %v failed:%v", sc.getServerDetail(), cmd, err)
	}
	return err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	DPrintf("[%v]Received Query RPC:%v From Client", sc.getServerDetail(), args)
	clientId, cmdId := args.ClientId, args.CommandId

	// 如果是重复命令，直接返回之前的结果
	sc.mu.Lock()
	if sc.checkIfCommandAlreadyExecuted(clientId, cmdId) {
		DPrintf("[%s]Dupliacte Query Reuqest %v", sc.getServerDetail(), args)
		reply.Err = OK
		reply.Config = *sc.history[clientId][cmdId]
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	cmd := buildCmd(QUERY, args.ClientId, args.CommandId, args)
	cmd.QueryArgs = *args
	sc.submitCommand(cmd)
	reply.Err = cmd.Status
	if reply.Err != OK {
		DPrintf("[%s]Submit Command %v Failed:%v", sc.getServerDetail(), args, reply.Err)
		return
	}
	reply.Config = cmd.Conf
}

func (sc *ShardCtrler) submitCommand(cmd *Command) {
	DPrintf("[%v]SubmitCommand", sc.getServerDetail())
	sc.mu.Lock()
	cmdIdx, term, isLeader := sc.rf.Start(*cmd)
	if !isLeader {
		cmd.Status = ErrWrongLeader
		sc.mu.Unlock()
		return
	}
	sc.setRaftTerm(int32(term))
	sc.submitCmd[cmdIdx] = cmd
	sc.mu.Unlock()

	DPrintf("[%s]Submit Command %v To Raft, Index:%d, Term:%d", sc.getServerDetail(), cmd, cmdIdx, term)
	for !sc.killed() && sc.getAppliedLogIdx() < int32(cmdIdx) && sc.getRaftTerm() == int32(term) {
	}

	if cmd.Status == LogNotMatch {
		DPrintf("[%s]Command %v Not Match, Need Re-Submit To KVServer", sc.getServerDetail(), cmd)
	}

	if sc.getRaftTerm() != int32(term) {
		cmd.Status = TermChanged
		DPrintf("[%s]Command %v Is Expired, SubmitTerm:%d, CurrentTerm:%d, Need Re-Submit To KVServer",
			sc.getServerDetail(), cmd, term, sc.getRaftTerm())
	}

	if sc.killed() {
		cmd.Status = Killed
	}
}

func (sc *ShardCtrler) ticker() {
	DPrintf("[%s]Ticker Start", sc.getServerDetail())
	for {
		select {
		case msg := <-sc.applyCh:
			sc.mu.Lock()
			cmdIdx, cmd := msg.CommandIndex, msg.Command.(Command)
			DPrintf("[%s]Receive Command %d:%v From Raft applyChan", sc.getServerDetail(), cmdIdx, &cmd)
			sc.applyCommand(cmdIdx, &cmd)
			sc.setAppliedLogIdx(int32(cmdIdx))
			sc.mu.Unlock()
		case <-sc.killCh:
			DPrintf("[%s]Sever Been Killed, Ticker End", sc.getServerDetail())
			return
		}
	}
}

func (sc *ShardCtrler) monitorTerm() {
	for !sc.killed() {
		term, _ := sc.rf.GetState()
		if int32(term) != sc.getRaftTerm() {
			DPrintf("[%s]Raft Term Change:%d-->%d", sc.getServerDetail(), sc.getRaftTerm(), term)
			sc.setRaftTerm(int32(term))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (sc *ShardCtrler) applyCommand(cmdIdx int, cmd *Command) {
	// 执行Command到本地状态机
	prevCmd, ok := sc.prevCmd[cmd.ClientId]
	if !ok || !compareCmd(cmd, prevCmd) {
		switch cmd.Type {
		case JOIN:
			sc.applyJoinCmd(&cmd.JoinArgs)
		case LEAVE:
			sc.applyLeaveCmd(&cmd.LeaveArgs)
		case MOVE:
			sc.applyMoveCmd(&cmd.MoveArgs)
		default:
		}
	}
	sc.prevCmd[cmd.ClientId] = cmd

	// 检查CmdIdx所在位置的Command是否发生改变
	cmd2, ok := sc.submitCmd[cmdIdx]
	if !ok {
		return
	}
	if !compareCmd(cmd, cmd2) {
		DPrintf("[%v]Log At %d Not Match, Return", sc.getServerDetail(), cmdIdx)
		cmd2.Status = LogNotMatch
		return
	}

	cmd2.Status = OK
	clientId, cmdId := cmd2.ClientId, cmd2.CmdId
	sc.matchIndex[clientId] = maxi32(sc.matchIndex[clientId], cmdId)
	DPrintf("[%v]Update Client %d's Match Index To %d", sc.getServerDetail(), clientId, sc.matchIndex[clientId])
	// 处理QueryCmd
	if cmd2.Type == QUERY {
		conf := sc.applyQueryCmd(&cmd2.QueryArgs)
		cmd2.Conf = Config{
			Num:    conf.Num,
			Shards: conf.Shards,
			Groups: make(map[int][]string),
		}
		//for idx, val := range sc.configs {
		//	DPrintf("Query: %d: %v", idx, val)
		//}
		for k, v := range conf.Groups {
			cmd2.Conf.Groups[k] = make([]string, len(v))
			copy(cmd2.Conf.Groups[k], v)
		}
		if sc.history[clientId] == nil {
			sc.history[clientId] = make(map[int32]*Config)
		}
		sc.history[clientId][cmdId] = conf
	}
	delete(sc.submitCmd, cmdIdx)
}

func (sc *ShardCtrler) applyJoinCmd(args *JoinArgs) {
	oldConf := sc.getLatestConfig()
	newConf := sc.createNewConfig()

	// 复制旧的Groups到NewConfig
	for gid, serverAddr := range oldConf.Groups {
		newConf.Groups[gid] = make([]string, len(serverAddr))
		copy(newConf.Groups[gid], serverAddr)
	}

	// 将新的Groups也添加进来,并记录新来的Gid
	var newGidList []int
	for gid, serverAddr := range args.Servers {
		newConf.Groups[gid] = make([]string, len(serverAddr))
		copy(newConf.Groups[gid], serverAddr)

		newGidList = append(newGidList, gid)
	}

	// 对新来的Gid进行排序，保证所有节点分配顺序一致
	sort.Slice(newGidList, func(i, j int) bool {
		return newGidList[i] < newGidList[j]
	})

	// 计算每个Gid应该负责几个Shard,向下取整
	times := NShards / len(newConf.Groups)

	// 统计Gid负责的Shard数
	gidCnt := make(map[int]int)
	newConf.Shards = oldConf.Shards
	// 将新的Gid分配给Shard
	for idx, newGidIdx := 0, 0; idx < NShards; idx++ {
		oldGid := oldConf.Shards[idx]
		gidCnt[oldGid]++
		DPrintf("Time:%d Gid:%d Cnt:%d", times, oldGid, gidCnt[oldGid])
		if gidCnt[oldGid] > times || newConf.Shards[idx] == 0 {
			newConf.Shards[idx] = newGidList[newGidIdx]
			newGidIdx = (newGidIdx + 1) % len(newGidList)
		}
	}

	sc.configs = append(sc.configs, *newConf)
}

func (sc *ShardCtrler) applyLeaveCmd(args *LeaveArgs) {
	leaveGids := args.GIDs
	oldConf := sc.getLatestConfig()
	newConf := sc.createNewConfig()
	gidCnt := make(map[int]int)

	// 将旧的Groups除去要Leave的，其余添加进newConf
	for gid, serverAddr := range oldConf.Groups {
		if contains(leaveGids, gid) {
			continue
		}
		newConf.Groups[gid] = make([]string, len(serverAddr))
		copy(newConf.Groups[gid], serverAddr)
		gidCnt[gid] = 0
	}

	// gidCnt长度为0，即所有Gid都为空
	if len(gidCnt) != 0 {
		// 把旧的Shard分配情况复制进NewConf
		newConf.Shards = oldConf.Shards
		// 统计之前Shard对Gid的使用频率
		for _, gid := range oldConf.Shards {
			if contains(leaveGids, gid) {
				continue
			}
			gidCnt[gid]++
		}
		// 对Gid按照使用频率进行排序,频率相同就按照Gid排序
		var gidList []Pair
		for gid, cnt := range gidCnt {
			gidList = append(gidList, Pair{gid, cnt})
		}
		sort.Slice(gidList, func(i, j int) bool {
			if gidList[i].cnt == gidList[j].cnt {
				return gidList[i].gid < gidList[j].gid
			}
			return gidList[i].cnt < gidList[j].cnt
		})

		// 计算每个Gid应该负责几个Shard，向下取整
		times := NShards / len(newConf.Groups)
		newConf.Shards = oldConf.Shards
		// 将新的Gid分配给Shard
		for idx, newGidIdx := 0, 0; idx < NShards; idx++ {
			oldGid := newConf.Shards[idx]
			if gidCnt[oldGid] > times || contains(leaveGids, oldGid) {
				newConf.Shards[idx] = gidList[newGidIdx].gid
				newGidIdx = (newGidIdx + 1) % len(gidList)
			}
		}
	}
	sc.configs = append(sc.configs, *newConf)
}

func (sc *ShardCtrler) applyMoveCmd(cmd *MoveArgs) {
	oldConf := sc.getLatestConfig()
	newConf := sc.createNewConfig()
	newConf.Shards = oldConf.Shards
	for gid, serverAddr := range oldConf.Groups {
		newConf.Groups[gid] = make([]string, len(serverAddr))
		copy(newConf.Groups[gid], serverAddr)
	}
	newConf.Shards[cmd.Shard] = cmd.GID
	sc.configs = append(sc.configs, *newConf)
}

func (sc *ShardCtrler) applyQueryCmd(args *QueryArgs) (conf *Config) {
	if args.Num == -1 || args.Num >= len(sc.configs) {
		return sc.getLatestConfig()
	}
	return &sc.configs[args.Num]
}

func (sc *ShardCtrler) Report(args *QueryArgs, reply *QueryReply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	clientID, cmdId := args.ClientId, args.CommandId
	DPrintf("[%v]Receive Report RPC %v, Delete History", sc.getServerDetail(), args)
	delete(sc.history[clientID], cmdId)
	reply.Err = OK
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	sc.killCh <- true
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Command{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.killCh = make(chan bool)
	sc.history = make(map[int64]map[int32]*Config)
	sc.matchIndex = make(map[int64]int32)
	sc.prevCmd = make(map[int64]*Command)
	sc.submitCmd = make(map[int]*Command)

	go sc.ticker()
	go sc.monitorTerm()

	return sc
}
