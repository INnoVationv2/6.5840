package shardctrler

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
)

type Command struct {
	Args

	Conf Config
}

func (cmd *Command) String() string {
	return fmt.Sprintf("%v,Config:%v", &cmd.Args, cmd.Conf)
}

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32
	confDB  DB

	shutdownCh chan struct{}
	//记录已经执行的最大的LogEntry的Index
	appliedLogIdx int32
	submitCmd     map[int64]map[int32]*Reply

	history map[int64]map[int32]*Config
	//记录Server已经执行的最大的Command Index
	matchIndex map[int64]int32

	configCnt int32
}

func (sc *ShardCtrler) getConfigNo() int {
	return int(atomic.AddInt32(&sc.configCnt, 1))
}

func (sc *ShardCtrler) Join(args *Args, reply *Reply) {
	sc.submitCommand(args, reply)
}

func (sc *ShardCtrler) Leave(args *Args, reply *Reply) {
	sc.submitCommand(args, reply)
}

func (sc *ShardCtrler) Move(args *Args, reply *Reply) {
	sc.submitCommand(args, reply)
}

func (sc *ShardCtrler) Query(args *Args, reply *Reply) {
	sc.submitCommand(args, reply)
}

func (sc *ShardCtrler) submitCommand(args *Args, reply *Reply) {
	DPrintf("[%v]Received %s RPC:%v From Client", sc.getServerDetail(), args.Type, args)

	clientId, cmdId := args.ClientId, args.CommandId
	// 如果是重复请求，直接返回OK
	if val, ok := sc.getHistory(clientId, cmdId); ok {
		DPrintf("[%s]Dupliacte Reuqest %v", sc.getServerDetail(), args)
		reply.Status = OK
		if args.Type == QUERY {
			reply.Config = *val
		}
		return
	}
	cmd := &Command{Args: *args}

	sc.addSubmitCmd(cmd, reply)
	defer sc.deleteSubmitCmd(cmd)

	sc.submitCommandToRaft(cmd, reply)
	if reply.Status != OK {
		DPrintf("[%v]Command %v failed:%v", sc.getServerDetail(), cmd, reply.Status)
	}
}

func (sc *ShardCtrler) submitCommandToRaft(cmd *Command, reply *Reply) {
	DPrintf("[%v]SubmitCommand %v", sc.getServerDetail(), cmd)
	defer DPrintf("[%v]SubmitCommand %v Complete", sc.getServerDetail(), cmd)

	cmdIdx, term, isLeader := sc.rf.Start(*cmd)
	if !isLeader {
		DPrintf("[%v]Not Leader", sc.getServerDetail())
		reply.Status = ErrNotLeader
		return
	}
	DPrintf("[%v]Success Submit Command %v To Raft, CmdIdx:%d", sc.getServerDetail(), cmd, cmdIdx)

	for !sc.killed() && sc.getRaftTerm() <= term && sc.getAppliedLogIdx() < int32(cmdIdx) {
	}
}

func (sc *ShardCtrler) ticker() {
	DPrintf("[%s]Ticker Start", sc.getServerDetail())
	defer DPrintf("[%s]Ticker Stop", sc.getServerDetail())
	for {
		select {
		case msg := <-sc.applyCh:
			cmdIdx, cmd := int32(msg.CommandIndex), msg.Command.(Command)
			DPrintf("[%s]Receive Command[%d] %v From Raft applyChan\n", sc.getServerDetail(), cmdIdx, &cmd)
			sc.applyCommand(&cmd)
			sc.setAppliedLogIdx(cmdIdx)
		case <-sc.shutdownCh:
			DPrintf("[%s]Sever Been Killed, Ticker End", sc.getServerDetail())
			return
		}
	}
}

func (sc *ShardCtrler) applyCommand(cmd *Command) {
	DPrintf("[%v]Apply %v ", sc.getServerDetail(), cmd)
	defer DPrintf("[%v]Apply %v Complete", sc.getServerDetail(), cmd)

	if cmd.Type != QUERY && sc.matchIndex[cmd.ClientId] < cmd.CommandId {
		// 执行Command到本地状态机
		switch cmd.Type {
		case JOIN:
			sc.applyJoinCmd(&cmd.Args)
		case LEAVE:
			sc.applyLeaveCmd(&cmd.Args)
		case MOVE:
			sc.applyMoveCmd(&cmd.Args)
		}
	}

	var conf *Config
	// 处理QueryCmd
	if cmd.Type == QUERY {
		conf = sc.applyQueryCmd(&cmd.Args)
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.setHistory(cmd.ClientId, cmd.CommandId, conf)
	if cmd.CommandId > sc.matchIndex[cmd.ClientId] {
		sc.matchIndex[cmd.ClientId] = cmd.CommandId
	}
	if reply, ok := sc.submitCmd[cmd.ClientId][cmd.CommandId]; ok {
		reply.Status = OK
		if conf != nil {
			reply.Config = *conf
		}
	}
}

func (sc *ShardCtrler) applyMoveCmd(cmd *Args) {
	oldConf := sc.confDB.get(-1)
	newConf := sc.createNewConfByOldConf(oldConf)
	newConf.Shards[cmd.Shard] = cmd.GID
	sc.confDB.append(newConf)
}

func (sc *ShardCtrler) applyQueryCmd(args *Args) *Config {
	return sc.confDB.get(args.ConfigIdx)
}

// 新的GID加入，重新平均分配切片
func (sc *ShardCtrler) applyJoinCmd(args *Args) {
	oldConf := sc.confDB.get(-1)
	newConf := sc.createNewConfByOldConf(oldConf)

	// 将要Join的GID加入
	for gid, serverAddr := range args.Servers {
		newConf.Groups[gid] = make([]string, len(serverAddr))
		copy(newConf.Groups[gid], serverAddr)
	}

	reShard(newConf)
	sc.confDB.append(newConf)
}

func (sc *ShardCtrler) applyLeaveCmd(args *Args) {
	leaveGIDs := args.GIDs
	oldConf := sc.confDB.get(-1)
	newConf := sc.createNewConfByOldConf(oldConf)

	// 删除要Leave的GID
	for _, gid := range leaveGIDs {
		delete(newConf.Groups, gid)
	}

	// 将要Leave的GID负责的Shard置为未分配状态
	for shard, gid := range newConf.Shards {
		if contains(leaveGIDs, gid) {
			newConf.Shards[shard] = 0
		}
	}

	reShard(newConf)
	sc.confDB.append(newConf)
}

func reShard(conf *Config) {
	// 没有可用GID, 返回
	if len(conf.Groups) == 0 {
		conf.Shards = [NShards]int{}
		return
	}

	gidShardCnt := make(map[int]int)
	for gid := range conf.Groups {
		gidShardCnt[gid] = 0
	}
	// 统计当前每个GID负责的Shard个数
	for _, gid := range conf.Shards {
		if gid != 0 {
			gidShardCnt[gid]++
		}
	}

	// 按照GID负责的Shard数从小到大排序，如果负责的Shard数相同，则按照Gid本身排序
	// 确保所有节点分配顺序一致，分配结果相同
	var gidList []Pair
	for gid, cnt := range gidShardCnt {
		gidList = append(gidList, Pair{gid, cnt})
	}
	sort.Slice(gidList, func(i, j int) bool {
		a, b := gidList[i], gidList[j]
		return a.cnt < b.cnt || (a.cnt == b.cnt && a.gid < b.gid)
	})

	// 计算每个Gid负责的Shard上限,至少负责1个
	times := maxInt(NShards/len(conf.Groups), 1)

	// 重分配Shard给Gid，尽可能少的移动Shard
	for shardIdx, gidIdx := 0, 0; shardIdx < NShards; shardIdx++ {
		oldGid := conf.Shards[shardIdx]
		if oldGid != 0 && gidShardCnt[oldGid] <= times {
			continue
		}

		// 寻找可分配的 GID
		for gidIdx < len(gidList) && gidShardCnt[gidList[gidIdx].gid] >= times {
			gidIdx++
		}
		if gidIdx >= len(gidList) {
			return
		}
		newGid := gidList[gidIdx].gid
		conf.Shards[shardIdx] = newGid
		gidShardCnt[newGid]++
		if oldGid != 0 {
			gidShardCnt[oldGid]--
		}
	}
}

func (sc *ShardCtrler) Report(args *Args, reply *Reply) {
	DPrintf("[%v]Receive Report RPC %v, Delete History", sc.getServerDetail(), args)
	sc.deleteHistory(args.ClientId, args.CommandId)
	reply.Status = OK
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	close(sc.shutdownCh)
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

func (sc *ShardCtrler) killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	labgob.Register(Command{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.confDB = buildInMemoryDB()

	sc.shutdownCh = make(chan struct{})
	sc.history = make(map[int64]map[int32]*Config)
	sc.matchIndex = make(map[int64]int32)
	sc.submitCmd = make(map[int64]map[int32]*Reply)

	go sc.ticker()

	return sc
}
