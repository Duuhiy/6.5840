package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	MaxTaskRunTime   = 10 * time.Second
	ScheduleInterval = 1 * time.Second
)

const (
	TaskStatusReady   = 0
	TaskStatusQueue   = 1
	TaskStatusRunning = 2
	TaskStatusFinish  = 3
	TaskStatusErr     = 4
)

const (
	MapStatus    = 0
	ReduceStatus = 1
	DoneStatus   = 2
)

type taskStatus struct {
	Status    int
	Workerid  int
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	files      []string
	taskStatus []taskStatus // 记录每个task的状态信息,当前把每个file作为一个task
	nReduce    int
	taskCh     chan Task
	phase      int
	numWorker  int
}

var mu sync.Mutex

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *GetTaskArgs, resp *GetTaskResp) error {
	// 这里加锁会导致task channel空时阻塞在这里
	//mu.Lock()
	//defer mu.Unlock()
	t := <-c.taskCh
	resp.Task = t
	mu.Lock()
	defer mu.Unlock()
	c.taskStatus[t.TaskId].Status = TaskStatusRunning
	c.taskStatus[t.TaskId].StartTime = time.Now()
	c.taskStatus[t.TaskId].Workerid = args.WorkerId
	return nil
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, resp *RegisterWorkerResp) error {
	mu.Lock()
	defer mu.Unlock()

	c.numWorker++
	resp.Id = c.numWorker
	//fmt.Println("worker注册， id = ", resp.Id)
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, resp *ReportTaskResp) error {
	mu.Lock()
	defer mu.Unlock()

	if c.phase != args.Task.TaskType || c.taskStatus[args.Task.TaskId].Workerid != args.WorkerId {
		return nil
	}
	//fmt.Println("taskStatus", args.Task.TaskType)
	if args.Done {
		//fmt.Println(args.Task.TaskId, " 号任务完成")
		c.taskStatus[args.Task.TaskId].Status = TaskStatusFinish
	} else {
		//fmt.Println(args.Task.TaskId, " 号任务出错")
		c.taskStatus[args.Task.TaskId].Status = TaskStatusErr
	}
	//go c.schedule()
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

func (c *Coordinator) toTask(ind int) Task {
	t := Task{
		FileName: "",
		TaskId:   ind,
		TaskType: c.phase,
		NReduce:  c.nReduce,
		NMap:     len(c.files),
		//TaskStatus: TaskStatusQueue,
	}
	if c.phase == MapStatus {
		t.FileName = c.files[ind]
	}
	return t
}

func (c *Coordinator) schedule() {
	mu.Lock()
	//fmt.Println("schedule 获取到锁了")
	defer mu.Unlock()

	if c.phase == DoneStatus {
		return
	}

	finishMap := true
	//fmt.Println("c.phase : ", c.phase)
	for ind, t := range c.taskStatus {
		switch t.Status {
		case TaskStatusReady:
			finishMap = false
			c.taskCh <- c.toTask(ind)
			//t.Status = TaskStatusQueue	t是一个复制，修改t没用
			c.taskStatus[ind].Status = TaskStatusQueue
		case TaskStatusQueue:
			finishMap = false
		case TaskStatusRunning:
			finishMap = false
			if time.Now().Sub(t.StartTime) > MaxTaskRunTime {
				// 超时了，重新加入队列中
				//fmt.Println("超时了，重新加入队列中")
				c.taskStatus[ind].Status = TaskStatusQueue
				c.taskCh <- c.toTask(ind)
			}
		case TaskStatusFinish:
			//fmt.Println(ind, " 号任务已完成")
		case TaskStatusErr:
			//fmt.Println("出错了，重新加入队列中")
			finishMap = false
			c.taskStatus[ind].Status = TaskStatusQueue
			c.taskCh <- c.toTask(ind)
		default:
			panic("t.status err")
		}
	}
	//fmt.Println("一轮schedule完成")
	if finishMap {
		if c.phase == MapStatus {
			// 可以开始reduce
			//fmt.Println("map已完成，开始reduce")
			c.phase = ReduceStatus
			c.taskStatus = make([]taskStatus, c.nReduce)
		} else {
			c.phase = DoneStatus
		}
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	ret := false
	// Your code here.
	if c.phase == DoneStatus {
		ret = true
	}
	return ret
}

func (c *Coordinator) tickSchedule() {
	for c.phase != DoneStatus {
		go c.schedule()
		//fmt.Println(c.phase)
		time.Sleep(ScheduleInterval)
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//fmt.Println("MakeCoordinator")
	c := Coordinator{}
	// Your code here.
	c.nReduce = nReduce
	c.files = files
	if nReduce > len(files) {
		c.taskCh = make(chan Task, nReduce)
	} else {
		c.taskCh = make(chan Task, len(c.files))
	}
	c.phase = MapStatus
	c.taskStatus = make([]taskStatus, len(files))
	//fmt.Println("开始周期性调度")
	go c.tickSchedule()

	c.server()
	return &c
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
