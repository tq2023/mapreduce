package mr

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)
import "net"
import "net/rpc"

// 在一定时间内检查任务情况（本实验中为 10 秒）
var overTime time.Duration = 10 * time.Second

type Master struct {
	files    []string           // 输入文件的列表
	nReduce  int                // reduce 任务的数量
	nMap     int                // map 任务的数量
	mapCh    chan int           // 一个缓冲通道，用于分配 map 任务
	reduceCh chan int           // 一个缓冲通道，用于分配 reduce 任务
	wgMap    sync.WaitGroup     // 一个 WaitGroup，用于等待所有 map 任务完成
	wgReduce sync.WaitGroup     // 一个 WaitGroup，用于等待所有 reduce 任务完成
	onceMap  sync.Once          // 一个 Once，用于保证只执行一次 map 完成的操作
	ctx      context.Context    // 一个 Context，用于控制所有协程的退出
	cancel   context.CancelFunc // 一个 CancelFunc，用于取消 Context
	mu       sync.Mutex         // 用于保护共享数据的锁
	timers   []*time.Timer      // 用于定时检查任务状态的计时器切片
}

func (m *Master) AssignTask(args *WorkerArgs, reply *WorkerReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	select {
	case i := <-m.mapCh:
		//if ok {
		reply.TaskType = "map"
		reply.TaskNum = i
		reply.Files = m.files
		reply.NReduce = m.nReduce
		reply.StartTime = time.Now()
		// 启动对应编号的计时器
		m.timers[i].Reset(overTime)
		go m.checkTask("map", i) // 启动一个协程检查任务状态
		//}

	case i := <-m.reduceCh:
		reply.TaskType = "reduce"
		reply.TaskNum = i
		reply.Files = m.files
		reply.NReduce = m.nReduce
		reply.StartTime = time.Now()
		m.timers[i+m.nMap].Reset(overTime)
		go m.checkTask("reduce", i)
	case <-m.ctx.Done(): // 如果 context 被取消，说明所有 reduce 任务完成了
		reply.TaskType = "exit"
	default:
		reply.TaskType = "wait"
	}
	return nil
}

// 定义一个 server 方法，接收一个 Master 类型的指针作为接收者
func (m *Master) server() {
	// 注册 Master 的方法，使用默认的编解码器
	rpc.Register(m)
	// 创建一个 Unix 域套接字的地址
	sockname := masterSock()
	// 删除可能已经存在的同名文件
	os.Remove(sockname)
	// 在该地址上监听 Unix 域套接字的连接
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// 启动一个新的 goroutine，用于接受和处理客户端的连接请求
	go func() {
		for {
			// 接受一个新的连接，如果出错则打印日志并退出
			conn, err := l.Accept()
			if err != nil {
				log.Fatal("accept error:", err)
				continue
			}

			// 启动另一个新的 goroutine，用于在该连接上提供 rpc 服务
			go rpc.ServeConn(conn)
		}
	}()
}

// Done
// main/mrmaster.go定期调用Done()来查找
func (m *Master) Done() bool {
	ret := false

	// 等待所有的 map 任务完成
	m.wgMap.Wait()
	// 保证只执行一次 map 完成的操作
	m.onceMap.Do(func() {
		// 关闭 mapCh，表示没有更多的 map 任务了
		//close(m.mapCh)/**/
		log.Println("all map tasks done")
	})

	// 等待所有的 reduce 任务完成
	m.wgReduce.Wait()
	// 取消 context，通知所有协程退出
	m.cancel()
	ret = true
	log.Println("all reduce tasks done")
	time.Sleep(2 * time.Second)
	return ret
}

// MakeMaster
// files 是文件的路径加名字
// nReduce 是要使用的 reduce 任务的个数。
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.files = files
	m.nReduce = nReduce
	m.nMap = len(files) //map 任务的数量

	// 创建 mapCh、reduceCh 并分别指定它们的容量为 nMap、nReduce
	m.mapCh = make(chan int, m.nMap)
	m.reduceCh = make(chan int, m.nReduce)

	// 初始化 WaitGroup，分别设置计数器为 nMap 和 nReduce
	m.wgMap.Add(m.nMap)
	m.wgReduce.Add(m.nReduce)
	m.ctx, m.cancel = context.WithCancel(context.Background())

	// 初始化 Once
	m.onceMap = sync.Once{}
	// 初始化锁
	m.mu = sync.Mutex{}
	// 初始化计时器切片，长度为 map 任务数加 reduce 任务数
	m.timers = make([]*time.Timer, m.nMap+m.nReduce)
	for i := range m.timers {
		// 初始化每个计时器，但不启动
		m.timers[i] = time.NewTimer(0)
		if !m.timers[i].Stop() {
			// 清空信号通道，避免阻塞
			<-m.timers[i].C
		}
	}

	// 启动两个 goroutine，分别调用 assignMapTask 和 assignReduceTask 函数
	go m.assignMapTask()
	go m.assignReduceTask()

	m.server()
	return &m
}

// 负责分发 map 任务，向 mapCh 中发送 Map 任务编号。
func (m *Master) assignMapTask() {

	for i := 0; i < m.nMap; i++ { // 遍历 [0, nMap) 的每个整数 i，表示 Map 任务编号
		//m.mapCh <- i // 将 i 发送到 mapCh 中
		m.mapCh <- i
	}

}

// 负责向 reduceCh 中发送 Reduce 任务编号。
func (m *Master) assignReduceTask() {
	m.wgMap.Wait() // 等待所有的 map 任务完成

	for i := 0; i < m.nReduce; i++ {
		m.reduceCh <- i
	}
}

// checkTask是用于检查任务状态的协程函数，
// taskType 任务类型
// taskNum 任务编号
func (m *Master) checkTask(taskType string, taskNum int) {
	var timer *time.Timer
	if taskType == "map" {
		timer = m.timers[taskNum] // 获取对应编号的计时器指针
	} else if taskType == "reduce" {
		timer = m.timers[m.nMap+taskNum] // 注意偏移量
	}
	select {
	case <-timer.C: // 如果计时器到期，说明任务超时了
		m.mu.Lock() // 加锁，避免数据竞争
		if taskType == "map" {

			m.mapCh <- taskNum
			log.Printf("map task %d finished\n", taskNum)

		} else if taskType == "reduce" {
			m.reduceCh <- taskNum // 重新将任务编号放回reduceCh中，等待分配给其他worker
			log.Printf("reduce task %d timeout, reassign\n", taskNum)
		}
		m.mu.Unlock() // 解锁

		return
	case <-time.After(overTime): // 如果在2秒内没有收到计时器的信号，说明任务已经完成了
		m.mu.Lock() // 加锁，避免数据竞争
		if taskType == "reduce" {
			// 拼接文件的名称
			fileName := fmt.Sprintf("mr-out-%d", taskNum)
			// 判断文件是否存在
			if _, err := os.Stat(fileName); os.IsNotExist(err) {
				// 如果文件不存在，重新将任务编号放回reduceCh中
				m.reduceCh <- taskNum
				log.Printf("reduce task %d finished, but file %s not found, reassign\n", taskNum, fileName)
			}
		}
		m.mu.Unlock() // 解锁
		return        // 直接返回，不做任何操作
	}
}

func (m *Master) Feedback(args *FeedbackArgs, reply *FeedbackReply) error {
	m.mu.Lock()
	defer m.mu.Unlock() // 延迟解锁
	var timer *time.Timer

	if args.TaskType == "map" {
		var tempbool = false
		for j := 0; j < m.nReduce; j++ {
			filename := fmt.Sprintf("mr-%d-%d", args.TaskNum, j) // 拼接文件的名称
			if _, err := os.Stat(filename); os.IsNotExist(err) {
				// 如果文件不存在，重新将任务编号放回mapCh中
				m.mapCh <- args.TaskNum
				tempbool = true
				log.Printf("map task %d finished, but file %s not found, reassign\n", args.TaskNum, filename)
				break // 跳出循环
			}
		}
		if !tempbool {
			m.wgMap.Done() // 调用 WaitGroup 的 Done 方法
		}
		// 获取对应编号的计时器指针
		timer = m.timers[args.TaskNum]

	} else if args.TaskType == "reduce" {

		m.wgReduce.Done() // 调用 WaitGroup 的 Done 方法

		// 注意偏移量
		timer = m.timers[m.nMap+args.TaskNum]
	}
	reply.OK = true // 设置RPC返回值为true，表示成功接收到反馈信息

	endTime := time.Now()
	// 计算任务耗时
	duration := endTime.Sub(args.StartTime)
	log.Printf("%s task %d finished in %v\n", args.TaskType, args.TaskNum, duration)
	// 重置计时器
	if timer.Reset(overTime) { // 重置并重新启动计时器，如果返回true，说明计时器还没有到期
		timer.Stop() // 停止计时器
	} else { // 如果返回false，说明计时器已经到期，信号通道可能有值
		select {
		case <-timer.C: // 清空信号通道，避免阻塞
		default:
		}
	}
	return nil
}
