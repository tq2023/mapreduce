package mr

import (
	"os"
	"strconv"
	"time"
)

// WorkerArgs
// 定义一个新的 RPC 参数类型和一个新的 RPC 应答类型
type WorkerArgs struct {
	//没有字段，worker 不需要向 master 发送任何参数
}

type WorkerReply struct {
	TaskType  string   // master 分配给 worker 的任务类型，可以是 "map"、"reduce" 或 "exit"
	TaskNum   int      // master 分配给 worker 的任务编号，可以是文件索引（对于 map）或 reduce 编号（对于 reduce）
	NReduce   int      // reduce 任务的数量
	Files     []string // 输入文件的列表
	StartTime time.Time
}

// FeedbackArgs
// 添加一个新的结构体：FeedbackArgs，表示工作线程发送的 RPC 请求参数。
// 它有三个字段：TaskType、TaskNum 和 Done。
// TaskType 是一个字符串，表示任务的类型，可以是 "map" 或 "reduce"。
// TaskNum 是一个整数，表示任务的编号。
// Done 是一个布尔值，表示任务是否完成。
type FeedbackArgs struct {
	TaskType  string
	TaskNum   int
	Done      bool
	StartTime time.Time
}

// FeedbackReply
// 添加一个新的结构体：FeedbackReply，表示主进程返回的 RPC 响应结果。
// 它只有一个字段：OK。
// OK 是一个布尔值，表示反馈是否成功。
type FeedbackReply struct {
	OK bool
}

func masterSock() string {
	//s := "/var/tmp/824-mr-"
	s := "E:\\go\\workspace\\mapreduce\\src\\output"
	s += strconv.Itoa(os.Getuid())
	return s
}
