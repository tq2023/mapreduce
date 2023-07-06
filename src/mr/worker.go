package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/rpc"

	"io"
	"os"
	"sort"
	"time"
)
import "log"
import "hash/fnv"

var nReduce int    // reduce任务的个数
var files []string //输入文件列表

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker
// main/mrworker.go调用这个函数。
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// 不断发送 RPC 请求并执行任务
	for {

		// 创建一个 WorkerArgs 结构体，并将其作为指针传入 call 函数中
		args := WorkerArgs{}
		reply := &WorkerReply{}

		err := call("Master.AssignTask", &args, &reply, "1234")
		if err != nil {
			log.Printf("RPC 请求失败")
			time.Sleep(time.Second)
			continue
		}

		// 将 NReduce 和 Files 的值赋值给全局变量
		if reply.NReduce > 0 {
			nReduce = reply.NReduce // 获取 reduce 任务的数量

		}
		if len(reply.Files) > 0 {
			files = reply.Files // 获取输入文件的列表
		}

		//  根据 reply.TaskType 的值执行不同的逻辑
		switch reply.TaskType {
		case "map":
			doMap(mapf, reply.TaskNum, nReduce)
			feedback("map", reply.TaskNum, reply.StartTime)
		case "reduce":
			doReduce(reducef, reply.TaskNum, len(files))
			feedback("reduce", reply.TaskNum, reply.StartTime)
		case "wait":
			time.Sleep(2 * time.Second)
			log.Printf("等待3s")
		case "exit":
			log.Printf("任务全部完成,退出")
			os.Exit(0)
		default:
			log.Printf("未知的任务类型:%v", reply.TaskType)
			break
		}
		time.Sleep(time.Second)
	}
}

// 中间文件的数量等于map任务的总数乘以reduce任务的总数，即nMap * nReduce。
func doMap(mapf func(string, string) []KeyValue, fileNum int, nReduce int) {
	filename := files[fileNum] // 根据 fileNum 获取输入文件的名称

	content, err := os.ReadFile(filename)
	if err != nil {
		log.Fatal("读取文件错误:", err)
	}
	kva := mapf(filename, string(content))
	buckets := make(map[int][]KeyValue) //  创建一个 map 来存储按照 reduce 编号分组的键值对
	for _, kv := range kva {
		r := ihash(kv.Key) % nReduce //  使用 ihash 函数计算出它应该属于哪个 reduce 编号 r
		buckets[r] = append(buckets[r], kv)
	}
	tempFiles := make([]*os.File, nReduce) // 创建一个临时文件指针切片 tempFiles
	ws := make([]*bufio.Writer, nReduce)
	encs := make([]*json.Encoder, nReduce) //  创建一个 json 编码器切片 encs
	for r := 0; r < nReduce; r++ {
		tempFile, err := os.CreateTemp("", "temp.*")
		//file, err := os.Create(filename)
		if err != nil {
			log.Fatal("创建文件错误:", err)
		}
		tempFiles[r] = tempFile
		w := bufio.NewWriter(tempFile)
		ws[r] = w
		enc := json.NewEncoder(w) //  创建一个 json 编码器 enc
		encs[r] = enc             //  将 enc 追加到 encs 中

	}
	for r, kvs := range buckets { //  遍历 buckets 中的每个键值对切片 kvs 和对应的 reduce 编号 r
		for _, kv := range kvs { //  遍历 kvs 中的每个键值对 kv
			err := encs[r].Encode(&kv) //  使用 encs[r] 将 kv 编码并写入对应的中间文件中

			if err != nil {
				log.Fatal("编码错误:", err)
			}
		}
		ws[r].Flush()
	}
	for r := 0; r < nReduce; r++ { // 遍历每个reduce编号r
		filename := fmt.Sprintf("mr-%d-%d", fileNum, r) //  中间文件名  “mr-fileNum-nReducer”

		tempFiles[r].Close()
		err = os.Rename(tempFiles[r].Name(), filename)
		if err != nil {
			log.Fatal("重命名文件错误:", err)
		}
	}
}

// 输出文件的数量等于 reduce 任务的总数，即nReduce。
// nFiles 是 txt 文件的数量
func doReduce(reducef func(string, []string) string, reduceNum int, nFiles int) {
	counts := make(map[string][]string) //  创建一个 map 来存储按照键分组的值列表
	for i := 0; i < nFiles; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reduceNum) //  中间文件名  “mr-fileNum-nReducer”
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal("打开文件错误:", err)
		}
		dec := json.NewDecoder(file) //  创建一个 json 解码器 dec
		for {                        //  遍历这个文件中的每个键值对 kv
			var kv KeyValue
			err := dec.Decode(&kv) //  使用 dec 将 kv 解码并读取出来
			if err != nil {
				if err == io.EOF { //  如果遇到文件结束符，就跳出循环
					break
				} else {
					log.Fatal("解码错误:", err)
				}
			}
			counts[kv.Key] = append(counts[kv.Key], kv.Value) //  将 kv.Value 追加到 counts[kv.Key] 中
		}
		defer file.Close() //  使用 defer 关键字，在函数结束时关闭这个文件
	}
	keys := make([]string, 0, len(counts)) //  创建一个切片 keys，用于存储 counts 中的所有键
	for k := range counts {                //  遍历 counts 中的每个键 k
		keys = append(keys, k) //  将 k 追加到 keys 中
	}
	sort.Strings(keys)
	filename := fmt.Sprintf("mr-out-%d", reduceNum) // 最终文件名 mr-out-nReducer

	tempFile, err := os.CreateTemp("", "temp.*")
	if err != nil {
		log.Fatal("创建文件错误:", err)
	}
	w := bufio.NewWriter(tempFile) //  创建一个缓冲写入器 w
	for _, k := range keys {       //  遍历 keys 中的每个键 k
		res := reducef(k, counts[k])
		fmt.Fprintf(w, "%v %v\n", k, res) //  将 k 和 res 写入 w 中，以空格分隔，并换行
	}
	w.Flush()
	tempFile.Close()
	err = os.Rename(tempFile.Name(), filename)
	if err != nil {
		log.Fatal("重命名错误：", err)
	}

}

func feedback(taskType string, taskNum int, startTime time.Time) {
	args := FeedbackArgs{}
	args.TaskType = taskType
	args.TaskNum = taskNum
	// 设置任务完成状态为true
	args.Done = true
	args.StartTime = startTime
	reply := FeedbackReply{}
	// 调用Master.Feedback方法，发送反馈信息，并接收响应结果
	err := call("Master.Feedback", &args, &reply, "1234")
	if err != nil || !reply.OK {
		log.Printf("Feedback %s task %d failed: %v\n", taskType, taskNum, err) // 打印反馈失败的信息和错误原因
	} else {
		duration := time.Now().Sub(args.StartTime)
		log.Printf("Feedback  %s task %d success.\tfinished in %v\n", taskType, taskNum, duration)
	}
}

// 通常返回true。
// 出错返回false。
func call(rpcname string, args interface{}, reply interface{}, addr string) error {
	// 使用 Unix 域套接字来拨号，使用 masterSock() 函数来获取地址
	client, err := rpc.Dial("unix", masterSock())
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer client.Close()

	err = client.Call(rpcname, args, reply)
	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}
