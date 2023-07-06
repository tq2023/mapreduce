package main

import (
	"fmt"
	"io/ioutil"
	"mapreduce/src/mr"
	"strings"
	"time"
)
import "os"

// 接受一些命令行参数，表示输入文件的列表。
// 调用 MakeMaster 函数，创建一个 Master 结构体，
// 并传入输入文件列表和 Reduce 任务数量
func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster filespath\n")
		os.Exit(1)
	}
	filename := GetFileName(os.Args[1])

	m := mr.MakeMaster(filename, 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}

func GetFileName(path string) []string {
	var filename []string
	// 读取文件夹下的所有文件和子文件夹
	files, err := ioutil.ReadDir(path)
	if err != nil {
		fmt.Println("Error reading folder:", err)
		return filename
	}

	// 遍历所有文件和子文件夹，筛选出后缀为 .txt 的文件
	for _, file := range files {
		name := file.Name()
		if strings.HasSuffix(name, ".txt") {

			filename = append(filename, path+"/"+name)
		}
	}
	return filename
}
