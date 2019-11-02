package main

import (
	"flag"
	"fmt"
	"go-crontab/crontab/worker"
	"runtime"
	"time"
)

var (
	confFile string // 配置文件路径
)

// 解析命令行参数
func initArgs() {
	// worker -config ./worker.json
	// worker -h
	flag.StringVar(&confFile, "config", "./worker.json", "worker.json")
	flag.Parse()
}

// 初始化线程数量
func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	var (
		err error
	)

	// 初始化命令行参数
	initArgs()

	// 初始化线程
	initEnv()

	if err = worker.InitConfig(confFile); err != nil {
		goto ERR
	}

	// 启动执行器
	if err = worker.InitExecutor(); err != nil {
		fmt.Println("InitExecutor err: ", err)
		goto ERR
	}

	// 启动调度器
	if err = worker.InitScheduler(); err != nil {
		fmt.Println("InitScheduler err: ", err)
		goto ERR
	}

	if err = worker.InitJobMgr(); err != nil {
		fmt.Println("InitJobMgr err: ", err)
		goto ERR
	}

	// 正常退出
	for {
		time.Sleep(1 * time.Second)
	}

ERR:
	fmt.Println(err)
}
