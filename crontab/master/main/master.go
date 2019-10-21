package main

import (
	"flag"
	"fmt"
	"go-crontab/crontab/master"
	"runtime"
)

var (
	confFile string
)

// decode cmd args
func initArgs() {
	// master -config ./master.json
	flag.StringVar(&confFile, "config", "./master.json", "master configuration file")
	flag.Parse()
}

func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	var (
		err error
	)

	initArgs()

	initEnv()

	if err = master.InitConfig(confFile); err != nil {
		goto ERR
	}

	if err = master.InitApiServer(); err != nil {
		goto ERR
	}

	return

ERR:
	fmt.Println(err)
}
