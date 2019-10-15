package main

import (
	"context"
	"fmt"
	"os/exec"
	"time"
)

type result struct {
	err    error
	output []byte
}

func main() {
	var (
		ctx        context.Context
		cancelFunc context.CancelFunc
		cmd        *exec.Cmd
		resultChan chan *result
		res        *result
	)

	resultChan = make(chan *result, 10000)
	ctx, cancelFunc = context.WithCancel(context.TODO())

	go func() {
		var (
			output []byte
			err    error
		)
		cmd = exec.CommandContext(ctx, "/bin/bash", "-c", "sleep 2;echo hello")

		output, err = cmd.CombinedOutput()

		resultChan <- &result{
			err:    err,
			output: output,
		}

	}()

	// time.Sleep(3 * time.Second)
	time.Sleep(3 * time.Second)

	cancelFunc()

	res = <-resultChan

	fmt.Println(res.err, string(res.output))

}
