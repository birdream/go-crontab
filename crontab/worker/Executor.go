package worker

import (
	"context"
	"go-crontab/crontab/common"
	"math/rand"
	"os/exec"
	"time"
)

type Executor struct{}

var G_executor *Executor

// InitExecutor 初始化执行器
func InitExecutor() (err error) {
	G_executor = &Executor{}
	return
}

// ExecuteJob ...
func (ex *Executor) ExecuteJob(info *common.JobExecuteInfo) {
	go func() {
		var (
			cmd     *exec.Cmd
			err     error
			output  []byte
			result  *common.JobExecuteResult
			jobLock *JobLock
		)

		result = &common.JobExecuteResult{
			ExecuteInfo: info,
			Output:      make([]byte, 0),
		}

		// 初始化分布式锁
		jobLock = G_jobMgr.CreateJobLock(info.Job.Name)

		result.StartTime = time.Now()

		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

		err = jobLock.TryLock()
		defer jobLock.Unlock()

		if err != nil {
			// 上锁失败
			result.Err = err
			result.EndTime = time.Now()
		} else {
			// execute shell cmd
			cmd = exec.CommandContext(context.TODO(), "/bin/bash", "-c", info.Job.Command)

			output, err = cmd.CombinedOutput()

			result.Output = output
			result.Err = err
			result.EndTime = time.Now()
		}

		// 任务执行完成后，把执行的结果返回给Scheduler，Scheduler会从executingTable中删除掉执行记录
		G_scheduler.PushJobResult(result)
	}()
}
