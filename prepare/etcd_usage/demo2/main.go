package main

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/etcd/mvcc/mvccpb"

	"go.etcd.io/etcd/clientv3"
)

func main() {
	var (
		config  clientv3.Config
		client  *clientv3.Client
		err     error
		kv      clientv3.KV
		delResp *clientv3.DeleteResponse
		// getResp *clientv3.GetResponse
		kvPair *mvccpb.KeyValue
	)

	config = clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"}, // 集群列表
		DialTimeout: 5 * time.Second,
	}

	if client, err = clientv3.New(config); err != nil {
		fmt.Println(err)
		return
	}

	kv = clientv3.NewKV(client)

	// =========WithPrevKV
	kv.Put(context.TODO(), "/cron/jobs/job1", "value_xixi")
	if delResp, err = kv.Delete(context.TODO(), "/cron/jobs/job1", clientv3.WithPrevKV()); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("succeed to deleted WithPrevKV")
	if len(delResp.PrevKvs) != 0 {
		for _, kvPair = range delResp.PrevKvs {
			fmt.Println("deleted: ", string(kvPair.Key), string(kvPair.Value))
		}
	}

	// =========WithPrefix
	kv.Put(context.TODO(), "/cron/jobs/job1", "value_xixi")
	kv.Put(context.TODO(), "/cron/jobs/job2", "value_xixi")
	kv.Put(context.TODO(), "/cron/jobs/job3", "value_xixi")
	if delResp, err = kv.Delete(context.TODO(), "/cron/jobs", clientv3.WithPrefix()); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("succeed to deleted WithPrefix")
	// if len(delResp.PrevKvs) != 0 {
	// 	for _, kvPair = range delResp.PrevKvs {
	// 		fmt.Println("deleted: ", string(kvPair.Key), string(kvPair.Value))
	// 	}
	// }

	// =========WithFromKey  WithLimit
	// TODO not working
	// kv.Put(context.TODO(), "/cron/jobs/job1", "value_xixi")
	// kv.Put(context.TODO(), "/cron/jobs/job2", "value_xixi")
	// kv.Put(context.TODO(), "/cron/jobs/job3", "value_xixi")
	// if delResp, err = kv.Delete(context.TODO(), "/cron/jobs/job1", clientv3.WithFromKey(), clientv3.WithLimit(1)); err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	// fmt.Println("succeed to deleted WithFromKey_WithLimit")
}
