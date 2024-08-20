package main

import (
	"fmt"
	"time"
)

func asyncTask(done chan bool) {
	time.Sleep(10 * time.Second)
	done <- true
}

func timeoutHandler() {
	fmt.Println("超时...")
}

func main() {
	done := make(chan bool)

	go asyncTask(done)

	timeout := time.After(2 * time.Second)

	select {
	case <-done:
		fmt.Println("任务完成")
	case <-timeout:
		timeoutHandler()
	}
}
