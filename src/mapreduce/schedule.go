package mapreduce

import (
	"fmt"
	"sync"
)

type ConcurrentMap struct {
	sync.Mutex
	m map[string]int
}

func (cm *ConcurrentMap) TestandSet(key string, old int, new int) bool {
	cm.Lock()
	defer cm.Unlock()
	if cm.m[key] == old {
		cm.m[key] = new
		return true
	}
	return false
}

func (cm *ConcurrentMap) Set(key string, val int) int {
	cm.Lock()
	defer cm.Unlock()
	res := cm.m[key]
	cm.m[key] = val
	return res
}

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)
	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	freeFlags := new(ConcurrentMap)
	freeFlags.m = make(map[string]int)

	for _, ele := range mr.workers {
		freeFlags.Lock()
		freeFlags.m[ele] = 0
		freeFlags.Unlock()
	}
	i := 0
	finishedTask := 0
	doneWork := make(chan string)
	failedTask := make(chan int)
L:
	for {
		select {
		case newWorker := <-mr.registerChannel:
			debug("NewWorker: ", newWorker)
			freeFlags.Set(newWorker, 0)
		case freeWorker := <-doneWork:
			finishedTask++
			debug("FreeWorker: %s,finished Task:%d\n", freeWorker, finishedTask)
			if finishedTask == ntasks {
				debug("All %v tasks are finished\n", ntasks)
				break L
			}
			freeFlags.Set(freeWorker, 0)
		case failed := <-failedTask:
			debug("Failed Task: %d, retry!\n", failed)
			go func() {
				startIndex := 0
				for {
					if startIndex < len(mr.workers) {
						if freeFlags.TestandSet(mr.workers[startIndex], 0, 1) {
							debug("Retry failed Task: %d, Worker: %s\n", failed, mr.workers[startIndex])
							mr.allocateWork(mr.workers[startIndex], failed, ntasks, phase, doneWork, failedTask)
							break
						}
						startIndex = (startIndex + 1) % len(mr.workers)
					}
				}
			}()
		default:
			l := len(mr.workers)
			for startIndex := 0; startIndex < l; startIndex++ {
				if freeFlags.TestandSet(mr.workers[startIndex], 0, 1) {
					mr.allocateWork(mr.workers[startIndex], i, ntasks, phase, doneWork, failedTask)
					i++
				}
			}
		}
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}

func (mr *Master) allocateWork(worker string, numTask int, totalNum int, phase jobPhase, doneWork chan string, failedTask chan int) {
	if numTask < totalNum {
		var args DoTaskArgs
		args.JobName = mr.jobName
		args.TaskNumber = numTask
		args.Phase = phase
		if phase == mapPhase {
			args.File = mr.files[numTask]
			args.NumOtherPhase = mr.nReduce
		} else {
			args.NumOtherPhase = len(mr.files)
		}
		go func() {
			var reply struct{}
			finished := call(worker, "Worker.DoTask", &args, &reply)
			debug("RPC call worker %s Worker.DoTask, TaskNum: %d, return %v\n", worker, numTask, finished)
			if finished == true {
				doneWork <- worker
			} else {
				failedTask <- numTask
			}
		}()
	}
}
