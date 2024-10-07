package nyxsmt

import (
	"container/heap"
	"context"
	"database/sql"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

// Task manager simple interface (task_manager_simple.go)
type TaskManagerSimple struct {
	providers       map[string]*ProviderData
	taskInQueue     map[string]bool
	taskInQueueLock sync.RWMutex
	isRunning       int32
	shutdownCh      chan struct{}
	wg              sync.WaitGroup
	logger          *zerolog.Logger
	getTimeout      func(string) time.Duration
}

type ProviderData struct {
	taskQueue     TaskQueuePrio
	taskQueueLock sync.Mutex
	servers       []string
	serverWorkers map[string]chan struct{}
}

func NewTaskManagerSimple(providers *[]IProvider, servers map[string][]string, logger *zerolog.Logger, getTimeout func(string) time.Duration) *TaskManagerSimple {
	tm := &TaskManagerSimple{
		providers:       make(map[string]*ProviderData),
		taskInQueue:     make(map[string]bool),
		taskInQueueLock: sync.RWMutex{},
		isRunning:       0,
		shutdownCh:      make(chan struct{}),
		logger:          logger,
		getTimeout:      getTimeout,
	}

	// Initialize providers
	for _, provider := range *providers {
		providerName := provider.Name()
		serverList, ok := servers[providerName]
		if !ok {
			serverList = []string{}
		}
		pd := &ProviderData{
			taskQueue:     TaskQueuePrio{},
			taskQueueLock: sync.Mutex{},
			servers:       serverList,
			serverWorkers: make(map[string]chan struct{}),
		}
		for _, server := range serverList {
			pd.serverWorkers[server] = make(chan struct{}, 1) // Buffered channel with capacity 1
		}
		tm.providers[providerName] = pd
	}

	return tm
}

func (tm *TaskManagerSimple) IsRunning() bool {
	return atomic.LoadInt32(&tm.isRunning) == 1
}

func (tm *TaskManagerSimple) setTaskInQueue(task ITask, inQueue bool) {
	tm.taskInQueueLock.Lock()
	defer tm.taskInQueueLock.Unlock()
	tm.taskInQueue[task.GetID()] = inQueue
}

func (tm *TaskManagerSimple) isTaskInQueue(task ITask) bool {
	tm.taskInQueueLock.RLock()
	defer tm.taskInQueueLock.RUnlock()
	return tm.taskInQueue[task.GetID()]
}

func (tm *TaskManagerSimple) delTaskInQueue(task ITask) {
	tm.taskInQueueLock.Lock()
	defer tm.taskInQueueLock.Unlock()
	delete(tm.taskInQueue, task.GetID())
}

func (tm *TaskManagerSimple) AddTasks(tasks []ITask) (count int, err error) {
	for _, task := range tasks {
		if tm.AddTask(task) {
			count++
		}
	}
	return count, err
}

func (tm *TaskManagerSimple) AddTask(task ITask) bool {
	if !tm.IsRunning() {
		return false
	}

	if tm.isTaskInQueue(task) {
		return false
	}
	tm.setTaskInQueue(task, true)

	providerName := task.GetProvider().Name()
	pd, ok := tm.providers[providerName]
	if !ok {
		err := fmt.Errorf("provider '%s' not found", providerName)
		tm.logger.Error().Err(err).Msgf("[tms|%s|%s] error", providerName, task.GetID())
		task.MarkAsFailed(0, err)
		task.OnComplete()
		tm.delTaskInQueue(task)
		return false
	}

	pd.taskQueueLock.Lock()
	defer pd.taskQueueLock.Unlock()
	heap.Push(&pd.taskQueue, &TaskWithPriority{
		task:     task,
		priority: task.GetPriority(),
	})

	return true
}

func (tm *TaskManagerSimple) Start() {
	if tm.IsRunning() {
		return
	}

	atomic.StoreInt32(&tm.isRunning, 1)

	for providerName := range tm.providers {
		tm.wg.Add(1)
		go tm.providerDispatcher(providerName)
	}
}

func (tm *TaskManagerSimple) providerDispatcher(providerName string) {
	defer tm.wg.Done()
	pd := tm.providers[providerName]

	for {
		select {
		case <-tm.shutdownCh:
			return
		default:
			// Lock the task queue
			pd.taskQueueLock.Lock()
			if pd.taskQueue.Len() == 0 {
				pd.taskQueueLock.Unlock()
				time.Sleep(100 * time.Millisecond)
				continue
			}
			// Get the next task
			taskWithPriority := heap.Pop(&pd.taskQueue).(*TaskWithPriority)
			pd.taskQueueLock.Unlock()
			task := taskWithPriority.task

			// Find an available server
			serverFound := false
		loop:
			for _, server := range pd.servers {
				serverChan := pd.serverWorkers[server]
				select {
				case serverChan <- struct{}{}:
					// We got the server, process the task
					serverFound = true
					tm.wg.Add(1)
					go tm.processTask(task, providerName, server)
					// Break out of the loop
				default:
					// Server is busy
				}
				if serverFound {
					break loop
				}
			}
			if !serverFound {
				go func() {
					time.Sleep(1 * time.Second)
					tm.logger.Warn().Msgf("[tms|%s|%s] No available servers, requeuing task", providerName, task.GetID())
					pd.taskQueueLock.Lock()
					heap.Push(&pd.taskQueue, taskWithPriority)
					pd.taskQueueLock.Unlock()
				}()
			}
		}
	}
}

func (tm *TaskManagerSimple) processTask(task ITask, providerName, server string) {

	started := time.Now()

	defer tm.wg.Done()
	// First defer to release the server semaphore
	defer func() {
		pd := tm.providers[providerName]
		serverChan := pd.serverWorkers[server]
		<-serverChan
	}()

	// Second defer to handle panics
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("panic occurred: %v\n%s", r, string(debug.Stack()))
			tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] panic", providerName, task.GetID(), server)
			task.MarkAsFailed(time.Since(started).Milliseconds(), err)
			task.OnComplete()
			tm.delTaskInQueue(task)
		}
	}()
	err, totalTime := tm.HandleWithTimeout(providerName, task, server, tm.HandleTask)
	if err != nil {
		retries := task.GetRetries()
		maxRetries := task.GetMaxRetries()
		if retries >= maxRetries || err == sql.ErrNoRows {
			tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] max retries reached", providerName, task.GetID(), server)
			task.MarkAsFailed(totalTime, err)
			task.OnComplete()
			tm.delTaskInQueue(task)
		} else {
			tm.logger.Debug().Err(err).Msgf("[tms|%s|%s|%s] retrying (%d/%d)", providerName, task.GetID(), server, retries+1, maxRetries)
			task.UpdateRetries(retries + 1)
			tm.delTaskInQueue(task)
			tm.AddTask(task)
		}
	} else {
		task.MarkAsSuccess(totalTime)
		task.OnComplete()
		tm.delTaskInQueue(task)
	}
}

func (tm *TaskManagerSimple) HandleTask(task ITask, server string) error {
	provider := task.GetProvider()
	err := provider.Handle(task, server)
	return err
}

func (tm *TaskManagerSimple) Shutdown() {
	if !tm.IsRunning() {
		tm.logger.Debug().Msg("[tms] Task manager shutdown [ALREADY STOPPED]")
		return
	}
	close(tm.shutdownCh)
	tm.wg.Wait()
	atomic.StoreInt32(&tm.isRunning, 0)
	tm.logger.Debug().Msg("[tms] Task manager shutdown [FINISHED]")
}

// func GetTimeoutByProvider(provider string) time.Duration {

func (tm *TaskManagerSimple) HandleWithTimeout(pn string, task ITask, server string, handler func(ITask, string) error) (error, int64) {
	var err error
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic occurred: %v", r)
			tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] panic in task", pn, task.GetID(), server)
		}
	}()

	maxTimeout := tm.getTimeout(pn)
	ctx, cancel := context.WithTimeout(context.Background(), maxTimeout)
	defer cancel()

	done := make(chan error, 1)
	startTime := time.Now()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("panic occurred in handler: %v\n%s", r, string(debug.Stack()))
				tm.logger.Error().Err(err).Msgf("[tms|%s|%s|%s] panic in handler", pn, task.GetID(), server)
				done <- err
			}
		}()

		tm.logger.Debug().Msgf("[tms|%s|%s] Task STARTED on server %s", pn, task.GetID(), server)
		done <- handler(task, server)
	}()

	select {
	case <-ctx.Done():
		err = fmt.Errorf("[tms|%s|%s] Task timed out on server %s", pn, task.GetID(), server)
		tm.logger.Error().Err(err).Msgf("[%s|%s] Task FAILED-TIMEOUT on server %s, took %s", pn, task.GetID(), server, time.Since(startTime))
	case err = <-done:
		if err == nil {
			tm.logger.Debug().Msgf("[tms|%s|%s] Task COMPLETED on server %s, took %s", pn, task.GetID(), server, time.Since(startTime))
		} else {
			tm.logger.Error().Err(err).Msgf("[tms|%s|%s] Task FAILED on server %s, took %s", pn, task.GetID(), server, time.Since(startTime))
		}
	}

	return err, time.Since(startTime).Milliseconds()
}

var (
	TaskQueueManagerInstance *TaskManagerSimple
	taskManagerMutex         sync.Mutex
	taskManagerCond          = sync.NewCond(&taskManagerMutex)
	addMaxRetries            = 3
)

// AddTask is the function to be called to add tasks externally
func AddTask(task ITask, logger *zerolog.Logger) {
	tries := 0
	for {
		if tries >= addMaxRetries {
			logger.Error().Msg("[tms|add-task] Task not added, max retries reached")
			return
		}
		taskManagerMutex.Lock()
		for TaskQueueManagerInstance == nil || !TaskQueueManagerInstance.IsRunning() {
			taskManagerCond.Wait()
		}
		tmInstance := TaskQueueManagerInstance
		taskManagerMutex.Unlock()

		// Try to add the task
		if added := tmInstance.AddTask(task); !added {
			logger.Debug().Msg("[tms|add-task] Task not added, retrying")
			time.Sleep(250 * time.Millisecond)
			tries++
			continue
		}
		break
	}
}

// Initialize the TaskManager
func InitTaskQueueManager(logger *zerolog.Logger, providers *[]IProvider, tasks []ITask, servers map[string][]string, getTimeout func(string) time.Duration) {
	taskManagerMutex.Lock()
	defer taskManagerMutex.Unlock()
	logger.Info().Msg("[tms] Task manager initialization")

	TaskQueueManagerInstance = NewTaskManagerSimple(providers, servers, logger, getTimeout)
	TaskQueueManagerInstance.Start()
	logger.Info().Msg("[tms] Task manager started")

	// Signal that the TaskManager is ready
	taskManagerCond.Broadcast()

	// Add uncompleted tasks
	RequeueTaskIfNeeded(logger, tasks)
}
func RequeueTaskIfNeeded(logger *zerolog.Logger, tasks []ITask) {
	// Get all uncompleted tasks
	count, _ := TaskQueueManagerInstance.AddTasks(tasks)
	logger.Info().Msgf("[tms] Requeued %d. (%d tasks in queue)", count, len(tasks))
}
