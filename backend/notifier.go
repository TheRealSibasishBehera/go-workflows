package backend

type Notifier interface {
	WorkflowTaskReady() <-chan struct{}
	ActivityTaskReady() <-chan struct{}
}
