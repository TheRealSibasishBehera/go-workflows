//go:build !short

package sqlite

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/worker"
	"github.com/cschleiden/go-workflows/workflow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func add(ctx context.Context, a, b int) (int, error) {
	return a + b, nil
}

func multiply(ctx context.Context, a, b int) (int, error) {
	return a * b, nil
}

func increment(ctx context.Context, a int) (int, error) {
	return a + 1, nil
}

func notifierSimpleWorkflow(ctx workflow.Context, start int) (int, error) {
	r1, err := workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, add, start, 10).Get(ctx)
	if err != nil {
		return 0, err
	}

	r2, err := workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, multiply, r1, 2).Get(ctx)
	if err != nil {
		return 0, err
	}

	r3, err := workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, increment, r2).Get(ctx)
	if err != nil {
		return 0, err
	}

	r4, err := workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, add, r3, 100).Get(ctx)
	if err != nil {
		return 0, err
	}

	return r4, nil
}

func notifierMultiStepWorkflow(ctx workflow.Context, start int) (int, error) {
	val := start
	for i := 0; i < 6; i++ {
		r, err := workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, increment, val).Get(ctx)
		if err != nil {
			return 0, err
		}
		val = r
	}
	return val, nil
}

func notifierSubWorkflow(ctx workflow.Context, x int) (int, error) {
	return x * 3, nil
}

func notifierParentWorkflow(ctx workflow.Context, x int) (int, error) {
	r1, err := workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, increment, x).Get(ctx)
	if err != nil {
		return 0, err
	}

	r2, err := workflow.CreateSubWorkflowInstance[int](ctx, workflow.DefaultSubWorkflowOptions, notifierSubWorkflow, r1).Get(ctx)
	if err != nil {
		return 0, err
	}

	r3, err := workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, add, r2, 5).Get(ctx)
	if err != nil {
		return 0, err
	}

	return r3, nil
}

func notifierSignalWorkflow(ctx workflow.Context, input int) (int, error) {
	ch := workflow.NewSignalChannel[int](ctx, "test-signal")
	received, _ := ch.Receive(ctx)

	r, err := workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, add, input, received).Get(ctx)
	if err != nil {
		return 0, err
	}

	return r, nil
}

func notifierTimerWorkflow(ctx workflow.Context, input int) (int, error) {
	r1, err := workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, increment, input).Get(ctx)
	if err != nil {
		return 0, err
	}

	_, err = workflow.ScheduleTimer(ctx, 50*time.Millisecond).Get(ctx)
	if err != nil {
		return 0, err
	}

	r2, err := workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, add, r1, 10).Get(ctx)
	if err != nil {
		return 0, err
	}

	return r2, nil
}

func setupNotifierTest(t *testing.T, opts ...option) (*sqliteBackend, *worker.Worker, *client.Client, context.CancelFunc) {
	t.Helper()

	allOpts := []option{
		WithBackendOptions(backend.WithStickyTimeout(0)),
	}
	allOpts = append(allOpts, opts...)

	b := NewInMemoryBackend(allOpts...)

	ctx, cancel := context.WithCancel(context.Background())

	w := worker.New(b, nil)

	w.RegisterWorkflow(notifierSimpleWorkflow)
	w.RegisterWorkflow(notifierMultiStepWorkflow)
	w.RegisterWorkflow(notifierParentWorkflow)
	w.RegisterWorkflow(notifierSubWorkflow)
	w.RegisterWorkflow(notifierSignalWorkflow)
	w.RegisterWorkflow(notifierTimerWorkflow)
	w.RegisterActivity(add)
	w.RegisterActivity(multiply)
	w.RegisterActivity(increment)

	require.NoError(t, w.Start(ctx))

	c := client.New(b)

	t.Cleanup(func() {
		cancel()
		if err := w.WaitForCompletion(); err != nil {
			t.Logf("worker cleanup error: %v", err)
		}
		if err := b.Close(); err != nil {
			t.Logf("backend close error: %v", err)
		}
	})

	return b, w, c, cancel
}

func runAndAwaitWorkflow[T any](t *testing.T, ctx context.Context, c *client.Client, wf interface{}, args ...interface{}) (T, error) {
	t.Helper()

	instance, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: uuid.NewString(),
	}, wf, args...)
	require.NoError(t, err)

	return client.GetWorkflowResult[T](ctx, c, instance, 30*time.Second)
}

func TestNotifierIntegration_SimpleWorkflow(t *testing.T) {
	_, _, c, _ := setupNotifierTest(t, WithNotifierEnabled())

	result, err := runAndAwaitWorkflow[int](t, context.Background(), c, notifierSimpleWorkflow, 5)
	require.NoError(t, err)
	require.Equal(t, ((5+10)*2+1)+100, result)
}

func TestNotifierIntegration_MultiStepWorkflowLatency(t *testing.T) {

	bNotifier := NewInMemoryBackend(
		WithNotifierEnabled(),
		WithBackendOptions(backend.WithStickyTimeout(0)),
	)
	defer bNotifier.Close()

	bPolling := NewInMemoryBackend(
		WithBackendOptions(
			backend.WithStickyTimeout(0),
		),
	)
	defer bPolling.Close()

	runWithBackend := func(b *sqliteBackend) time.Duration {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := worker.New(b, &worker.Options{
			WorkflowWorkerOptions: worker.WorkflowWorkerOptions{
				WorkflowPollers:         1,
				WorkflowPollingInterval: 200 * time.Millisecond,
			},
			ActivityWorkerOptions: worker.ActivityWorkerOptions{
				ActivityPollers:         1,
				ActivityPollingInterval: 200 * time.Millisecond,
			},
		})

		w.RegisterWorkflow(notifierMultiStepWorkflow)
		w.RegisterActivity(increment)
		require.NoError(t, w.Start(ctx))
		defer func() {
			cancel()
			_ = w.WaitForCompletion()
		}()

		c := client.New(b)

		start := time.Now()
		_, err := runAndAwaitWorkflow[int](t, ctx, c, notifierMultiStepWorkflow, 0)
		elapsed := time.Since(start)
		require.NoError(t, err)
		return elapsed
	}

	notifierDuration := runWithBackend(bNotifier)
	pollingDuration := runWithBackend(bPolling)

	t.Logf("notifier: %v, polling: %v", notifierDuration, pollingDuration)
	require.Less(t, notifierDuration, pollingDuration, "notifier-enabled workflow should complete faster than polling-only workflow")
}

func TestNotifierIntegration_SubWorkflow(t *testing.T) {
	_, _, c, _ := setupNotifierTest(t, WithNotifierEnabled())

	result, err := runAndAwaitWorkflow[int](t, context.Background(), c, notifierParentWorkflow, 4)
	require.NoError(t, err)
	require.Equal(t, (4+1)*3+5, result)
}

func TestNotifierIntegration_SignalWorkflow(t *testing.T) {
	_, _, c, _ := setupNotifierTest(t, WithNotifierEnabled())

	ctx := context.Background()

	instance, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: uuid.NewString(),
	}, notifierSignalWorkflow, 10)
	require.NoError(t, err)

	require.NoError(t, c.SignalWorkflow(ctx, instance.InstanceID, "test-signal", 25))

	result, err := client.GetWorkflowResult[int](ctx, c, instance, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, 35, result)
}

func TestNotifierIntegration_MultipleConcurrentWorkflows(t *testing.T) {
	_, _, c, _ := setupNotifierTest(t, WithNotifierEnabled())

	ctx := context.Background()

	numWorkflows := 15
	var wg sync.WaitGroup
	var errors atomic.Int32

	for i := 0; i < numWorkflows; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			instance, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
				InstanceID: uuid.NewString(),
			}, notifierSimpleWorkflow, idx)
			if err != nil {
				t.Logf("create workflow %d: %v", idx, err)
				errors.Add(1)
				return
			}

			result, err := client.GetWorkflowResult[int](ctx, c, instance, 30*time.Second)
			if err != nil {
				t.Logf("workflow %d result error: %v", idx, err)
				errors.Add(1)
				return
			}

			expected := ((idx + 10) * 2) + 1 + 100
			if result != expected {
				t.Logf("workflow %d: expected %d, got %d", idx, expected, result)
				errors.Add(1)
			}
		}(i)
	}

	wg.Wait()
	require.Equal(t, int32(0), errors.Load(), "all workflows should complete successfully")
}

func TestNotifierIntegration_WorkflowWithTimer(t *testing.T) {
	_, _, c, _ := setupNotifierTest(t, WithNotifierEnabled())

	result, err := runAndAwaitWorkflow[int](t, context.Background(), c, notifierTimerWorkflow, 5)
	require.NoError(t, err)
	require.Equal(t, (5+1)+10, result)
}
