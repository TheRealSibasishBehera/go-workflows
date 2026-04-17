package sqlite

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/worker"
	"github.com/cschleiden/go-workflows/workflow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestStress_Notifier_ParallelWorkflows(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	b := NewInMemoryBackend(
		WithNotifierEnabled(),
		WithBackendOptions(
			backend.WithStickyTimeout(0),
		),
	)

	addActivity := func(ctx context.Context, a, b int) (int, error) {
		return a + b, nil
	}

	sequentialWf := func(ctx workflow.Context, start int) (int, error) {
		result := start
		for i := 0; i < 4; i++ {
			r, err := workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, addActivity, result, 1).Get(ctx)
			if err != nil {
				return 0, err
			}
			result = r
		}
		return result, nil
	}

	w := worker.New(b, &worker.Options{
		WorkflowWorkerOptions: worker.WorkflowWorkerOptions{
			WorkflowPollers:          2,
			MaxParallelWorkflowTasks: 2,
		},
		ActivityWorkerOptions: worker.ActivityWorkerOptions{
			ActivityPollers:          2,
			MaxParallelActivityTasks: 4,
		},
	})

	require.NoError(t, w.RegisterWorkflow(sequentialWf))
	require.NoError(t, w.RegisterActivity(addActivity))
	require.NoError(t, w.Start(ctx))

	t.Cleanup(func() {
		cancel()
		w.WaitForCompletion()
		b.Close()
	})

	c := client.New(b)

	const numWorkflows = 50
	var wg sync.WaitGroup
	wg.Add(numWorkflows)

	results := make([]int, numWorkflows)
	errs := make([]error, numWorkflows)

	for i := 0; i < numWorkflows; i++ {
		go func(idx int) {
			defer wg.Done()

			instance, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
				InstanceID: uuid.NewString(),
			}, sequentialWf, idx)
			if err != nil {
				errs[idx] = err
				return
			}

			r, err := client.GetWorkflowResult[int](ctx, c, instance, 30*time.Second)
			if err != nil {
				errs[idx] = err
				return
			}
			results[idx] = r
		}(i)
	}

	wg.Wait()

	for i := 0; i < numWorkflows; i++ {
		require.NoError(t, errs[i], "workflow %d failed", i)
		require.Equal(t, i+4, results[i], "workflow %d result mismatch", i)
	}
}

func TestStress_Notifier_BurstActivityCompletion(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	b := NewInMemoryBackend(
		WithNotifierEnabled(),
		WithBackendOptions(
			backend.WithStickyTimeout(0),
		),
	)

	activityFn := func(ctx context.Context, val int) (int, error) {
		return val * 2, nil
	}

	fanoutWf := func(ctx workflow.Context, count int) (int, error) {
		futures := make([]workflow.Future[int], count)
		for i := 0; i < count; i++ {
			futures[i] = workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, activityFn, i+1)
		}

		total := 0
		for _, f := range futures {
			r, err := f.Get(ctx)
			if err != nil {
				return 0, err
			}
			total += r
		}
		return total, nil
	}

	w := worker.New(b, nil)
	require.NoError(t, w.RegisterWorkflow(fanoutWf))
	require.NoError(t, w.RegisterActivity(activityFn))
	require.NoError(t, w.Start(ctx))

	t.Cleanup(func() {
		cancel()
		w.WaitForCompletion()
		b.Close()
	})

	c := client.New(b)

	instance, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: uuid.NewString(),
	}, fanoutWf, 20)
	require.NoError(t, err)

	result, err := client.GetWorkflowResult[int](ctx, c, instance, 30*time.Second)
	require.NoError(t, err)

	expected := 0
	for i := 1; i <= 20; i++ {
		expected += i * 2
	}
	require.Equal(t, expected, result)
}

func TestStress_Notifier_WorkflowChaining(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	b := NewInMemoryBackend(
		WithNotifierEnabled(),
		WithBackendOptions(
			backend.WithStickyTimeout(0),
		),
	)

	act := func(ctx context.Context, v int) (int, error) {
		return v + 10, nil
	}

	chainWfD := func(ctx workflow.Context, v int) (int, error) {
		r, err := workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, act, v).Get(ctx)
		if err != nil {
			return 0, err
		}
		return r, nil
	}

	chainWfC := func(ctx workflow.Context, v int) (int, error) {
		r, err := workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, act, v).Get(ctx)
		if err != nil {
			return 0, err
		}
		return workflow.CreateSubWorkflowInstance[int](ctx, workflow.DefaultSubWorkflowOptions, chainWfD, r).Get(ctx)
	}

	chainWfB := func(ctx workflow.Context, v int) (int, error) {
		r, err := workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, act, v).Get(ctx)
		if err != nil {
			return 0, err
		}
		return workflow.CreateSubWorkflowInstance[int](ctx, workflow.DefaultSubWorkflowOptions, chainWfC, r).Get(ctx)
	}

	chainWfA := func(ctx workflow.Context, v int) (int, error) {
		r, err := workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, act, v).Get(ctx)
		if err != nil {
			return 0, err
		}
		return workflow.CreateSubWorkflowInstance[int](ctx, workflow.DefaultSubWorkflowOptions, chainWfB, r).Get(ctx)
	}

	w := worker.New(b, nil)
	require.NoError(t, w.RegisterWorkflow(chainWfA))
	require.NoError(t, w.RegisterWorkflow(chainWfB))
	require.NoError(t, w.RegisterWorkflow(chainWfC))
	require.NoError(t, w.RegisterWorkflow(chainWfD))
	require.NoError(t, w.RegisterActivity(act))
	require.NoError(t, w.Start(ctx))

	t.Cleanup(func() {
		cancel()
		w.WaitForCompletion()
		b.Close()
	})

	c := client.New(b)

	const numChains = 10
	var wg sync.WaitGroup
	wg.Add(numChains)

	results := make([]int, numChains)
	errs := make([]error, numChains)

	for i := 0; i < numChains; i++ {
		go func(idx int) {
			defer wg.Done()

			instance, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
				InstanceID: uuid.NewString(),
			}, chainWfA, idx)
			if err != nil {
				errs[idx] = err
				return
			}

			r, err := client.GetWorkflowResult[int](ctx, c, instance, 30*time.Second)
			if err != nil {
				errs[idx] = err
				return
			}
			results[idx] = r
		}(i)
	}

	wg.Wait()

	for i := 0; i < numChains; i++ {
		require.NoError(t, errs[i], "chain %d failed", i)
		require.Equal(t, i+40, results[i], "chain %d result mismatch", i)
	}
}

func TestStress_Notifier_SignalWhilePolling(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	b := NewInMemoryBackend(
		WithNotifierEnabled(),
		WithBackendOptions(
			backend.WithStickyTimeout(0),
		),
	)

	act := func(ctx context.Context, v int) (int, error) {
		return v + 1, nil
	}

	signalWf := func(ctx workflow.Context, expectedSignals int) (int, error) {
		ch := workflow.NewSignalChannel[int](ctx, "test-signal")

		total := 0
		for i := 0; i < expectedSignals; i++ {
			v, ok := ch.Receive(ctx)
			if !ok {
				return 0, nil
			}
			r, err := workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, act, v).Get(ctx)
			if err != nil {
				return 0, err
			}
			total += r
		}
		return total, nil
	}

	w := worker.New(b, nil)
	require.NoError(t, w.RegisterWorkflow(signalWf))
	require.NoError(t, w.RegisterActivity(act))
	require.NoError(t, w.Start(ctx))

	t.Cleanup(func() {
		cancel()
		w.WaitForCompletion()
		b.Close()
	})

	c := client.New(b)

	instance, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
		InstanceID: uuid.NewString(),
	}, signalWf, 5)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(5)

	for i := 0; i < 5; i++ {
		go func(idx int) {
			defer wg.Done()
			time.Sleep(time.Duration(idx*50) * time.Millisecond)
			c.SignalWorkflow(ctx, instance.InstanceID, "test-signal", idx*10)
		}(i)
	}

	wg.Wait()

	result, err := client.GetWorkflowResult[int](ctx, c, instance, 30*time.Second)
	require.NoError(t, err)

	expected := 0
	for i := 0; i < 5; i++ {
		expected += i*10 + 1
	}
	require.Equal(t, expected, result)
}

func TestStress_Notifier_MixedWorkload(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	b := NewInMemoryBackend(
		WithNotifierEnabled(),
		WithBackendOptions(
			backend.WithStickyTimeout(0),
		),
	)

	act := func(ctx context.Context, v int) (int, error) {
		return v * 2, nil
	}

	simpleWf := func(ctx workflow.Context, v int) (int, error) {
		return workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, act, v).Get(ctx)
	}

	subWf := func(ctx workflow.Context, v int) (int, error) {
		return workflow.CreateSubWorkflowInstance[int](ctx, workflow.DefaultSubWorkflowOptions, simpleWf, v).Get(ctx)
	}

	timerWf := func(ctx workflow.Context, v int) (int, error) {
		workflow.Sleep(ctx, 5*time.Millisecond)
		return workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, act, v).Get(ctx)
	}

	mixedSignalWf := func(ctx workflow.Context, v int) (int, error) {
		ch := workflow.NewSignalChannel[int](ctx, "mixed-signal")
		sig, _ := ch.Receive(ctx)
		r, err := workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, act, v).Get(ctx)
		if err != nil {
			return 0, err
		}
		return r + sig, nil
	}

	w := worker.New(b, nil)
	require.NoError(t, w.RegisterWorkflow(simpleWf))
	require.NoError(t, w.RegisterWorkflow(subWf))
	require.NoError(t, w.RegisterWorkflow(timerWf))
	require.NoError(t, w.RegisterWorkflow(mixedSignalWf))
	require.NoError(t, w.RegisterActivity(act))
	require.NoError(t, w.Start(ctx))

	t.Cleanup(func() {
		cancel()
		w.WaitForCompletion()
		b.Close()
	})

	c := client.New(b)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var allErrs []error

	collectError := func(err error) {
		mu.Lock()
		allErrs = append(allErrs, err)
		mu.Unlock()
	}

	const count = 5

	wg.Add(count * 4)

	for i := 0; i < count; i++ {
		go func(idx int) {
			defer wg.Done()
			instance, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
				InstanceID: uuid.NewString(),
			}, simpleWf, idx)
			if err != nil {
				collectError(err)
				return
			}
			r, err := client.GetWorkflowResult[int](ctx, c, instance, 30*time.Second)
			if err != nil {
				collectError(err)
				return
			}
			if r != idx*2 {
				collectError(nil)
			}
		}(i)

		go func(idx int) {
			defer wg.Done()
			instance, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
				InstanceID: uuid.NewString(),
			}, subWf, idx)
			if err != nil {
				collectError(err)
				return
			}
			r, err := client.GetWorkflowResult[int](ctx, c, instance, 30*time.Second)
			if err != nil {
				collectError(err)
				return
			}
			if r != idx*2 {
				collectError(nil)
			}
		}(i)

		go func(idx int) {
			defer wg.Done()
			instance, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
				InstanceID: uuid.NewString(),
			}, timerWf, idx)
			if err != nil {
				collectError(err)
				return
			}
			r, err := client.GetWorkflowResult[int](ctx, c, instance, 30*time.Second)
			if err != nil {
				collectError(err)
				return
			}
			if r != idx*2 {
				collectError(nil)
			}
		}(i)

		go func(idx int) {
			defer wg.Done()
			instance, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
				InstanceID: uuid.NewString(),
			}, mixedSignalWf, idx)
			if err != nil {
				collectError(err)
				return
			}
			time.Sleep(time.Duration(50+idx*20) * time.Millisecond)
			if err := c.SignalWorkflow(ctx, instance.InstanceID, "mixed-signal", idx*100); err != nil {
				collectError(err)
				return
			}
			r, err := client.GetWorkflowResult[int](ctx, c, instance, 30*time.Second)
			if err != nil {
				collectError(err)
				return
			}
			if r != idx*2+idx*100 {
				collectError(nil)
			}
		}(i)
	}

	wg.Wait()

	for _, err := range allErrs {
		require.NoError(t, err)
	}
}

func TestStress_Notifier_CancelUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

	b := NewInMemoryBackend(
		WithNotifierEnabled(),
		WithBackendOptions(
			backend.WithStickyTimeout(0),
		),
	)

	act := func(ctx context.Context, v int) (int, error) {
		return v + 1, nil
	}

	finishableWf := func(ctx workflow.Context, v int) (int, error) {
		r, err := workflow.ExecuteActivity[int](ctx, workflow.DefaultActivityOptions, act, v).Get(ctx)
		if err != nil {
			return 0, err
		}
		return r, nil
	}

	blockingWf := func(ctx workflow.Context, v int) (int, error) {
		_, err := workflow.ScheduleTimer(ctx, time.Minute*10).Get(ctx)
		if err != nil && err != workflow.Canceled {
			return 0, err
		}
		return v, nil
	}

	w := worker.New(b, nil)
	require.NoError(t, w.RegisterWorkflow(finishableWf))
	require.NoError(t, w.RegisterWorkflow(blockingWf))
	require.NoError(t, w.RegisterActivity(act))
	require.NoError(t, w.Start(ctx))

	t.Cleanup(func() {
		cancel()
		w.WaitForCompletion()
		b.Close()
	})

	c := client.New(b)

	const totalWorkflows = 20
	const cancelCount = 10

	instances := make([]*workflow.Instance, totalWorkflows)

	for i := 0; i < cancelCount; i++ {
		instance, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
			InstanceID: uuid.NewString(),
		}, blockingWf, i)
		require.NoError(t, err)
		instances[i] = instance
	}

	for i := cancelCount; i < totalWorkflows; i++ {
		instance, err := c.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{
			InstanceID: uuid.NewString(),
		}, finishableWf, i)
		require.NoError(t, err)
		instances[i] = instance
	}

	for i := 0; i < cancelCount; i++ {
		require.NoError(t, c.CancelWorkflowInstance(ctx, instances[i]))
	}

	for i := cancelCount; i < totalWorkflows; i++ {
		r, err := client.GetWorkflowResult[int](ctx, c, instances[i], 30*time.Second)
		require.NoError(t, err, "finishable workflow %d should complete", i)
		require.Equal(t, i+1, r, "finishable workflow %d result mismatch", i)
	}

	for i := 0; i < cancelCount; i++ {
		r, err := client.GetWorkflowResult[int](ctx, c, instances[i], 30*time.Second)
		require.NoError(t, err, "cancelled workflow %d should complete gracefully", i)
		require.Equal(t, i, r, "cancelled workflow %d result mismatch", i)
	}
}
