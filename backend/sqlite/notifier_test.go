package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/backend/history"
	"github.com/cschleiden/go-workflows/core"
	"github.com/cschleiden/go-workflows/workflow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertChannelReceives(t *testing.T, ch <-chan struct{}, within time.Duration, msgAndArgs ...interface{}) {
	t.Helper()
	select {
	case <-ch:
		return
	case <-time.After(within):
		t.Fatalf("expected channel to receive within %v: %v", within, msgAndArgs)
	}
}

func assertChannelNotReceive(t *testing.T, ch <-chan struct{}, within time.Duration, msgAndArgs ...interface{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("expected channel NOT to receive, but it did: %v", msgAndArgs)
	case <-time.After(within):
		return
	}
}

func drainChannel(ch <-chan struct{}) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func newEnabledBackend(t *testing.T) *sqliteBackend {
	t.Helper()
	b := NewInMemoryBackend(
		WithNotifierEnabled(),
		WithBackendOptions(backend.WithStickyTimeout(0)),
	)
	t.Cleanup(func() { b.Close() })
	return b
}

func newDisabledBackend(t *testing.T) *sqliteBackend {
	t.Helper()
	b := NewInMemoryBackend(
		WithBackendOptions(backend.WithStickyTimeout(0)),
	)
	t.Cleanup(func() { b.Close() })
	return b
}

func createTestInstance(t *testing.T, ctx context.Context, b *sqliteBackend) *workflow.Instance {
	t.Helper()
	wfi := core.NewWorkflowInstance(uuid.NewString(), uuid.NewString())
	err := b.CreateWorkflowInstance(ctx, wfi, history.NewHistoryEvent(
		1, time.Now(), history.EventType_WorkflowExecutionStarted,
		&history.ExecutionStartedAttributes{Queue: workflow.QueueDefault},
	))
	require.NoError(t, err)
	return wfi
}

func getAndCompleteWorkflowTask(t *testing.T, ctx context.Context, b *sqliteBackend, wfi *workflow.Instance, activityEvents []*history.Event) {
	t.Helper()
	queues := []workflow.Queue{workflow.QueueDefault, core.QueueSystem}
	require.NoError(t, b.PrepareWorkflowQueues(ctx, queues))

	task, err := b.GetWorkflowTask(ctx, queues)
	require.NoError(t, err)
	require.NotNil(t, task)

	events := append([]*history.Event{}, task.NewEvents...)
	events = append(events, activityEvents...)

	sequenceID := int64(1)
	for i := range events {
		sequenceID++
		events[i].SequenceID = sequenceID
	}

	err = b.CompleteWorkflowTask(ctx, task, core.WorkflowInstanceStateActive, events, activityEvents, nil, nil)
	require.NoError(t, err)
}

func TestNotifier_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	t.Run("EnabledBackendImplementsNotifier", func(t *testing.T) {
		t.Parallel()
		b := newEnabledBackend(t)
		var _ backend.Notifier = b
		n, ok := interface{}(b).(backend.Notifier)
		require.True(t, ok)
		require.NotNil(t, n.WorkflowTaskReady())
		require.NotNil(t, n.ActivityTaskReady())
	})

	t.Run("DisabledBackendReturnsNilChannels", func(t *testing.T) {
		t.Parallel()
		b := newDisabledBackend(t)
		var _ backend.Notifier = b
		n, ok := interface{}(b).(backend.Notifier)
		require.True(t, ok)
		require.Nil(t, n.WorkflowTaskReady())
		require.Nil(t, n.ActivityTaskReady())
	})
}

func TestNotifier_ChannelsFireOnCreateWorkflow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newEnabledBackend(t)
	wfCh := b.WorkflowTaskReady()
	actCh := b.ActivityTaskReady()

	drainChannel(wfCh)
	drainChannel(actCh)

	wfi := core.NewWorkflowInstance(uuid.NewString(), uuid.NewString())
	err := b.CreateWorkflowInstance(ctx, wfi, history.NewHistoryEvent(
		1, time.Now(), history.EventType_WorkflowExecutionStarted,
		&history.ExecutionStartedAttributes{Queue: workflow.QueueDefault},
	))
	require.NoError(t, err)

	assertChannelReceives(t, wfCh, time.Second, "WorkflowTaskReady should fire after CreateWorkflowInstance")
	assertChannelNotReceive(t, actCh, 50*time.Millisecond, "ActivityTaskReady should not fire on CreateWorkflowInstance")
}

func TestNotifier_ChannelsFireOnCompleteActivityTask(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newEnabledBackend(t)
	wfCh := b.WorkflowTaskReady()
	actCh := b.ActivityTaskReady()

	activityScheduledEvent := history.NewPendingEvent(
		time.Now(), history.EventType_ActivityScheduled,
		&history.ActivityScheduledAttributes{Queue: workflow.QueueDefault},
		history.ScheduleEventID(1),
	)

	wfi := createTestInstance(t, ctx, b)

	drainChannel(wfCh)
	drainChannel(actCh)

	getAndCompleteWorkflowTask(t, ctx, b, wfi, []*history.Event{activityScheduledEvent})

	assertChannelReceives(t, actCh, time.Second, "ActivityTaskReady should fire after CompleteWorkflowTask with activity events")

	drainChannel(wfCh)

	require.NoError(t, b.PrepareActivityQueues(ctx, []workflow.Queue{workflow.QueueDefault}))
	actTask, err := b.GetActivityTask(ctx, []workflow.Queue{workflow.QueueDefault})
	require.NoError(t, err)
	require.NotNil(t, actTask)

	resultEvent := history.NewHistoryEvent(1, time.Now(), history.EventType_ActivityCompleted, &history.ActivityCompletedAttributes{})
	err = b.CompleteActivityTask(ctx, actTask, resultEvent)
	require.NoError(t, err)

	assertChannelReceives(t, wfCh, time.Second, "WorkflowTaskReady should fire after CompleteActivityTask")
}

func TestNotifier_ChannelsFireOnCompleteWorkflowTask(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newEnabledBackend(t)
	wfCh := b.WorkflowTaskReady()
	actCh := b.ActivityTaskReady()

	activityScheduledEvent := history.NewPendingEvent(
		time.Now(), history.EventType_ActivityScheduled,
		&history.ActivityScheduledAttributes{Queue: workflow.QueueDefault},
		history.ScheduleEventID(1),
	)

	wfi := createTestInstance(t, ctx, b)

	drainChannel(wfCh)
	drainChannel(actCh)

	getAndCompleteWorkflowTask(t, ctx, b, wfi, []*history.Event{activityScheduledEvent})

	assertChannelReceives(t, actCh, time.Second, "ActivityTaskReady should fire when activities are scheduled")
}

func TestNotifier_SignalCoalescing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newEnabledBackend(t)
	wfCh := b.WorkflowTaskReady()

	drainChannel(wfCh)

	for i := 0; i < 10; i++ {
		wfi := core.NewWorkflowInstance(uuid.NewString(), uuid.NewString())
		err := b.CreateWorkflowInstance(ctx, wfi, history.NewHistoryEvent(
			1, time.Now(), history.EventType_WorkflowExecutionStarted,
			&history.ExecutionStartedAttributes{Queue: workflow.QueueDefault},
		))
		require.NoError(t, err)
	}

	assertChannelReceives(t, wfCh, time.Second, "channel should receive at least one signal")

	drainChannel(wfCh)

	select {
	case <-wfCh:
		t.Fatal("channel should have at most one signal after coalescing")
	default:
	}
}

func TestNotifier_NilChannelsWhenDisabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newDisabledBackend(t)
	wfCh := b.WorkflowTaskReady()
	actCh := b.ActivityTaskReady()

	assert.Nil(t, wfCh)
	assert.Nil(t, actCh)

	wfi := core.NewWorkflowInstance(uuid.NewString(), uuid.NewString())
	err := b.CreateWorkflowInstance(ctx, wfi, history.NewHistoryEvent(
		1, time.Now(), history.EventType_WorkflowExecutionStarted,
		&history.ExecutionStartedAttributes{Queue: workflow.QueueDefault},
	))
	require.NoError(t, err)

	queues := []workflow.Queue{workflow.QueueDefault, core.QueueSystem}
	require.NoError(t, b.PrepareWorkflowQueues(ctx, queues))
	task, err := b.GetWorkflowTask(ctx, queues)
	require.NoError(t, err)
	require.NotNil(t, task)

	err = b.CompleteWorkflowTask(ctx, task, core.WorkflowInstanceStateActive, task.NewEvents, nil, nil, nil)
	require.NoError(t, err)
}

func TestNotifier_SignalWorkflow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newEnabledBackend(t)
	wfCh := b.WorkflowTaskReady()

	wfi := createTestInstance(t, ctx, b)

	queues := []workflow.Queue{workflow.QueueDefault, core.QueueSystem}
	require.NoError(t, b.PrepareWorkflowQueues(ctx, queues))
	task, err := b.GetWorkflowTask(ctx, queues)
	require.NoError(t, err)
	require.NotNil(t, task)
	err = b.CompleteWorkflowTask(ctx, task, core.WorkflowInstanceStateActive, task.NewEvents, nil, nil, nil)
	require.NoError(t, err)

	drainChannel(wfCh)

	signalEvent := history.NewPendingEvent(
		time.Now(), history.EventType_SignalReceived,
		&history.SignalReceivedAttributes{Name: "test-signal"},
	)
	err = b.SignalWorkflow(ctx, wfi.InstanceID, signalEvent)
	require.NoError(t, err)

	assertChannelReceives(t, wfCh, time.Second, "WorkflowTaskReady should fire after SignalWorkflow")
}

func TestNotifier_CancelWorkflow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newEnabledBackend(t)
	wfCh := b.WorkflowTaskReady()

	wfi := createTestInstance(t, ctx, b)

	queues := []workflow.Queue{workflow.QueueDefault, core.QueueSystem}
	require.NoError(t, b.PrepareWorkflowQueues(ctx, queues))
	task, err := b.GetWorkflowTask(ctx, queues)
	require.NoError(t, err)
	require.NotNil(t, task)
	err = b.CompleteWorkflowTask(ctx, task, core.WorkflowInstanceStateActive, task.NewEvents, nil, nil, nil)
	require.NoError(t, err)

	drainChannel(wfCh)

	cancelEvent := history.NewWorkflowCancellationEvent(time.Now())
	err = b.CancelWorkflowInstance(ctx, wfi, cancelEvent)
	require.NoError(t, err)

	assertChannelReceives(t, wfCh, time.Second, "WorkflowTaskReady should fire after CancelWorkflowInstance")
}
