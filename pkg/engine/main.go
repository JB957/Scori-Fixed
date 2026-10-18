package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/scorify/scorify/pkg/cache"
	"github.com/scorify/scorify/pkg/config"
	"github.com/scorify/scorify/pkg/ent"
	"github.com/scorify/scorify/pkg/ent/checkconfig"
	"github.com/scorify/scorify/pkg/ent/minion"
	"github.com/scorify/scorify/pkg/ent/round"
	"github.com/scorify/scorify/pkg/ent/status"
	"github.com/scorify/scorify/pkg/graph/model"
	"github.com/scorify/scorify/pkg/helpers"
	"github.com/scorify/scorify/pkg/static"
	"github.com/scorify/scorify/pkg/structs"
	"github.com/sirupsen/logrus"
)

type state int

const (
	EnginePaused state = iota
	EngineWaiting
	EngineRunning
)

type Client struct {
	lock                 *sync.Mutex
	state                atomic.Int32
	nextRoundAt          atomic.Int64
	loopCancel           context.CancelFunc
	loopVersion          uint64
	ctx                  context.Context
	ent                  *ent.Client
	redis                *redis.Client
	taskRequestChan      chan<- *structs.TaskRequest
	taskResponseChan     <-chan *structs.TaskResponse
	workerStatusChan     chan<- *structs.WorkerStatus
	kothTaskRequestChan  chan<- *structs.KothTaskRequestBundle
	kothTaskResponseChan <-chan *structs.KothTaskResponse
}

func NewEngine(
	ctx context.Context,
	entClient *ent.Client,
	redis *redis.Client,
	taskRequestChan chan<- *structs.TaskRequest,
	taskResponseChan <-chan *structs.TaskResponse,
	workerStatusChan chan<- *structs.WorkerStatus,
	kothTaskRequestChan chan<- *structs.KothTaskRequestBundle,
	kothTaskResponseChan <-chan *structs.KothTaskResponse,
) *Client {
	return &Client{
		lock:                 &sync.Mutex{},
		ctx:                  ctx,
		ent:                  entClient,
		redis:                redis,
		taskRequestChan:      taskRequestChan,
		taskResponseChan:     taskResponseChan,
		workerStatusChan:     workerStatusChan,
		kothTaskRequestChan:  kothTaskRequestChan,
		kothTaskResponseChan: kothTaskResponseChan,
	}
}

func (e *Client) getState() state {
	return state(e.state.Load())
}

func (e *Client) setState(s state) {
	e.state.Store(int32(s))
}

func (e *Client) casState(old, new state) bool {
	return e.state.CompareAndSwap(int32(old), int32(new))
}

func (e *Client) Stop() error {
	if e.getState() == EngineRunning {
		_, err := cache.PublishEngineState(context.Background(), e.redis, model.EngineStateStopping)
		if err != nil {
			return err
		}
	}

	e.lock.Lock()
	defer e.lock.Unlock()

	if e.getState() == EngineWaiting || e.getState() == EngineRunning {
		e.setState(EnginePaused)
		e.nextRoundAt.Store(0)
		e.loopVersion++
		if e.loopCancel != nil {
			e.loopCancel()
			e.loopCancel = nil
		}
		_, err := cache.PublishEngineState(context.Background(), e.redis, model.EngineStatePaused)
		return err
	}

	return fmt.Errorf("cannot stop engine from state %q", e.getState())
}

func (e *Client) Start() error {
	e.lock.Lock()
	defer e.lock.Unlock()

	if !e.casState(EnginePaused, EngineWaiting) {
		return fmt.Errorf("engine is not paused (current state: %d)", e.getState())
	}

	loopCtx, cancel := context.WithCancel(e.ctx)
	e.loopCancel = cancel
	e.loopVersion++
	version := e.loopVersion
	ticker := time.NewTicker(config.Interval)
	e.nextRoundAt.Store(time.Now().Add(config.Interval).UnixNano())
	go e.loop(loopCtx, ticker, version)

	_, err := cache.PublishEngineState(context.Background(), e.redis, model.EngineStateWaiting)
	return err
}

func (e *Client) State() (model.EngineState, error) {
	switch e.getState() {
	case EnginePaused:
		return model.EngineStatePaused, nil
	case EngineWaiting:
		return model.EngineStateWaiting, nil
	case EngineRunning:
		return model.EngineStateRunning, nil
	}

	return "", fmt.Errorf("unknown engine state %q", e.getState())
}

func (e *Client) NextRoundAt() time.Time {
	if e.getState() == EnginePaused {
		return time.Time{}
	}
	next := e.nextRoundAt.Load()
	if next == 0 {
		return time.Time{}
	}
	return time.Unix(0, next)
}

func (e *Client) loop(ctx context.Context, ticker *time.Ticker, version uint64) {

	defer func() {
		ticker.Stop()
		e.lock.Lock()
		defer e.lock.Unlock()
		if e.loopVersion == version {
			e.loopCancel = nil
			e.nextRoundAt.Store(0)
			e.setState(EnginePaused)
			cache.PublishEngineState(context.Background(), e.redis, model.EngineStatePaused)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case tick := <-ticker.C:
			if ctx.Err() != nil || e.getState() == EnginePaused {
				return
			}
			err := e.loopRoundRunner(ctx, version, tick)
			if err != nil {
				if err == context.Canceled {
					return
				}
				logrus.WithError(err).Error("failed to run round")
				return
			}
			e.lock.Lock()
			if ctx.Err() != nil || e.loopVersion != version {
				e.lock.Unlock()
				return
			}
			e.setState(EngineWaiting)
			_, err = cache.PublishEngineState(context.Background(), e.redis, model.EngineStateWaiting)
			e.lock.Unlock()
			if err != nil {
				logrus.WithError(err).Error("failed to publish engine state")
				return
			}
		}
	}
}

func (e *Client) loopRoundRunner(ctx context.Context, version uint64, tick time.Time) error {
	e.lock.Lock()
	defer e.lock.Unlock()
	if ctx.Err() != nil || e.loopVersion != version {
		return context.Canceled
	}
	e.nextRoundAt.Store(tick.Add(config.Interval).UnixNano())

	e.setState(EngineRunning)
	_, err := cache.PublishEngineState(context.Background(), e.redis, model.EngineStateRunning)
	if err != nil {
		return err
	}

	roundCtx, cancel := context.WithTimeout(e.ctx, config.Interval-time.Millisecond*500)
	defer cancel()

	// Get the current round number
	var roundNumber int
	entLastRound, err := e.ent.Round.Query().
		Order(
			ent.Desc(round.FieldNumber),
		).
		First(e.ctx)
	if err != nil {
		roundNumber = 1
	} else {
		roundNumber = entLastRound.Number + 1
	}

	// Create new round
	entRound, err := e.ent.Round.Create().
		SetNumber(roundNumber).
		Save(e.ctx)
	if err != nil {
		logrus.WithError(err).Error("failed to create new round")
		return err
	}
	err = cache.SetRound(roundCtx, e.redis, entRound)
	if err != nil {
		logrus.WithError(err).Error("failed to set round")
		return err
	}

	_, err = cache.PublishLatestRound(roundCtx, e.redis, entRound)
	if err != nil {
		logrus.WithError(err).Error("failed to set latest round object")
		return err
	}

	entDisabledMinions, err := e.ent.Minion.Query().
		Where(
			minion.Deactivated(true),
		).
		All(e.ctx)
	if err != nil {
		logrus.WithError(err).Error("failed to get disabled minions")
		return err
	}

	disabledMinions := structs.WorkerStatus(
		static.MapSlice(
			entDisabledMinions,
			func(i int, m *ent.Minion) uuid.UUID {
				return m.ID
			},
		),
	)

	select {
	case <-roundCtx.Done():
		logrus.WithError(roundCtx.Err()).Error("failed to send disabled minions to workers")
		return nil
	case <-time.After(time.Millisecond * 500):
		logrus.Error("failed to send disabled minions to workers")
	case e.workerStatusChan <- &disabledMinions:
	}

	logrus.WithField("time", time.Now()).Infof("Running round %d", roundNumber)

	// Run the round
	err = e.runRound(roundCtx, entRound)

	logrus.WithField("time", time.Now()).Infof("Round %d complete", roundNumber)

	return err
}

func (e *Client) runRound(ctx context.Context, entRound *ent.Round) error {
	// Get all the tasks
	tasks, err := e.ent.CheckConfig.Query().
		WithUser().
		WithCheck().
		Order(
			// ID are uuids and thus check orders are shuffled
			ent.Desc(checkconfig.FieldID),
		).
		All(ctx)
	if err != nil {
		return err
	}

	var entStatuses []*ent.Status
	if len(tasks) > 0 {
		entStatuses, err = e.ent.Status.CreateBulk(
			static.MapSlice(
				tasks,
				func(i int, task *ent.CheckConfig) *ent.StatusCreate {
					return e.ent.Status.Create().
						SetRound(entRound).
						SetUser(task.Edges.User).
						SetCheck(task.Edges.Check).
						SetPoints(task.Edges.Check.Weight).
						SetStatus(status.StatusUnknown)
				},
			)...,
		).Save(ctx)
		if err != nil {
			return err
		}
	}

	kothTasks, err := e.ent.KothCheck.Query().
		All(ctx)
	if err != nil {
		return err
	}

	var entKothStatuses []*ent.KothStatus
	if len(kothTasks) > 0 {
		entKothStatuses, err = e.ent.KothStatus.CreateBulk(
			static.MapSlice(
				kothTasks,
				func(i int, task *ent.KothCheck) *ent.KothStatusCreate {
					return e.ent.KothStatus.Create().
						SetRound(entRound).
						SetPoints(task.Weight).
						SetCheck(task)
				},
			)...,
		).Save(ctx)
		if err != nil {
			return err
		}
	}

	// Create a map of round tasks to keep track of the tasks
	roundTasks := structs.NewSyncMap[uuid.UUID, *ent.CheckConfig]()
	kothRoundTasks := structs.NewSyncMap[uuid.UUID, *ent.KothCheck]()

	for i, entStatus := range entStatuses {
		roundTasks.Set(entStatus.ID, tasks[i])
	}

	for i, entKothStatus := range entKothStatuses {
		kothRoundTasks.Set(entKothStatus.ID, kothTasks[i])
	}

	var submitWG sync.WaitGroup
	submitWG.Add(2)

	// Submit tasks to the workers
	go func() {
		defer submitWG.Done()
		for _, entStatus := range entStatuses {
			entConfig, ok := roundTasks.Get(entStatus.ID)
			if !ok {
				logrus.WithField("id", entStatus.ID).Error("failed to get task")
				continue
			}

			conf, err := json.Marshal(entConfig.Config)
			if err != nil {
				logrus.WithError(err).Error("failed to marshal check config")
				continue
			}

			select {
			case <-ctx.Done():
				return
			case e.taskRequestChan <- &structs.TaskRequest{
				StatusID:   entStatus.ID,
				SourceName: entConfig.Edges.Check.Source,
				Config:     string(conf),
			}:
			}
		}
	}()

	go func() {
		defer submitWG.Done()
		for _, entKothStatus := range entKothStatuses {
			entKothCheck, ok := kothRoundTasks.Get(entKothStatus.ID)
			if !ok {
				logrus.WithField("id", entKothStatus.ID).Error("failed to get koth task")
				continue
			}

			select {
			case <-ctx.Done():
				return
			case e.kothTaskRequestChan <- &structs.KothTaskRequestBundle{
				KothTaskRequest: structs.KothTaskRequest{
					StatusID: entKothStatus.ID,
					Filename: entKothCheck.File,
				},
				RoutingKey: entKothCheck.Topic,
			}:
			}
		}
	}()

	allChecksReported := make(chan struct{}, 1)
	allKothChecksReported := make(chan struct{}, 1)
	checksOnce := &sync.Once{}
	kothOnce := &sync.Once{}
	checksRemain := roundTasks.Length() > 0
	kothChecksRemain := kothRoundTasks.Length() > 0
	// Bound concurrent status writes so large rounds cannot create one
	// goroutine and database operation per response at once.
	updateSlots := make(chan struct{}, min(max(runtime.NumCPU(), 4), 16))
	var updateWG sync.WaitGroup

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		if missing := roundTasks.Keys(); len(missing) > 0 {
			if _, err := e.ent.Status.Update().
				Where(status.IDIn(missing...)).
				SetStatus(status.StatusUnknown).
				SetPoints(0).
				Save(ctx); err != nil {
				logrus.WithError(err).Error("failed to mark unreported statuses as unknown")
			}
		}

		for koth_status_id := range kothRoundTasks.Map() {
			logrus.WithField("status", koth_status_id).Debug("koth status not reported, set to 0")
		}

		_, err = entRound.Update().
			SetComplete(true).
			Save(ctx)
		if err != nil {
			logrus.WithError(err).Error("failed to set round as complete")
		}

		type userScore struct {
			UserID uuid.UUID `json:"user_id"`
			Sum    int       `json:"sum"`
		}
		var users []userScore

		err = e.ent.Status.Query().
			Where(
				status.HasRoundWith(round.ID(entRound.ID)),
			).
			GroupBy(status.FieldUserID).
			Aggregate(ent.Sum(status.FieldPoints)).
			Scan(ctx, &users)
		if err != nil {
			logrus.WithError(err).Error("failed to aggregate points")
		} else {
			if len(users) > 0 {
				_, err = e.ent.ScoreCache.CreateBulk(
					static.MapSlice(
						users,
						func(i int, user userScore) *ent.ScoreCacheCreate {
							return e.ent.ScoreCache.Create().
								SetRound(entRound).
								SetUserID(user.UserID).
								SetPoints(user.Sum)
						},
					)...,
				).Save(ctx)
				if err != nil {
					logrus.WithError(err).Error("failed to create score cache")
				}
			}
		}

		scoreboard, err := helpers.Scoreboard(ctx, e.ent)
		if err != nil {
			logrus.WithError(err).Error("failed to get scoreboard")
		} else {
			_, err = cache.PublishScoreboardUpdate(ctx, e.redis, scoreboard)
			if err != nil {
				logrus.WithError(err).Error("failed to publish scoreboard")
			}
		}

		kothScoreboard, err := helpers.KothScoreboard(ctx, e.ent)
		if err != nil {
			logrus.WithError(err).Error("failed to get koth scoreboard")
		} else {
			_, err = cache.PublishKothScoreboardUpdate(ctx, e.redis, kothScoreboard)
			if err != nil {
				logrus.WithError(err).Error("failed to publish koth scoreboard")
			}
		}
	}()
	defer submitWG.Wait()
	defer updateWG.Wait()

	// Wait for the results
	for checksRemain || kothChecksRemain {
		select {
		case <-allChecksReported:
			checksRemain = false
		case <-allKothChecksReported:
			kothChecksRemain = false
		case <-ctx.Done():
			return nil
		case result := <-e.taskResponseChan:
			select {
			case <-ctx.Done():
				return nil
			case updateSlots <- struct{}{}:
			}
			updateWG.Add(1)
			if result.Status == status.StatusUp || result.Status == status.StatusDown || result.Status == status.StatusUnknown {
				go func() {
					defer updateWG.Done()
					defer func() { <-updateSlots }()
					e.updateStatus(ctx, roundTasks, result.StatusID, result.Error, result.Status, result.MinionID, allChecksReported, checksOnce)
				}()
			} else {
				go func() {
					defer updateWG.Done()
					defer func() { <-updateSlots }()
					e.updateStatus(ctx, roundTasks, result.StatusID, result.Error, status.StatusUnknown, uuid.UUID{}, allChecksReported, checksOnce)
				}()
				logrus.WithFields(logrus.Fields{
					"status":    result.Status,
					"status_id": result.StatusID,
				}).Error("unknown status")
			}
		case result := <-e.kothTaskResponseChan:
			select {
			case <-ctx.Done():
				return nil
			case updateSlots <- struct{}{}:
			}
			updateWG.Add(1)
			go func() {
				defer updateWG.Done()
				defer func() { <-updateSlots }()
				e.updateKothStatus(ctx, kothRoundTasks, result, allKothChecksReported, kothOnce)
			}()
		}
	}

	return nil
}

func cleanStatus(s string) string {
	s = strings.ReplaceAll(s, "\x00", "")
	if len(s) > 4096 {
		return s[:4096]
	}
	return s
}

func (e *Client) updateStatus(ctx context.Context, roundTasks *structs.SyncMap[uuid.UUID, *ent.CheckConfig], statusID uuid.UUID, errorMessage string, checkStatus status.Status, minionID uuid.UUID, allChecksReported chan<- struct{}, once *sync.Once) {
	_, ok := roundTasks.Get(statusID)
	if !ok {
		logrus.WithField("status_id", statusID).Error("uuid not belong to round was submitted")
		return
	}

	entStatusUpdate := e.ent.Status.UpdateOneID(statusID).
		SetStatus(checkStatus).
		SetMinionID(minionID)

	if errorMessage != "" {
		entStatusUpdate.SetError(cleanStatus(errorMessage))
	}

	if checkStatus != status.StatusUp {
		entStatusUpdate.SetPoints(0)
	}

	_, err := entStatusUpdate.Save(ctx)
	if err != nil {
		logrus.WithField("id", statusID).WithError(err).Error("failed to update status")
		return
	}
	_, remaining := roundTasks.Pop(statusID)

	if remaining == 0 {
		once.Do(func() {
			allChecksReported <- struct{}{}
		})
	}
}

func (e *Client) updateKothStatus(ctx context.Context, roundTasks *structs.SyncMap[uuid.UUID, *ent.KothCheck], kothTaskResponse *structs.KothTaskResponse, allChecksReported chan<- struct{}, once *sync.Once) {
	_, ok := roundTasks.Get(kothTaskResponse.StatusID)
	if !ok {
		logrus.WithField("status_id", kothTaskResponse.StatusID).Error("uuid not belong to round was submitted")
		return
	}

	entKothStatusUpdate := e.ent.KothStatus.UpdateOneID(kothTaskResponse.StatusID)

	if kothTaskResponse.Error != "" {
		entKothStatusUpdate.SetError(cleanStatus(kothTaskResponse.Error))
	}

	userID, err := uuid.Parse(strings.TrimSpace(kothTaskResponse.Content))
	if err == nil {
		entKothStatusUpdate.SetUserID(userID)
	}

	_, err = entKothStatusUpdate.SetMinionID(kothTaskResponse.MinionID).Save(ctx)
	if err != nil {
		logrus.WithField("status_id", kothTaskResponse.StatusID).WithError(err).Error("failed to update koth status")
		return
	}
	_, remaining := roundTasks.Pop(kothTaskResponse.StatusID)
	if remaining == 0 {
		once.Do(func() {
			allChecksReported <- struct{}{}
		})
	}
}
