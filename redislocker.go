package redislocker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
	"github.com/tus/tusd/v2/pkg/handler"
)

type LockerOption func(l *RedisLocker)

func WithLogger(logger *slog.Logger) LockerOption {
	return func(l *RedisLocker) {
		l.logger = logger
	}
}

func New(client *redis.Client, lockerOptions ...LockerOption) handler.Locker {
	rs := redsync.New(goredis.NewPool(client))

	locker := &RedisLocker{
		rs:    rs,
		redis: client,
	}
	for _, option := range lockerOptions {
		option(locker)
	}
	//defaults
	if locker.logger == nil {
		locker.logger = slog.Default()
	}

	return locker
}

type LockExchange interface {
	Listen(ctx context.Context, id string, callback func())
	Request(ctx context.Context, id string)
}

type RedisLockExchange struct {
	client *redis.Client
}

func (e *RedisLockExchange) channelName(id string) string {
	return fmt.Sprintf("tusd_lock_release_request_%s", id)
}

func (e *RedisLockExchange) Listen(ctx context.Context, id string, callback func()) {
	psub := e.client.PSubscribe(ctx, e.channelName(id))
	c := psub.Channel()
	select {
	case <-c:
		callback()
	case <-ctx.Done():
		return
	}
}

func (e *RedisLockExchange) Request(ctx context.Context, id string) {

	e.client.Publish(ctx, e.channelName(id), "please release")
}

type RedisLocker struct {
	rs     *redsync.Redsync
	redis  *redis.Client
	logger *slog.Logger
}

func (locker *RedisLocker) NewLock(id string) (handler.Lock, error) {
	mutex := locker.rs.NewMutex(id)
	return &redisLock{
		id:    id,
		mutex: mutex,
		exchange: &RedisLockExchange{
			client: locker.redis,
		},
	}, nil
}

type redisLock struct {
	id       string
	mutex    *redsync.Mutex
	ctx      context.Context
	cancel   func()
	exchange LockExchange
}

func (l *redisLock) Lock(ctx context.Context, releaseRequested func()) error {
	if err := l.lock(ctx); err != nil {
		if err := l.retryLock(ctx); err != nil {
			return err
		}
	}
	go l.exchange.Listen(l.ctx, l.id, releaseRequested)
	return nil
}

func (l *redisLock) lock(ctx context.Context) error {
	if err := l.mutex.TryLockContext(ctx); err != nil {
		return err
	}

	l.ctx, l.cancel = context.WithCancel(context.Background())
	go l.keepAlive(l.ctx)

	return nil
}

func (l *redisLock) retryLock(ctx context.Context) error {
	for {
		//Question how many times should this send a request?
		//TODO do something with the IntCmd result
		l.exchange.Request(ctx, l.id)
		select {
		case <-time.After(100 * time.Millisecond):
			if err := l.lock(ctx); err != nil {
				continue
			}
			return nil
		case <-ctx.Done():
			return handler.ErrLockTimeout
		}
	}
}

func (l *redisLock) keepAlive(ctx context.Context) {
	//insures that an extend will be canceled if it's unlocked in the middle of an attempt
	for {
		select {
		case <-time.After(time.Until(l.mutex.Until()) - 4*time.Second):
			log.Println("attempting to extend lock")
			_, err := l.mutex.ExtendContext(ctx)
			if err != nil {
				log.Fatal("failed to extend lock:", err)
			}
		case <-ctx.Done():
			log.Println("lock was closed")
			return
		}
	}
}

func (l *redisLock) Unlock() error {
	_, err := l.mutex.Unlock()
	if l.cancel == nil {
		return errors.New("something's gone horribly wrong")
	}
	l.cancel()
	return err
}
