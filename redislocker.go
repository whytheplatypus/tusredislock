package redislocker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
	"github.com/tus/tusd/v2/pkg/handler"
)

func New(client *redis.Client) handler.Locker {
	rs := redsync.New(goredis.NewPool(client))
	return &RedisLocker{
		rs:    rs,
		redis: client,
	}
}

type RedisLocker struct {
	rs    *redsync.Redsync
	redis *redis.Client
}

func (locker *RedisLocker) NewLock(id string) (handler.Lock, error) {
	mutex := locker.rs.NewMutex(id)
	return &redisLock{
		id:    id,
		mutex: mutex,
		redis: locker.redis,
	}, nil
}

type redisLock struct {
	id        string
	mutex     *redsync.Mutex
	onRelease func()
	ctx       context.Context
	cancel    func()
	redis     *redis.Client
}

func (l *redisLock) channelName() string {
	return fmt.Sprintf("tusd_lock_release_request_%s", l.id)
}

func (l *redisLock) Lock(ctx context.Context, releaseRequested func()) error {
	if err := l.lock(ctx); err != nil {
		if err := l.retryLock(ctx); err != nil {
			return err
		}
	}
	l.onRelease = releaseRequested
	return nil
}

func (l *redisLock) lock(ctx context.Context) error {
	if err := l.mutex.TryLockContext(ctx); err != nil {
		return err
	}

	l.ctx, l.cancel = context.WithCancel(context.Background())
	psub := l.redis.PSubscribe(l.ctx, l.channelName())
	c := psub.Channel()
	go l.listen(l.ctx, c)
	go l.keepAlive(l.ctx)

	return nil
}

func (l *redisLock) retryLock(ctx context.Context) error {
	for {
		//Question how many times should this send a request?
		//TODO do something with the IntCmd result
		l.redis.Publish(ctx, l.channelName(), "please release")
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

func (l *redisLock) listen(ctx context.Context, unlockRequests <-chan *redis.Message) {
	select {
	case <-unlockRequests:
		l.onRelease()
	case <-ctx.Done():
		return
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
