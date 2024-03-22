package redislocker

import (
	"context"
	"errors"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis"
	"github.com/tus/tusd/v2/pkg/handler"
)

func New(client redis.Pool) handler.Locker {
	rs := redsync.New(client)
	return &RedisLocker{
		rs: rs,
	}
}

type RedisLocker struct {
	rs *redsync.Redsync
}

func (locker *RedisLocker) NewLock(id string) (handler.Lock, error) {
	mutex := locker.rs.NewMutex(id)
	return &redisLock{
		id:    id,
		mutex: mutex,
	}, nil
}

type redisLock struct {
	id        string
	mutex     *redsync.Mutex
	onRelease func()
	ctx       context.Context
	cancel    func()
}

func (l *redisLock) Lock(ctx context.Context, releaseRequested func()) error {
	if err := l.lock(ctx); err != nil {
		//attempt request release
		// needs to be able to steal a lock while something's trying to extend theirs.. not sure how to do this.
		select {
		case <-time.After(100 * time.Millisecond):
			return l.Lock(ctx, releaseRequested)
		case <-ctx.Done():
			// TODO extract real error types
			return errors.New("Timeout")
		}
	}

	return nil
}

func (l *redisLock) lock(ctx context.Context) error {
	if err := l.mutex.TryLockContext(ctx); err != nil {
		return err
	}
	// leave room for something to steal the lock
	// could rewrite with extend but i'm not sure how something could steal
	l.ctx, l.cancel = context.WithDeadline(context.Background(), l.mutex.Until().Add(1*time.Second))
	go func() {
		<-l.ctx.Done()
		if err := l.lock(context.TODO()); err != nil {
			l.onRelease()
		}
	}()
	return nil
}

func (l *redisLock) Unlock() error {
	_, err := l.mutex.Unlock()
	defer l.cancel()
	return err
}
