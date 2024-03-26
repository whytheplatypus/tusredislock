package redislocker_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	redislocker "github.com/whytheplatypus/tusredislock"
)

func TestLockUnlock(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "redispw", // no password set
		DB:       0,         // use default DB
	})

	locker := redislocker.New(rdb)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	l, err := locker.NewLock("test")
	if err != nil {
		t.Error(err)
	}
	requestRelease := func() {
		t.Error("shouldn't have been calld")
	}
	if err := l.Lock(ctx, requestRelease); err != nil {
		t.Error(err)
	}
	if err := l.Unlock(); err != nil {
		t.Error(err)
	}
	if err := l.Lock(ctx, requestRelease); err != nil {
		t.Error(err)
	}
	if err := l.Unlock(); err != nil {
		t.Error(err)
	}
}

func TestHeldLockExchange(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "redispw", // no password set
		DB:       0,         // use default DB
	})

	locker := redislocker.New(rdb)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	l, err := locker.NewLock("test")
	if err != nil {
		t.Error(err)
	}
	requestRelease := func() {
		l.Unlock()
	}
	if err := l.Lock(ctx, requestRelease); err != nil {
		t.Error(err)
	}
	//assert that request release is called
	otherL, err := locker.NewLock("test")
	if err != nil {
		t.Error(err)
	}
	if err := otherL.Lock(ctx, requestRelease); err != nil {
		t.Error(err)
	}
	if err := otherL.Unlock(); err != nil {
		t.Error(err)
	}
}

func TestHeldLockNoExchange(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "redispw", // no password set
		DB:       0,         // use default DB
	})

	locker := redislocker.New(rdb)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	l, err := locker.NewLock("test")
	if err != nil {
		t.Error(err)
	}
	requestRelease := func() {
		t.Log("release requested")
	}
	if err := l.Lock(ctx, requestRelease); err != nil {
		t.Error(err)
	}
	//assert that request release is called
	otherL, err := locker.NewLock("test")
	if err != nil {
		t.Error(err)
	}
	if err := otherL.Lock(ctx, requestRelease); err == nil {
		t.Error("should have errored")
	} else {
		t.Log(err)
	}
}
