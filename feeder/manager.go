package feeder

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// Manager represents a collection of crypto price feeders
type Manager struct {
	mu          sync.RWMutex
	feeders     map[string]*feeder
	r           *redis.Client
	stopCleanup chan bool
}

// New returns a Manager with default cleanup interval.
func New() *Manager {
	return NewWithInterval(defaultInterval)
}

// NewWithInterval returns a Manager with custom cleanup interval.
func NewWithInterval(t time.Duration) *Manager {
	rc := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	m := &Manager{
		feeders: make(map[string]*feeder),
		r:       rc,
	}

	// start a goroutine dedicated to stale connection cleanup
	if t > 0 {
		go m.startCleanup(t)
	}
	return m
}

// Run acts as main entrypoint of the application.
func (m *Manager) Run() error {
	var ctx context.Context
	var cancel context.CancelFunc
	var pschannels []string
	var err error

	for {
		ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		pschannels, err = m.r.PubSubChannels(ctx, "pair*").Result()
		if err != nil {
			return err
		}

		m.mu.Lock()
		for _, channel := range pschannels {
			if _, ok := m.feeders[channel]; !ok {
				feeder, err := m.spawnFeeder(channel)
				if err != nil {
					return err
				}
				m.feeders[channel] = feeder
			}
		}
		m.mu.Unlock()
	}
}

func (m *Manager) startCleanup(interval time.Duration) {
	m.stopCleanup = make(chan bool)
	ticker := time.NewTicker(interval)

	// To-do:
	for {
		select {
		case <-ticker.C:
			m.removeStale()
		case <-m.stopCleanup:
			ticker.Stop()
			log.Println("stopping cleanup process...")
			return
		}
	}
}

func (m *Manager) removeStale() {
	m.mu.Lock()
	for symbol, feeder := range m.feeders {
		feeder.mu.Lock()
		if time.Since(feeder.lastPublishSuccess) > 1*time.Minute {
			feeder.close <- true
			log.Printf("signaled to close stale feeder %s", feeder.symbol)
			delete(m.feeders, symbol)
			log.Printf("removed stale feeder %s", feeder.symbol)
		}
		feeder.mu.Unlock()
	}
	m.mu.Unlock()
}
