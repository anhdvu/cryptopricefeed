package feeder

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
)

var (
	timeout         = 2 * time.Minute
	defaultInterval = 30 * time.Second
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

	if t > 0 {
		go m.startCleanup(t)
	}
	return m
}

// Run acts as main entrypoint
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

type feeder struct {
	mu                 sync.RWMutex
	r                  *redis.Client
	c                  *websocket.Conn
	lastPublishSuccess time.Time
	close              chan bool
	disconnected       bool
	symbol             string
	price              chan string
}

func (m *Manager) spawnFeeder(channelName string) (*feeder, error) {
	u := genURI(channelName)
	c, _, err := websocket.DefaultDialer.Dial(u, nil)
	log.Printf("created websocket to %s", u)
	if err != nil {
		return nil, err
	}

	f := &feeder{
		r:      m.r,
		c:      c,
		symbol: channelName,
		close:  make(chan bool),
	}

	go f.start()
	return f, nil
}

func genURI(channelname string) string {
	parts := strings.Split(channelname, ":")
	cex := parts[1]

	switch cex {
	case "binance":
		symbol := strings.Replace(parts[2], "-", "", -1)
		return fmt.Sprintf("wss://stream.binance.com:9443/ws/%s@trade", symbol)
	default:
		return "unsupported exchange"
	}
}

func (f *feeder) start() {
	f.price = make(chan string)
	f.lastPublishSuccess = time.Now()

	go func() {
		defer func() {
			if err := recover(); err != nil {
				fmt.Println("shutting down panic goroutine...")
			}
		}()

		for {
			_, message, err := f.c.ReadMessage()
			if err != nil {
				switch {
				case errors.Is(err, websocket.ErrCloseSent):
					log.Printf("")
				default:
					log.Printf("%v\nreturning to the caller...", err)
				}
				return
			}
			f.price <- string(message)
		}
	}()

	for {
		select {
		case <-f.close:
			log.Println("closing websocket connection...")
			err := f.c.Close()
			if err != nil {
				log.Println(err)
			}
			f.disconnected = true
			close(f.price)
			return
		case <-time.After(timeout):
			log.Println("timed out, no message has been received")
			err := f.c.Close()
			if err != nil {
				log.Println(err)
			}
			f.disconnected = true
			return
		case price := <-f.price:
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			result, err := f.r.Publish(ctx, f.symbol, price).Result()
			if err != nil {
				log.Println(err)
			}
			f.mu.Lock()
			if result > 0 {
				f.lastPublishSuccess = time.Now()
			}
			f.mu.Unlock()
		}
	}
}
