package feeder

import (
	"context"
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

type feeder struct {
	mu                 sync.RWMutex
	r                  *redis.Client
	c                  *websocket.Conn
	lastPublishSuccess time.Time
	close              chan bool
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
	done := make(chan bool)
	defer close(done)

	go func() {
		defer func() {
			if err := recover(); err != nil {
				fmt.Println("shutting down panic goroutine...")
			}
		}()

		for {
			select {
			case <-done:
				return
			default:
				_, message, err := f.c.ReadMessage()
				if err != nil {
					log.Printf("%v\nreturning to the caller...", err)
					return
				}
				f.price <- string(message)
			}
		}
	}()

	for {
		select {
		case <-f.close:
			log.Printf("closing websocket connection to %s...", f.symbol)
			done <- true
			close(f.price)
			err := f.c.Close()
			if err != nil {
				log.Println(err)
			}
			return

		case <-time.After(timeout):
			log.Println("timed out, no message was received")
			done <- true
			err := f.c.Close()
			if err != nil {
				log.Println(err)
			}
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
