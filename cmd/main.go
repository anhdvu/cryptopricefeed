package main

import (
	"log"

	"github.com/anhdvu/cryptopricefeed/feeder"
)

func main() {
	done := make(chan bool)
	man := feeder.New()
	err := man.Run()
	if err != nil {
		log.Println(err)
	}

	<-done
}
