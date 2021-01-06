package main

import (
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/config/property"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/core/logger"
)

func main() {
	// Connect
	clientConfig := hazelcast.NewConfig()
	clientConfig.NetworkConfig().AddAddress("hazelcast:5701")
	clientConfig.SetProperty(property.LoggingLevel.Name(), logger.DebugLevel)
	client, _ := hazelcast.NewClientWithConfig(clientConfig)

	// The map is stored on the server but we can access it from the client
	mapName := "default"
	mp, _ := client.GetMap(mapName)

	// clear the map
	mp.Clear()

	var wg sync.WaitGroup

	for i := 0; i < 1; i++ {
		wg.Add(1)
		go load(mp, 10000000, &wg)
	}

	wg.Add(1)
	go printSize(mp, &wg)

	wg.Add(1)
	go printNumGoroutine(&wg)

	wg.Wait()

	// Disconnect
	client.Shutdown()
}

func printSize(mp core.Map, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		size, _ := mp.Size()
		log.Printf("Map '%v' Size %v\n", mp.Name(), size)
		time.Sleep(time.Second)
	}
}

func load(mp core.Map, items int, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < items; i++ {
		log.Printf("I: %v", i)
		rnd := uuid.New().String()
		mp.SetWithTTL(rnd, rnd, time.Duration(3600)*time.Second)
		log.Println("Set!")
		mp.Get(rnd)
		log.Println("Get!")

	}
}

func printNumGoroutine(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		log.Printf("Number of Goroutines: %v", runtime.NumGoroutine())
		time.Sleep(time.Second)
	}
}
