package coordinator

import (
	"log"
	"os"
	"strconv"
	"time"
)

func (c *Coordinator) StartTrimScheduler(keepCount int, interval time.Duration) {
	if err := c.store.Trim(keepCount); err != nil {
		log.Println("Error trimming store at startup:", err)
	}
	go func() {
		for {
			if err := c.store.Trim(keepCount); err != nil {
				log.Println("Error trimming store:", err)
			}
			time.Sleep(interval)
		}
	}()
}

func (c *Coordinator) GetTrimIntervalInMinutes() time.Duration {
	intervalMinutes := 10080
	if val := os.Getenv("COORDINATOR_TRIM_INTERVAL_IN_MINUTES"); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			intervalMinutes = n
		} else {
			log.Printf("Invalid COORDINATOR_TRIM_INTERVAL_IN_MINUTES, using default: %d", intervalMinutes)
		}
	}
	return time.Duration(intervalMinutes) * time.Minute
}
