package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"log"
	"os"
	"sync"
	"time"
)

const (
	DbUser          = "postgres:123"
	DbName          = "demo"
	DbHost          = "192.168.150.130"
	GoroutinesCount = 10
	BatchSize       = 100
)

type Counter struct {
	mut sync.Mutex
	cnt int
	pos int
}

func getCounter(ctx context.Context) (ct *Counter, err error) {
	conn, err := pgx.Connect(ctx, "postgresql://"+DbUser+"@"+DbHost+"/"+DbName)
	if err != nil {
		return
	}
	defer conn.Close(ctx)
	cnt := 0
	err = conn.QueryRow(ctx,
		"SELECT count(*) FROM boarding_passes").Scan(
		&cnt,
	)
	if err != nil {
		return
	}
	log.Println("Creating counter. Size:", cnt)
	return &Counter{
		mut: sync.Mutex{},
		cnt: cnt,
		pos: 0,
	}, nil
}

func (ct *Counter) next() *int {
	ct.mut.Lock()
	defer ct.mut.Unlock()

	if ct.pos < ct.cnt {
		pos := ct.pos
		ct.pos += BatchSize
		return &pos
	}
	return nil
}

func readFromBase(ctx context.Context, id int, fileSync bool, ctr *Counter, endChan chan int) {
	conn, err := pgx.Connect(ctx, "postgresql://"+DbUser+"@"+DbHost+"/"+DbName)
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close(ctx)

	flush := "flush"
	if !fileSync {
		flush = "noflush"
	}
	fileName := fmt.Sprintf("res/dump%d.%s", id, flush)

	f, err := os.Create(fileName)
	if err != nil {
		log.Fatalln(err)
	}
	defer f.Close()

	for {
		offset := ctr.next()
		if offset == nil {
			endChan <- 1
			break
		}
		var ticketNo, seatNo string
		var flightId int
		rows, err := conn.Query(ctx, fmt.Sprintf(
			"SELECT ticket_no, flight_id, seat_no FROM boarding_passes OFFSET %d LIMIT %d", *offset, BatchSize))
		if err != nil {
			log.Fatalln(err)
		}
		i := 0
		for rows.Next() {
			err = rows.Scan(
				&ticketNo, &flightId, &seatNo,
			)
			if err != nil {
				log.Fatalln(err)
			}
			if _, err := f.Write([]byte(fmt.Sprintf("[%d]%d| %s %d %s\n", id, *offset+i, ticketNo,
				flightId, seatNo))); err != nil {
				log.Fatalln(err)
			}
			i++
		}
		if fileSync {
			f.Sync()
		}
	}

}

func runRead(ctr *Counter, fileSync bool) {
	endChan := make(chan int)
	for i := 0; i < GoroutinesCount; i++ {
		go readFromBase(context.Background(), i+1, fileSync, ctr, endChan)
	}

	for i := 0; i < GoroutinesCount; i++ {
		<-endChan
	}
}

func getTimeElapsed(message string, f func()) {
	start := time.Now()
	f()
	elapsed := time.Since(start)
	log.Printf("%s %d\n", message, elapsed.Milliseconds())
}

func main() {
	ctr, err := getCounter(context.Background())
	if err != nil {
		log.Fatalln(err)
	}
	getTimeElapsed("Took milliseconds without flush", func() {
		runRead(ctr, false)
	})

	ctr, err = getCounter(context.Background())
	if err != nil {
		log.Fatalln(err)
	}

	getTimeElapsed("Took milliseconds with flush", func() {
		runRead(ctr, true)
	})
}
