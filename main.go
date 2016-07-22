package main

import (
	"github.com/garyburd/redigo/redis"
	"github.com/valyala/fasthttp"

	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

const (
	FlushSize = 10000
)

var (
	MaxWorker = os.Getenv("MAX_WORKERS")
	MaxQueue  = os.Getenv("MAX_QUEUE")
)

var JobQueue chan Payload
var Redis *redis.Pool

type Payload struct {
	Action     string
	SpotID     string
	CampaignID string
	BannerID   string
	Delay      string
}

func (p *Payload) String() string {
	return time.Now().Format("2006-02-01") + ":" + p.Action + ":" + p.SpotID + ":" + p.CampaignID + ":" + p.BannerID + ":" + p.Delay
}

type Dispatcher struct {
	WorkerPool chan chan Payload
	maxWorkers int
}

func NewDispatcher(maxWorkers int) *Dispatcher {
	pool := make(chan chan Payload, maxWorkers)
	return &Dispatcher{WorkerPool: pool, maxWorkers: maxWorkers}
}

func (d *Dispatcher) Run() {
	for i := 0; i < d.maxWorkers; i++ {
		worker := NewWorker(d.WorkerPool)
		worker.Start()
	}

	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	log.Println("Dispatcher started")
	for {
		select {
		case job := <-JobQueue:
			// a job request has been received
			go func(job Payload) {
				// try to obtain a worker job channel that is available.
				// this will block until a worker is idle
				jobChannel := <-d.WorkerPool

				// dispatch the job to the worker job channel
				jobChannel <- job
			}(job)
		}
	}
}

// Worker represents the worker that executes the job
type Worker struct {
	WorkerPool chan chan Payload
	JobChannel chan Payload
	quit       chan bool
}

func NewWorker(workerPool chan chan Payload) Worker {
	return Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan Payload),
		quit:       make(chan bool)}
}

// Start method starts the run loop for the worker, listening for a quit channel in
// case we need to stop it
func (w Worker) Start() {
	log.Println("Worker started")
	go func() {
		con := Redis.Get()
		ctr := 0
		for {
			// register the current worker into the worker queue.
			w.WorkerPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:
				ctr++
				if ctr > FlushSize {
					con.Flush()
					ctr = 0
				}
				err := con.Send("INCR", job.String())
				if err != nil {
					log.Printf("Redis error: %s", err.Error())
				}

			case <-w.quit:
				// we have received a signal to stop
				return
			}
		}

	}()
}

// Stop signals the worker to stop listening for work requests.
func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}

func handler(ctx *fasthttp.RequestCtx) {

	if string(ctx.Method()) != "GET" {
		ctx.Error("Unsupported method", fasthttp.StatusMethodNotAllowed)
		return
	}

	var content = Payload{}
	content.Action = string(ctx.FormValue("a"))
	content.SpotID = string(ctx.FormValue("s"))
	content.CampaignID = string(ctx.FormValue("c"))
	content.BannerID = string(ctx.FormValue("b"))
	content.Delay = string(ctx.FormValue("d"))

	JobQueue <- content

	ctx.Response.Header.SetStatusCode(http.StatusOK)
}

func main() {
	mw, _ := strconv.Atoi(MaxWorker)
	mq, _ := strconv.Atoi(MaxQueue)

	Redis = &redis.Pool{
		MaxIdle: mw,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", ":6379")
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	JobQueue = make(chan Payload, mq)

	dispatcher := NewDispatcher(mw)
	dispatcher.Run()

	log.Println("Listen on port 3000")
	fasthttp.ListenAndServe(":3000", handler)
}
