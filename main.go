package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"

	"net/http"
	_ "net/http/pprof"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

type handler struct {
	store Store
	conn  *amqp.Connection
}

func main() {
	go http.ListenAndServe(":8080", nil)

	viper.AutomaticEnv()
	viper.SetDefault("BATCH_SIZE", 1024)
	viper.SetDefault("BATCH_WAIT", time.Second)
	configFile := flag.String("config", "./config.toml", "path of the config file")
	flag.Parse()
	viper.SetConfigFile(*configFile)
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		log.Printf("cannot read config file: %v\nUse env instead\n", err)
	}

	log.Println("BATCH_SIZE:", viper.GetInt("BATCH_SIZE"))
	log.Println("BATCH_WAIT:", viper.GetDuration("BATCH_WAIT"))

	h := &handler{
		store: NewPostgreSQL(viper.GetString("POSTGRESQL_URI")),
	}

	h.conn, err = amqp.Dial(viper.GetString("RABBITMQ_URI"))
	failOnError(err, "Failed to connect to RabbitMQ")

	go func() {
		log.Fatalf("closing: %s", <-h.conn.NotifyClose(make(chan *amqp.Error)))
	}()

	log.Println("Ready, wait on topic 'sensors'")

	go h.handleAccountDeleted()
	go h.handlerRPCFindLastest()

	h.handleSensorStore(viper.GetInt("BATCH_SIZE"), viper.GetDuration("BATCH_WAIT"))
}

func (h *handler) handleSensorStore(prefectCount int, batchWaitTime time.Duration) {
	ch, err := h.conn.Channel()
	failOnError(err, "Failed to open a channel")

	err = ch.ExchangeDeclare(
		"sensors", // name
		"topic",   // type
		true,      // durable
		false,     // auto-deleted
		false,     // internal
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"sensors", // name
		true,      // durable
		false,     // delete when usused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		prefectCount, // prefetch count
		0,            // prefetch size
		false,        // global
	)
	failOnError(err, "Failed to set QoS")

	// Get every routing key
	err = ch.QueueBind(
		q.Name,    // queue name
		"#",       // routing key
		"sensors", // exchange
		false,     // no wait
		nil,       // args
	)
	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
		q.Name,            // queue
		"sensors-storage", // consumer
		false,             // auto ack
		false,             // exclusive
		false,             // no local
		false,             // no wait
		nil,               // args
	)
	failOnError(err, "Failed to register a consumer")

	batchQueue := make([]*amqp.Delivery, 0, prefectCount)

	sendBatchSensors := func() {
		if len(batchQueue) == 0 {
			return
		}
		sensors := make([]Sensor, 0, len(batchQueue))
		for _, d := range batchQueue {
			rk := strings.Split(d.RoutingKey, ".")
			aid, _ := uuid.FromString(rk[0])
			sid, _ := uuid.FromString(rk[1])

			timestamp, _ := time.Parse(time.RFC3339Nano, d.Headers["timestamp"].(string))

			s := Sensor{
				AID:        aid,
				SID:        sid,
				ReceivedAt: timestamp,
				Data:       json.RawMessage(d.Body),
			}
			sensors = append(sensors, s)
		}

		defer func() {
			// // free the queue
			// for i := range batchQueue {
			// 	batchQueue[i] = nil
			// }
			// batchQueue = batchQueue[:0]
			batchQueue = make([]*amqp.Delivery, 0, prefectCount)
		}()

		err := h.store.BatchSensors(sensors)
		if err != nil {
			batchQueue[len(batchQueue)-1].Nack(true, true)
			log.Printf("fail insert db: %v", err)
			return
		}
		batchQueue[len(batchQueue)-1].Ack(true)
		log.Printf("insert %d sensors to db", len(batchQueue))
	}

	timer := time.NewTimer(batchWaitTime)
	for {
		select {
		case <-timer.C:
			if len(batchQueue) > 0 {
				sendBatchSensors()
			}
		case d := <-msgs:
			if len(batchQueue) == 0 {
				// reset the time so that the first msgs enqueu will be insterted at max batchWaitTime
				timer.Reset(batchWaitTime)
			}

			// fill the batch queue of messages
			if len(batchQueue) < cap(batchQueue) {
				batchQueue = append(batchQueue, &d)
			} else {
				log.Println("FULL")
			}

			// check if must insert the batch to the DB
			// insert only if the batch queue is full or the last inserted message is old
			if len(batchQueue) == cap(batchQueue) {
				sendBatchSensors()
			}
		}
	}
}

func (h *handler) handlerRPCFindLastest() {
	ch, err := h.conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"rpc_sensors_latest", // name
		false,                // durable
		false,                // delete when usused
		false,                // exclusive
		false,                // no-wait
		nil,                  // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	for d := range msgs {
		request := struct {
			Aid uuid.UUID `json:"aid"`
		}{}
		err := json.Unmarshal(d.Body, &request)
		if err != nil {
			log.Printf("fail to unmarchal request: %v", err)
			continue
		}

		log.Println("RPCFindLastest", request.Aid)

		sensors, err := h.store.FindLastByAID(request.Aid)
		if err != nil {
			log.Printf("fail to find %v: %v", request.Aid, err)
		}

		responce, err := json.Marshal(sensors)
		if err != nil {
			log.Printf("fail to marshal sensors data: %v", err)
		}

		err = ch.Publish(
			"",        // exchange
			d.ReplyTo, // routing key
			false,     // mandatory
			false,     // immediate
			amqp.Publishing{
				ContentType:   "application/json",
				CorrelationId: d.CorrelationId,
				Body:          responce,
			})
		if err != nil {
			log.Printf("failed to publish a message: %v", err)
		}
		d.Ack(false)
	}
}

func (h *handler) handleAccountDeleted() {
	ch, err := h.conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"sensor-storage_account", // name
		true,                     // durable
		false,                    // delete when usused
		false,                    // exclusive
		false,                    // no-wait
		nil,                      // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,     // queue name
		"#.delete", // routing key
		"account",  // exchange
		false,      // no-wait
		nil)
	if err != nil {
		log.Panicln(err)
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	log.Println("Ready, wait on topic 'account'")

	for d := range msgs {
		straid := strings.SplitN(d.RoutingKey, ".", 2)
		aid, err := uuid.FromString(straid[0])
		if err != nil {
			log.Printf("cannot extract aid from routing key %v: %v\n", d.RoutingKey, err)

			continue
		}
		log.Printf("purge sensors for account %v\n", aid)
		err = h.store.Purge(aid)
		if err != nil {
			log.Printf("cannot purge %v: %v\n", aid, err)
		}
	}
}
