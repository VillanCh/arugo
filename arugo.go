package arugo

import (
	"fmt"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

type NormalExitError struct{}

func (e NormalExitError) Error() string {
	return "Arugo normal exit"
}

type AmqpConsumeConfig struct {
	ConsumeKey string
	Exclusive  bool
	NoAck      bool
	NoLocal    bool
	NoWait     bool
	Args       amqp.Table
}

// 定义 Consumer 接口
type ArugoConsumerIf interface {
	GetConsumeConfig() *AmqpConsumeConfig
	GetQueueName() string
	OnChannelCreated(channel *amqp.Channel) error
	Handle(req amqp.Delivery) error
	GetArugoApp() (*Arugo, error)
	BindArugoApp(arugo *Arugo)
}

type ArugoConsumerBase struct {
	I ArugoConsumerIf

	QueueName  string
	_publisher *ArugoPublisher
	_app       *Arugo
}

func (b *ArugoConsumerBase) GetArugoApp() (*Arugo, error) {
	if b._app == nil {
		return nil, errors.New("no arugo is binded.")
	}

	return b._app, nil
}

func (b *ArugoConsumerBase) BindArugoApp(arugo *Arugo) {
	b._app = arugo
}

func (b *ArugoConsumerBase) GetPublisher() (*ArugoPublisher, error) {
	if b._publisher != nil {
		return b._publisher, nil
	}

	app, err := b.I.GetArugoApp()
	if err != nil {
		return nil, errors.Errorf("cannot get arugo: %s", err)
	}

	b._publisher, err = app.GetPublisher(3, true)
	if err != nil {
		return nil, errors.Errorf("cannot get publisher: %s", err)
	}

	return b._publisher, nil
}

func (b *ArugoConsumerBase) GetQueueName() string {
	return b.QueueName
}

func (b *ArugoConsumerBase) GetConsumeConfig() *AmqpConsumeConfig {
	return &AmqpConsumeConfig{
		ConsumeKey: "",
		Exclusive:  false,
		NoAck:      false,
		NoLocal:    false,
		NoWait:     false,
		Args:       nil,
	}
}

func (b *ArugoConsumerBase) Handle(req amqp.Delivery) error {
	panic("not implemented")
}

func (b *ArugoConsumerBase) OnChannelCreated(channel *amqp.Channel) error {
	panic("not implemented")
}

func (b *ArugoConsumerBase) Start(yamlConfig string, detach bool) error {
	app := NewArugo(yamlConfig)
	b.BindArugoApp(app)
	err := app.AddConsumer(b.QueueName, b.I)
	if err != nil {
		return errors.Errorf("cannot add consumer: %s", err)
	}
	return app.Start(detach)
}

// 这里是关于主控 Arugo 的定义
type arugoConsumer struct {
	Consumer ArugoConsumerIf
	Key      string
	Alive    bool
}

type Arugo struct {
	ArugoConfig    *Config
	AmqpURI        string
	Consumers      map[string]*arugoConsumer
	ChannelQos     int8
	entryLock      sync.Mutex
	IsWorking      bool
	notifyExitChan chan error
}

func NewArugo(configFile string) *Arugo {
	config := getConfig(configFile)
	url := fmt.Sprintf("amqp://%s:%s@%s:%s/%s",
		config.RabbitMQCredential.Username,
		config.RabbitMQCredential.Password,
		config.RabbitMQConnection.Host,
		config.RabbitMQConnection.Port,
		config.RabbitMQConnection.VHost,
	)

	app := &Arugo{
		ArugoConfig:    config,
		AmqpURI:        url,
		Consumers:      make(map[string]*arugoConsumer),
		ChannelQos:     1,
		IsWorking:      false,
		notifyExitChan: make(chan error),
	}
	return app
}

func (app *Arugo) AddConsumer(key string, consumer ArugoConsumerIf) error {
	if _, ok := app.Consumers[key]; ok {
		return errors.New(fmt.Sprintf(
			"the key: %s is repeat for consumer.", key,
		))
	}
	app.Consumers[key] = &arugoConsumer{
		Consumer: consumer,
		Key:      key,
		Alive:    false,
	}
	return nil
}

func _initConsumer(conn *amqp.Connection, consumer ArugoConsumerIf) (*amqp.Channel, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, errors.Errorf("%s start channel error: %s", consumer.GetQueueName(), err)
	}

	if err := channel.Qos(1, 0, false); err != nil {
		return nil, errors.Errorf("%s set qos error: %s", consumer.GetQueueName(), err)
	}

	err = consumer.OnChannelCreated(channel)
	if err != nil {
		return nil, errors.Errorf("%s execute OnChannelCreated error: %s", consumer.GetQueueName(), err)
	}
	return channel, nil
}

func (app *Arugo) initConsumer(conn *amqp.Connection, consumer *arugoConsumer) (<-chan amqp.Delivery, error) {
	queueName := consumer.Consumer.GetQueueName()
	consumeConfig := consumer.Consumer.GetConsumeConfig()
	channel, err := _initConsumer(conn, consumer.Consumer)
	if err != nil {
		return nil, errors.Errorf("initialize consumer error: %s", err)
	}

	var consumeKey string
	if consumeConfig.ConsumeKey != "" {
		consumeKey = consumeConfig.ConsumeKey
	} else {
		consumeKey = consumer.Key
	}

	comingChan, err := channel.Consume(
		queueName, consumeKey,
		consumeConfig.NoAck, consumeConfig.Exclusive,
		consumeConfig.NoLocal, consumeConfig.NoWait,
		consumeConfig.Args,
	)
	if err != nil {
		return nil, errors.Errorf("consuming consumer: %s error: %s", consumer.Consumer.GetQueueName(), err)
	}
	return comingChan, nil
}

func (app *Arugo) consumeAMQPDeliveryChannal(consumer *arugoConsumer, comingChan <-chan amqp.Delivery) {
	for {
		select {
		case delivery, ok := <-comingChan:
			if !ok {
				return
			} else {
				err := consumer.Consumer.Handle(delivery)
				if err != nil {
					log.Println("handle message:", delivery, "err:", err)
				}
			}
		}
	}
}

func (app *Arugo) run() error {
	conn, err := amqp.Dial(app.AmqpURI)
	if err != nil {
		log.Printf("connection error with %s", err)
		log.Printf("trying to reconnect.")
		return err
	}
	defer conn.Close()

	log.Println("connection is open.")

	for key, consumer := range app.Consumers {
		log.Printf("the Consumer: %s is initialized...\n", key)
		comingChan, err := app.initConsumer(conn, consumer)
		if err != nil {
			log.Printf("initialize Consumer: %s err: %s\n", key, err)
			return err
		}

		if consumer.Alive {
			continue
		}
		go func() {
			consumer.Alive = true
			defer func() { consumer.Alive = false }()
			app.consumeAMQPDeliveryChannal(consumer, comingChan)
			log.Printf("the consumer: %s exited.", key)
		}()
	}

	ticker := time.Tick(1 * time.Second)
	for {
		select {
		case <-ticker:
			//log.Printf("checking wether all consumers is alive.")
			for key, consumer := range app.Consumers {
				if !consumer.Alive {
					return errors.New(fmt.Sprintf("Consumer: %s unexpectly exited.", key))
				}
			}
		case <-app.notifyExitChan:
			return &NormalExitError{}
		}
	}
}

func (app *Arugo) GetPublisher(retry int, confirm bool) (*ArugoPublisher, error) {
	publisher := &ArugoPublisher{
		config:  app.ArugoConfig,
		conn:    nil,
		ch:      nil,
		retry:   retry,
		confirm: confirm,
	}
	return publisher, nil
}

func (app *Arugo) Start(Detach bool) error {

	go func() {
		for {
			app.IsWorking = true
			err := app.run()
			app.IsWorking = false

			if _, ok := err.(NormalExitError); ok {
				log.Printf("Arugo met a normal exit signal.")
				break
			} else {
				log.Printf("Arugo met an error: %s, restarting...", err)
				time.Sleep(3 * time.Second)
			}

		}
	}()

	if Detach {
		return nil
	} else {
		ticker := time.Tick(1 * time.Second)
		for {
			select {
			case <-ticker:
			}
		}
		return nil
	}

}

func (app *Arugo) Stop() {
	app.notifyExitChan <- nil
}
