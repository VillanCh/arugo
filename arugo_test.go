package arugo

import (
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"testing"
	"time"
)

//type ArugoConsumerIf interface {
//	GetConsumeConfig() *AmqpConsumeConfig
//	GetQueueName() string
//	OnChannelCreated(channel *amqp.Channel) error
//	Handle(req amqp.Delivery) error
//}
//type AmqpConsumeConfig struct {
//	ConsumeKey string
//	Exclusive  bool
//	NoAck      bool
//	NoLocal    bool
//	NoWait     bool
//	Args       amqp.Table
//}

var _checked bool

type MyConsumer struct {
	publisher *ArugoPublisher
}

func (c *MyConsumer) GetQueueName() string {
	return "arugo-test-queue"
}

func (c *MyConsumer) GetConsumeConfig() *AmqpConsumeConfig {
	return &AmqpConsumeConfig{
		ConsumeKey: c.GetQueueName(),
		Exclusive:  false,
		NoAck:      false,
		NoLocal:    false,
		NoWait:     false,
		Args:       nil,
	}
}

func (c *MyConsumer) OnChannelCreated(channel *amqp.Channel) error {
	err := channel.ExchangeDeclare(
		"arugo-test-exchange", "topic",
		false, false,
		false, false, nil,
	)
	if err != nil {
		return err
	}

	_, err = channel.QueueDeclare(
		c.GetQueueName(),
		false, false,
		false, false,
		nil,
	)
	if err != nil {
		return err
	}

	if err = channel.QueueBind(
		c.GetQueueName(),
		"arugo-key",
		"arugo-test-exchange",
		false, nil,
	); err != nil {
		return err
	}
	return nil
}

func (c *MyConsumer) Handle(delivery amqp.Delivery) error {
	defer func() {
		delivery.Ack(false)
	}()

	log.Println("recv message: ", string(delivery.Body))
	_checked = true

	return nil
}

func TestFunctions(t *testing.T) {
	app := NewArugo("config.yml")
	app.AddConsumer("test", &MyConsumer{})
	err := app.Start(true)
	if err != nil {
		t.Error(err)
		return
	}

	defer app.Stop()

	t.Log("三秒后开始测试发消息")
	time.Sleep(3 * time.Second)

	publisher, err := app.GetPublisher(5, true)
	if err != nil {
		t.Error(err)
		return
	}

	err = publisher.Publish("arugo-test-exchange", "arugo-key", false, false, amqp.Publishing{
		Body: []byte("testmessage"),
	})
	if err != nil {
		t.Error(err)
		return
	}

	t.Log("发送消息成功，2秒后测试收消息")

	time.Sleep(2 * time.Second)

	if _checked {
		t.Log("测试成功")
	} else {
		t.Error("没有收到消息")
	}
}
