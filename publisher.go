package arugo

import (
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type ArugoPublisher struct {
	config *Config
	conn   *amqp.Connection
	ch     *amqp.Channel

	confirm bool
	retry   int
}

func (p *ArugoPublisher) buildConnection() (*amqp.Connection, error) {
	conn, err := amqp.Dial(p.config.GetAMQPUrl())
	if err != nil {
		return nil, err
	}
	return conn, err
}

func (p *ArugoPublisher) buildChannelFromConn(conn *amqp.Connection, confirm bool) (*amqp.Channel, error) {
	if conn == nil {
		return nil, errors.New("Empty MQ connection")
	}
	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	if confirm {
		channel.Confirm(false)
	}

	return channel, nil
}

func (p *ArugoPublisher) publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	var err error
	if p.conn == nil {
		p.conn, err = p.buildConnection()
		if err != nil {
			return err
		}
	}

	if p.ch == nil {
		p.ch, err = p.buildChannelFromConn(p.conn, p.confirm)
		if err != nil {
			p.conn = nil
			return err
		}
	}

	err = p.ch.Publish(exchange, key, mandatory, immediate, msg)
	if err != nil {
		return err
	}

	return nil
}

func (p *ArugoPublisher) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	count := 0
	for {
		err := p.publish(exchange, key, mandatory, immediate, msg)
		if err != nil {
			count += 1
		} else {
			return nil
		}

		if count > p.retry {
			return err
		}
	}
}
