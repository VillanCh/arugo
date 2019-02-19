package arugo

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
)

type Config struct {
	RabbitMQConnection RabbitMQConnectionConfig `yaml:"rabbit_connection"`
	RabbitMQCredential RabbitMQCredentialConfig `yaml:"rabbit_credentials"`
}

func (c *Config) GetAMQPUrl() string {
	var proto string
	if c.RabbitMQConnection.SSL {
		proto = "amqps"
	} else {
		proto = "amqp"
	}

	return fmt.Sprintf("%s://%s:%s@%s:%s/%s", proto, c.RabbitMQCredential.Username,
		c.RabbitMQCredential.Password, c.RabbitMQConnection.Host, c.RabbitMQConnection.Port, c.RabbitMQConnection.VHost)
}

type RabbitMQConnectionConfig struct {
	Host       string             `yaml:"host"`
	Port       string             `yaml:"port"`
	VHost      string             `yaml:"virtual_host"`
	SSL        bool               `yaml:"ssl"`
	SSLOptions RabbitMQSSLOptions `yaml:"ssl_options"`
}

type RabbitMQSSLOptions struct {
	Certfile string `yaml:"certfile"`
	Keyfile  string `yaml:"keyfile"`
	CACerts  string `yaml:"ca_certs"`
}

type RabbitMQCredentialConfig struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

func replaceByEnv(raw string) string {
	reg, _ := regexp.Compile("\\${([a-zA-Z_0-9-]+)(:(.*))?}")
	paramsAll := reg.FindAllStringSubmatch(raw, -1)

	var current string
	current = raw
	for _, params := range paramsAll {
		envVal := os.Getenv(params[1])
		origin := params[0]

		var newVal string

		if envVal != "" {
			newVal = envVal
		} else {
			newVal = params[3]
		}
		current = strings.Replace(current, origin, newVal, -1)
	}
	return current
}

func getConfig(name string) *Config {
	confContent, err := ioutil.ReadFile(name)
	if err != nil {
		panic(err)
	}

	configRaw := string(confContent)
	config := replaceByEnv(configRaw)

	// expand environment variables
	confContent = []byte(config)
	conf := &Config{}
	if err := yaml.Unmarshal(confContent, conf); err != nil {
		panic(err)
	}
	return conf
}
