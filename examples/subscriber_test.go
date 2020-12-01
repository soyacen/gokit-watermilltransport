package examples

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-kit/kit/endpoint"
	"../watermilltransport"
)

func ExampleSubscriber() {

}

type User struct {
	Name string
	Age  int
}

type Service interface {
	Login(ctx context.Context, name string, age int) error
}

type basicService struct {
}

func (basicService) Login(ctx context.Context, name string, age int) error {
	log.Printf("login %s @ %d \n", name, age)
	return nil
}

func newKafkaSubscriber(consumerGroup string, brokers []string) (*kafka.Subscriber, error) {
	logger := &watermill.NopLogger{}

	var err error
	saramaConfig := kafka.DefaultSaramaSubscriberConfig()
	saramaConfig.Consumer.Offsets.Initial =  -2
	var subscriber *kafka.Subscriber

	subscriber, err = kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:               brokers,
			Unmarshaler:           &kafka.DefaultMarshaler{},
			OverwriteSaramaConfig: saramaConfig,
			ConsumerGroup:         consumerGroup,
			InitializeTopicDetails: &sarama.TopicDetail{
				NumPartitions:     8,
				ReplicationFactor: 1,
			},
		},
		logger,
	)
	if err != nil {
		return nil, err
	}
	return subscriber, nil
}

func TestNewSubscriber(t *testing.T) {
	brokers := []string{"10.18.1.77:9092", "10.18.1.78:9092", "10.18.1.79:9092"}
	watermillKafka, _ := newKafkaSubscriber("testuser1", brokers)
	handler := watermilltransport.NewSubscriber(
		makeEndpoint(&basicService{}),
		ExampleSubscriberDecodeRequestFunc,
		ExampleSubscriberEncodeResponseFunc,
		watermilltransport.WithSubscriberBefore(func(ctx context.Context, msg *message.Message) context.Context {
			fmt.Println("before func")
			return ctx
		}),
		watermilltransport.WithSubscriberAfter(func(ctx context.Context, msg *message.Message) context.Context {
			fmt.Println("after func")
			return ctx
		}),
		watermilltransport.WithSubscriberErrorEncoder(func(ctx context.Context, msg *message.Message, err error) {
			fmt.Println("error encoder", err)
		}),
	)
	msgs, _ := watermillKafka.Subscribe(context.Background(), "user")
	handler.ServeMsg(msgs)
}

func makeEndpoint(s Service) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*User)
		err = s.Login(ctx, req.Name, req.Age)
		if err != nil {
			return false, err
		}
		return true, nil
	}
}

func ExampleSubscriberDecodeRequestFunc(c context.Context, s *message.Message) (request interface{}, err error) {
  	value := []byte(s.Payload)
	log.Printf("%s", value)
	user := User{}
	err = json.Unmarshal(value, &user)
	if err != nil {
		return nil, err
	}
	return &user, nil
}

func ExampleSubscriberEncodeResponseFunc(ctx context.Context, ack *bool, resp interface{}) error {
	success, ok := resp.(bool)
	if !ok {
		return errors.New("unknown response type")
	}
	*ack = success
	return nil
}
