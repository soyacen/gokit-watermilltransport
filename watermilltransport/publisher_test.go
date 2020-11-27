package watermilltransport

import (
	 "testing"
	"context"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

func createPublisher(brokers []string) (message.Publisher, error) {
	kafkaPublisher, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:               brokers,
			Marshaler:             kafka.DefaultMarshaler{},
			OverwriteSaramaConfig: kafka.DefaultSaramaSyncPublisherConfig(),
		},
		watermill.NewStdLogger(true, true),
	)
	if err != nil {
		return nil, err
	}
	return kafkaPublisher, nil
}

type Response struct {
	partition int32
	offset    int64
}

func ExampleDecodeResponseFuncfunc(ctx context.Context, msg *message.Message, publishResult error) (response interface{}, err error) {
	return nil, nil
}

func PublisherBefore(ctx context.Context, msg *message.Message) context.Context {
	fmt.Println("PublisherBefore set header")
	msg.Metadata = make(message.Metadata)
	msg.Metadata.Set("traceID", "123456678")
	return ctx
}

func PublisherAfter(ctx context.Context, msg *message.Message, err error) context.Context {
	fmt.Println("PublisherAfter ...")
	return ctx
}

func TestPublihser(t *testing.T) {
	brokers := []string{"10.18.1.77:9092", "10.18.1.78:9092", "10.18.1.79:9092"}
	producer, _ := createPublisher(brokers)
	publihser := NewPublisher(
		producer,
		"user",
		JSONEncodeRequestFunc,
		ExampleDecodeResponseFuncfunc,
		WithPublisherBefore(PublisherBefore),
		WithPublisherAfter(PublisherAfter),
		WithPublisherTimeout(time.Second),
	).Endpoint()
	for i := 0; i < 100; i++ {
		resp, err := publihser(context.Background(), User{Name: "jax", Age: i})
		if err != nil {
			panic(err)
		}
		fmt.Println(resp)
	}
}