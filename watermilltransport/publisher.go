package watermilltransport

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-kit/kit/endpoint"
)

type (
	PublisherOption             func(*Publisher)
	PublisherEncodeRequestFunc  func(ctx context.Context, msg *message.Message, request interface{}) error
	PublisherDecodeResponseFunc func(ctx context.Context, msg *message.Message, publishResult error) (response interface{}, err error)
	PublisherBeforeFunc         func(ctx context.Context, msg *message.Message) context.Context
	PublisherAfterFunc          func(ctx context.Context, msg *message.Message, err error) context.Context
)
type Publisher struct {
	publisher message.Publisher
	topic     string
	enc       PublisherEncodeRequestFunc
	dec       PublisherDecodeResponseFunc
	before    []PublisherBeforeFunc
	after     []PublisherAfterFunc
	timeout   time.Duration
}

func NewPublisher(
	publisher message.Publisher,
	topic string,
	enc PublisherEncodeRequestFunc,
	dec PublisherDecodeResponseFunc,
	options ...PublisherOption,
) *Publisher {
	p := &Publisher{
		publisher: publisher,
		topic:     topic,
		enc:       enc,
		dec:       dec,
		timeout:   10 * time.Second,
	}
	for _, option := range options {
		option(p)
	}
	return p
}

func WithPublisherBefore(before ...PublisherBeforeFunc) PublisherOption {
	return func(p *Publisher) { p.before = append(p.before, before...) }
}

func WithPublisherAfter(after ...PublisherAfterFunc) PublisherOption {
	return func(p *Publisher) { p.after = append(p.after, after...) }
}

func WithPublisherTimeout(timeout time.Duration) PublisherOption {
	return func(p *Publisher) { p.timeout = timeout }
}

func (p Publisher) Endpoint() endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		ctx, cancel := context.WithTimeout(ctx, p.timeout)
		defer cancel()

		var msg message.Message
		if err := p.enc(ctx, &msg, request); err != nil {
			return nil, err
		}

		for _, f := range p.before {
			ctx = f(ctx, &msg)
		}

		err := p.publisher.Publish(p.topic, &msg)
		if err != nil {
			return nil, err
		}

		for _, f := range p.after {
			ctx = f(ctx, &msg, err)
		}

		response, err := p.dec(ctx, &msg, err)
		if err != nil {
			return nil, err
		}
		return response, nil
	}
}

func JSONEncodeRequestFunc(_ context.Context, msg *message.Message, request interface{}) error {
	switch payload := request.(type) {
	case []byte:
		msg.Payload = payload
		return nil
	case string:
		msg.Payload = []byte(payload)
		return nil
	default:
		jsonData, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		msg.Payload = jsonData
		return nil
	}
}
