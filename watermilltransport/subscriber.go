package watermilltransport

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/transport"
)

type (
	SubscriberOption             func(s *Subscriber)
	SubscriberDecodeIncomingFunc func(ctx context.Context, msg *message.Message) (request interface{}, err error)
	SubscriberEncodeOutgoingFunc func(ctx context.Context, ack *bool, resp interface{}) error
	SubscriberBeforeFunc         func(ctx context.Context, msg *message.Message) context.Context
	SubscriberAfterFunc          func(ctx context.Context, msg *message.Message) context.Context
	SubscriberAcknowledger       func(ctx context.Context, msg *message.Message, ack bool) bool
	SubscriberErrorEncoder       func(ctx context.Context, msg *message.Message, err error)
)

type Subscriber struct {
	e            endpoint.Endpoint
	dec          SubscriberDecodeIncomingFunc
	enc          SubscriberEncodeOutgoingFunc
	before       []SubscriberBeforeFunc
	after        []SubscriberAfterFunc
	acknowledger SubscriberAcknowledger
	errorHandler transport.ErrorHandler
	errorEncoder SubscriberErrorEncoder
}

func NewSubscriber(
	endpoint endpoint.Endpoint,
	dec SubscriberDecodeIncomingFunc,
	enc SubscriberEncodeOutgoingFunc,
	options ...SubscriberOption) *Subscriber {
	consumer := &Subscriber{
		e:            endpoint,
		dec:          dec,
		enc:          enc,
		acknowledger: SubscriberDefaultAcknowledger,
		errorHandler: transport.NewLogErrorHandler(log.NewNopLogger()),
		errorEncoder: SubscriberDefaultErrorEncoder,
	}
	for _, option := range options {
		option(consumer)
	}
	return consumer
}

func WithSubscriberBefore(before ...SubscriberBeforeFunc) SubscriberOption {
	return func(s *Subscriber) { s.before = append(s.before, before...) }
}

func WithSubscriberAfter(after ...SubscriberAfterFunc) SubscriberOption {
	return func(s *Subscriber) { s.after = append(s.after, after...) }
}

func WithSubscriberAcknowledger(acknowledger SubscriberAcknowledger) SubscriberOption {
	return func(s *Subscriber) { s.acknowledger = acknowledger }
}

func WithSubscriberErrorHandler(errorHandler transport.ErrorHandler) SubscriberOption {
	return func(s *Subscriber) { s.errorHandler = errorHandler }
}

func WithSubscriberErrorEncoder(errorEncoder SubscriberErrorEncoder) SubscriberOption {
	return func(s *Subscriber) { s.errorEncoder = errorEncoder }
}

func (s *Subscriber) ServeMsg(msgC <-chan *message.Message) {
	for msg := range msgC {
		s.handleMsg(msg)
	}
}

func (s *Subscriber) handleMsg(msg *message.Message) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, f := range s.before {
		ctx = f(ctx, msg)
	}
	request, err := s.dec(ctx, msg)
	if err != nil {
		s.errorHandler.Handle(ctx, err)
		s.errorEncoder(ctx, msg, err)
		return
	}
	response, err := s.e(ctx, request)
	if err != nil {
		s.errorHandler.Handle(ctx, err)
		s.errorEncoder(ctx, msg, err)
		return
	}
	for _, f := range s.after {
		ctx = f(ctx, msg)
	}

	var ack bool
	if err := s.enc(ctx, &ack, response); err != nil {
		s.errorHandler.Handle(ctx, err)
		s.errorEncoder(ctx, msg, err)
		return
	}

	if ok := s.acknowledger(ctx, msg, ack); !ok {
		err := fmt.Errorf("acknowledge result is %v", ok)
		s.errorHandler.Handle(ctx, err)
		s.errorEncoder(ctx, msg, err)
		return
	}
}

func SubscriberDefaultAcknowledger(ctx context.Context, msg *message.Message, ack bool) bool {
	if ack {
		return msg.Ack()
	}
	return msg.Nack()
}

func SubscriberDefaultErrorEncoder(ctx context.Context, msg *message.Message, err error) {

}
