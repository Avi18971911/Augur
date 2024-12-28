package event_bus

import (
	"encoding/json"
	"fmt"
	"github.com/asaskevich/EventBus"
	"go.uber.org/zap"
)

type AugurEventBus[InputType any, OutputType any] interface {
	Subscribe(topic string, handler func(input InputType) error, transactional bool) error
	Publish(topic string, arg OutputType) error
}

type AugurEventBusImpl[InputType any, OutputType any] struct {
	eventBus EventBus.Bus
	logger   *zap.Logger
}

func NewAugurEventBus[InputType any, OutputType any](
	eventBus EventBus.Bus,
	logger *zap.Logger,
) AugurEventBus[InputType, OutputType] {
	return &AugurEventBusImpl[InputType, OutputType]{
		eventBus: eventBus,
		logger:   logger,
	}
}

func (ev *AugurEventBusImpl[InputType, OutputType]) Subscribe(
	topic string,
	handler func(input InputType) error,
	transactional bool,
) error {
	err := ev.eventBus.SubscribeAsync(
		topic,
		func(arg string) {
			var input InputType
			err := json.Unmarshal([]byte(arg), &input)
			if err != nil {
				ev.logger.Error("Failed to unmarshal input during subscription of topic",
					zap.String("topic", topic),
					zap.Error(err),
				)
				return
			}
			err = handler(input)
			if err != nil {
				ev.logger.Error("Failed to handle input during subscription of topic",
					zap.String("topic", topic),
					zap.Error(err),
				)
			}
		},
		transactional,
	)
	if err != nil {
		return fmt.Errorf("failed to subscribe to topic %s: %w", topic, err)
	}
	return nil
}

func (ev *AugurEventBusImpl[InputType, OutputType]) Publish(
	topic string,
	arg OutputType,
) error {
	argBytes, err := json.Marshal(arg)
	if err != nil {
		return fmt.Errorf("failed to marshal output during publishing of topic %s: %w", topic, err)
	}
	ev.eventBus.Publish(topic, string(argBytes))
	return nil
}
