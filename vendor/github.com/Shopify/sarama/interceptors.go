package sarama

// ProducerInterceptor allows you to intercept (and possibly mutate) the records
// received by the producer before they are published to the Kafka cluster.
// https://cwiki.apache.org/confluence/display/KAFKA/KIP-42%3A+Add+Producer+and+Consumer+Interceptors#KIP42:AddProducerandConsumerInterceptors-Motivation
type ProducerInterceptor interface {

	// OnSend is called when the producer message is intercepted. Please avoid
	// modifying the message until it's safe to do so, as this is _not_ a copy
	// of the message.
	OnSend(*ProducerMessage)
}

// ConsumerInterceptor allows you to intercept (and possibly mutate) the records
// received by the consumer before they are sent to the messages channel.
// https://cwiki.apache.org/confluence/display/KAFKA/KIP-42%3A+Add+Producer+and+Consumer+Interceptors#KIP42:AddProducerandConsumerInterceptors-Motivation
type ConsumerInterceptor interface {

	// OnConsume is called when the consumed message is intercepted. Please
	// avoid modifying the message until it's safe to do so, as this is _not_ a
	// copy of the message.
	OnConsume(*ConsumerMessage)
}

func (msg *ProducerMessage) safelyApplyInterceptor(interceptor ProducerInterceptor) {
	defer func() {
		if r := recover(); r != nil {
			Logger.Printf("Error when calling producer interceptor: %s, %w\n", interceptor, r)
		}
	}()

	interceptor.OnSend(msg)
}

func (msg *ConsumerMessage) safelyApplyInterceptor(interceptor ConsumerInterceptor) {
	defer func() {
		if r := recover(); r != nil {
			Logger.Printf("Error when calling consumer interceptor: %s, %w\n", interceptor, r)
		}
	}()

	interceptor.OnConsume(msg)
}
