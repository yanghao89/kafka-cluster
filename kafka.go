package kafka_cluster

import (
	"log"
	"sync"

	"github.com/Shopify/sarama"
)

type KafkaClient struct {
	//异步生产者
	asyncProducer sarama.AsyncProducer
	//同步生产者
	syncProducer sarama.SyncProducer
	//
	consumer      sarama.Consumer
	consumerGroup sarama.ConsumerGroup
}

var (
	kafkaAsyncProducer sarama.AsyncProducer
	kafkaSyncProducer  sarama.SyncProducer
	kafkaConsumer      sarama.Consumer
	kafkaconsumerGroup sarama.ConsumerGroup
)

func NewKafkaClient() *KafkaClient {
	return &KafkaClient{
		asyncProducer: kafkaAsyncProducer,
		syncProducer:  kafkaSyncProducer,
		consumer:      kafkaConsumer,
		consumerGroup: kafkaconsumerGroup,
	}
}

var (
	asyncMutex    sync.Mutex
	syncMutex     sync.Mutex
	consumerMutex sync.Mutex
)

func initAsyncProducer(kafkaCluster []string, conf *sarama.Config) sarama.AsyncProducer {
	if kafkaAsyncProducer == nil {
		asyncMutex.Lock()
		defer asyncMutex.Unlock()
		if kafkaAsyncProducer == nil {
			asyncProducer, err := sarama.NewAsyncProducer(kafkaCluster, conf)
			if err != nil {
				log.Fatalf("kafka connect error:%v", err)
			}
			kafkaAsyncProducer = asyncProducer
		}
	}
	return kafkaAsyncProducer
}

func initSyncProducer(kafkaCluster []string, conf *sarama.Config) sarama.SyncProducer {
	if kafkaSyncProducer == nil {
		syncMutex.Lock()
		defer syncMutex.Unlock()
		if kafkaSyncProducer == nil {
			syncProducer, err := sarama.NewSyncProducer(kafkaCluster, conf)
			if err != nil {
				log.Fatalf("kafka connect error:%v", err)
			}
			kafkaSyncProducer = syncProducer
		}
	}
	return kafkaSyncProducer
}

func InitProducerKafka(kafkaCluster []string, conf *sarama.Config) {
	conf.Producer.Return.Successes = true
	conf.Producer.Return.Errors = true
	//初始化异步Producer
	initAsyncProducer(kafkaCluster, conf)
	//初始化同步Producer
	initSyncProducer(kafkaCluster, conf)
}

func initKafkaConsumer(kafkaCluster []string, conf *sarama.Config) sarama.Consumer {
	if kafkaConsumer == nil {
		consumerMutex.Lock()
		defer consumerMutex.Unlock()
		if kafkaConsumer == nil {
			syncConsumer, err := sarama.NewConsumer(kafkaCluster, conf)
			if err != nil {
				log.Fatalf("kafka connect error:%v", err)
			}
			kafkaConsumer = syncConsumer
		}
	}
	return kafkaConsumer
}

func InitConsumer(kafkaCluster []string, conf *sarama.Config) {
	// 返回错误
	conf.Consumer.Return.Errors = true
	// 默认从最新的offsets开始获取
	conf.Consumer.Offsets.Initial = sarama.OffsetNewest

	initKafkaConsumer(kafkaCluster, conf)
}

func (kc *KafkaClient) AsyncSend(topic string, msg string) (partition int32, offset int64, err error) {
	p := kc.asyncProducer
	p.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(msg),
	}
	select {
	case sur := <-p.Successes():
		partition, offset, err = sur.Partition, sur.Offset, nil
	case fail := <-p.Errors():
		partition, offset, err = 0, 0, fail.Err
	}
	return
}

func (kc *KafkaClient) SyncSend(topic, msg string) (partition int32, offset int64, err error) {
	partition, offset, err = kc.syncProducer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(msg),
	})
	return
}

func (kc *KafkaClient) CloseProducer() {
	kc.asyncProducer.Close()
	kc.syncProducer.Close()
}
