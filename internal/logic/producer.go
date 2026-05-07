package logic

import (
	"github.com/IBM/sarama"
)

// InitProducer 初始化Kafka同步生产者
func InitProducer(brokers []string, verson string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true // 同步生产者必须设置为true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll        // 强一致性：等待所有副本确认
	config.Producer.Partitioner = sarama.NewHashPartitioner // 根据Key(如ToUserID、groupID、roomID)投递到特定分区，实现会话内有序

	// 指定Kafka版本：
	v, err := sarama.ParseKafkaVersion(verson)
	if err != nil {
		return nil, err
	}

	config.Version = v

	return sarama.NewSyncProducer(brokers, config)
}
