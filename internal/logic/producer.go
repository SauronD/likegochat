package logic

import (
	"github.com/IBM/sarama"
)

// InitProducer 初始化 Kafka 同步生产者
func InitProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true // 同步生产者必须设置为 true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll        // 强一致性：等待所有副本确认
	config.Producer.Partitioner = sarama.NewHashPartitioner // 根据 Key (如 ToUserID) 投递到特定分区

	// 显式指定版本（与 Task 层保持一致）
	config.Version = sarama.V2_8_0_0

	return sarama.NewSyncProducer(brokers, config)
}
