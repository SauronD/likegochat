package task

import "time"

type Message struct {
	ID          int64     `gorm:"column:id;primaryKey;autoIncrement"`
	MsgID       int64     `gorm:"column:msg_id;not null"`
	ClientMSGID int64     `gorm:"column:client_msg_id;not null"`
	FromUserID  int64     `gorm:"column:from_user_id;not null"`
	ToUserID    int64     `gorm:"column:to_user_id;not null"`
	Content     string    `gorm:"column:content;type:text;not null"`
	MsgType     int32     `gorm:"column:msg_type;not null;default:1"`
	Status      int32     `gorm:"column:status;not null;default:0"`
	CreateTime  time.Time `gorm:"column:created_at;not null;default:CURRENT_TIMESTAMP;autoCreateTime"`
	UpdateTime  time.Time `gorm:"column:updated_at;not null;default:CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;autoUpdateTime"`
}

func (m Message) TableName() string {
	return "messages"
}
