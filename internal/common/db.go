package common

import (
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

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

type GroupMessage struct {
	ID          int64     `gorm:"column:id;primaryKey;autoIncrement"`
	MsgID       int64     `gorm:"column:msg_id;not null"`
	ClientMSGID int64     `gorm:"column:client_msg_id;not null"`
	FromUserID  int64     `gorm:"column:from_user_id;not null"`
	GroupID     int64     `gorm:"column:group_id;not null"`
	Content     string    `gorm:"column:content;type:text;not null"`
	MsgType     int32     `gorm:"column:msg_type;not null;default:1"`
	Status      int32     `gorm:"column:status;not null;default:0"`
	CreateTime  time.Time `gorm:"column:created_at;not null;default:CURRENT_TIMESTAMP;autoCreateTime"`
	UpdateTime  time.Time `gorm:"column:updated_at;not null;default:CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;autoUpdateTime"`
}

type Group struct {
	ID          int64      `gorm:"column:id;primaryKey;autoIncrement"`
	Name        string     `gorm:"column:group_name;not null"`
	Status      int32      `gorm:"column:group_status;not null;default:0"`
	OwnerUserID int64      `gorm:"column:owner_user_id;not null"`
	MemberCount int32      `gorm:"column:member_count;not null;default:1"`
	CreatedAt   *time.Time `gorm:"column:created_at;not null;default:CURRENT_TIMESTAMP;autoCreateTime"`
	UpdatedAt   *time.Time `gorm:"column:updated_at;not null;default:CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;autoUpdateTime"`
}

type GroupMember struct {
	ID         int64      `gorm:"column:id;primaryKey;autoIncrement"`
	GroupID    int64      `gorm:"column:group_id;not null"`
	UserID     int64      `gorm:"column:user_id;not null"`
	UserRole   int32      `gorm:"column:user_role;not null;default:0"`
	UserStatus int32      `gorm:"column:user_status;not null;default:0"`
	JoinAt     *time.Time `gorm:"column:joined_at;not null;default:CURRENT_TIMESTAMP;autoCreateTime"`
	UpdateAt   *time.Time `gorm:"column:updated_at;not null;default:CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;autoUpdateTime"`
}

func (m Message) TableName() string {
	return "messages"
}

func (Group) TableName() string {
	return "groups"
}
func (GroupMember) TableName() string {
	return "group_members"
}

type User struct {
	ID           int64     `gorm:"column:id;primaryKey;autoIncrement"`
	Username     string    `gorm:"column:username"`
	PasswordHash string    `gorm:"column:password_hash"`
	CreatTime    time.Time `gorm:"column:created_at;autoCreateTime"`
}

func (User) TableName() string {
	return "users"
}

type UserSession struct {
	ID        int64  `gorm:"column:id;primaryKey;autoIncrement"`
	UserID    int64  `gorm:"column:user_id"`
	SessionID string `gorm:"column:session_id"`
	// 登录时间，注意不能设置为指针，因为gorm插入时会换成NULL，和数据库NOT NULL冲突
	IssuedAt *time.Time `gorm:"column:issued_at;autoCreateTime"`
	// session过期时间
	ExpiresAt time.Time `gorm:"column:expires_at"`
	// session撤销时间，在数据库中可以为NULL，因此需要用指针来区别
	RevokedAt *time.Time `gorm:"column:revoked_at"`
	IP        string     `gorm:"column:ip"`
	UserAgent string     `gorm:"column:user_agent"`
}

// gorm结构体绑定表的方法：实现TableName()方法
func (UserSession) TableName() string {
	return "user_sessions"
}

// 返回*gorm.DB
func OpenMySQL(dsn string) (*gorm.DB, error) {
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		// 注册到日志
		Logger: &ZapGormLogger{
			LogLevel:                  gormlogger.Info, // Info级别会打印所有SQL
			SlowThreshold:             200 * time.Millisecond,
			IgnoreRecordNotFoundError: true,
		},
	})
	if err != nil {
		return nil, err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	// 配置连接池：最大连接数10、空闲连接池连接最大数量5、重新使用连接的最大时间30min
	sqlDB.SetMaxOpenConns(10)
	sqlDB.SetMaxIdleConns(5)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)

	return db, nil
}
