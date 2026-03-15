package common

import (
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

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
