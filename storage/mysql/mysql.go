package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/darkinno-tech/jtt-808-go-sdk/core"
	_ "github.com/go-sql-driver/mysql"
)

// MySQLStorage MySQL存储实现
type MySQLStorage struct {
	db *sql.DB
}

// Config MySQL配置
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

// NewMySQLStorage 创建MySQL存储
func NewMySQLStorage(config *Config) (*MySQLStorage, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		config.User, config.Password, config.Host, config.Port, config.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// 设置连接池参数
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(time.Hour)

	// 测试连接
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// 创建表
	if err := createTables(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	return &MySQLStorage{db: db}, nil
}

// createTables 创建表
func createTables(db *sql.DB) error {
	// 设备信息表
	deviceTable := `
	CREATE TABLE IF NOT EXISTS devices (
		id VARCHAR(64) PRIMARY KEY,
		province_id INT,
		city_id INT,
		manufacturer_id VARCHAR(32),
		terminal_type VARCHAR(64),
		terminal_id VARCHAR(32),
		plate_color TINYINT,
		plate_no VARCHAR(32),
		status TINYINT DEFAULT 0,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		INDEX idx_terminal_id (terminal_id),
		INDEX idx_plate_no (plate_no)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

	// 位置信息表
	locationTable := `
	CREATE TABLE IF NOT EXISTS locations (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		device_id VARCHAR(64) NOT NULL,
		alarm_flag INT UNSIGNED,
		status INT UNSIGNED,
		latitude DOUBLE,
		longitude DOUBLE,
		altitude INT,
		speed INT,
		direction INT,
		report_time TIMESTAMP,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		INDEX idx_device_id (device_id),
		INDEX idx_report_time (report_time)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

	// 报警信息表
	alarmTable := `
	CREATE TABLE IF NOT EXISTS alarms (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		device_id VARCHAR(64) NOT NULL,
		alarm_flag INT UNSIGNED,
		status INT UNSIGNED,
		latitude DOUBLE,
		longitude DOUBLE,
		altitude INT,
		speed INT,
		direction INT,
		alarm_type TINYINT,
		report_time TIMESTAMP,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		INDEX idx_device_id (device_id),
		INDEX idx_report_time (report_time)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

	tables := []string{deviceTable, locationTable, alarmTable}
	for _, table := range tables {
		if _, err := db.Exec(table); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	return nil
}

// SaveLocation 保存位置信息
func (s *MySQLStorage) SaveLocation(ctx context.Context, loc *core.LocationReport) error {
	if loc == nil {
		return fmt.Errorf("location is nil")
	}

	deviceID, _ := ctx.Value("deviceID").(string)
	if deviceID == "" {
		return fmt.Errorf("deviceID not found in context")
	}

	query := `INSERT INTO locations (device_id, alarm_flag, status, latitude, longitude, altitude, speed, direction, report_time)
			  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := s.db.ExecContext(ctx, query,
		deviceID, loc.AlarmFlag, loc.Status,
		loc.Latitude, loc.Longitude, loc.Altitude,
		loc.Speed, loc.Direction, loc.Time)

	return err
}

// GetLocations 获取位置信息
func (s *MySQLStorage) GetLocations(ctx context.Context, deviceID string, start, end time.Time) ([]*core.LocationReport, error) {
	query := `SELECT alarm_flag, status, latitude, longitude, altitude, speed, direction, report_time
			  FROM locations WHERE device_id = ? AND report_time BETWEEN ? AND ? ORDER BY report_time`

	rows, err := s.db.QueryContext(ctx, query, deviceID, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var locations []*core.LocationReport
	for rows.Next() {
		loc := &core.LocationReport{}
		err := rows.Scan(&loc.AlarmFlag, &loc.Status, &loc.Latitude, &loc.Longitude,
			&loc.Altitude, &loc.Speed, &loc.Direction, &loc.Time)
		if err != nil {
			return nil, err
		}
		locations = append(locations, loc)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return locations, nil
}

// SaveAlarm 保存报警信息
func (s *MySQLStorage) SaveAlarm(ctx context.Context, alarm *core.AlarmReport) error {
	if alarm == nil {
		return fmt.Errorf("alarm is nil")
	}

	deviceID, _ := ctx.Value("deviceID").(string)
	if deviceID == "" {
		return fmt.Errorf("deviceID not found in context")
	}

	query := `INSERT INTO alarms (device_id, alarm_flag, status, latitude, longitude, altitude, speed, direction, alarm_type, report_time)
			  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := s.db.ExecContext(ctx, query,
		deviceID, alarm.AlarmFlag, alarm.Status,
		alarm.Latitude, alarm.Longitude, alarm.Altitude,
		alarm.Speed, alarm.Direction, alarm.AlarmType, alarm.Time)

	return err
}

// GetAlarms 获取报警信息
func (s *MySQLStorage) GetAlarms(ctx context.Context, deviceID string, start, end time.Time) ([]*core.AlarmReport, error) {
	query := `SELECT alarm_flag, status, latitude, longitude, altitude, speed, direction, alarm_type, report_time
			  FROM alarms WHERE device_id = ? AND report_time BETWEEN ? AND ? ORDER BY report_time`

	rows, err := s.db.QueryContext(ctx, query, deviceID, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var alarms []*core.AlarmReport
	for rows.Next() {
		alarm := &core.AlarmReport{}
		err := rows.Scan(&alarm.AlarmFlag, &alarm.Status, &alarm.Latitude, &alarm.Longitude,
			&alarm.Altitude, &alarm.Speed, &alarm.Direction, &alarm.AlarmType, &alarm.Time)
		if err != nil {
			return nil, err
		}
		alarms = append(alarms, alarm)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return alarms, nil
}

// SaveDevice 保存设备信息
func (s *MySQLStorage) SaveDevice(ctx context.Context, deviceID string, info *core.TerminalRegister) error {
	if info == nil {
		return fmt.Errorf("device info is nil")
	}

	query := `INSERT INTO devices (id, province_id, city_id, manufacturer_id, terminal_type, terminal_id, plate_color, plate_no, status)
			  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
			  ON DUPLICATE KEY UPDATE
			  province_id = VALUES(province_id),
			  city_id = VALUES(city_id),
			  manufacturer_id = VALUES(manufacturer_id),
			  terminal_type = VALUES(terminal_type),
			  terminal_id = VALUES(terminal_id),
			  plate_color = VALUES(plate_color),
			  plate_no = VALUES(plate_no),
			  status = VALUES(status)`

	_, err := s.db.ExecContext(ctx, query,
		deviceID, info.ProvinceID, info.CityID,
		info.ManufacturerID, info.TerminalType, info.TerminalID,
		info.PlateColor, info.PlateNo, core.DeviceStatusOnline)

	return err
}

// GetDevice 获取设备信息
func (s *MySQLStorage) GetDevice(ctx context.Context, deviceID string) (*core.TerminalRegister, error) {
	query := `SELECT province_id, city_id, manufacturer_id, terminal_type, terminal_id, plate_color, plate_no
			  FROM devices WHERE id = ?`

	reg := &core.TerminalRegister{}
	err := s.db.QueryRowContext(ctx, query, deviceID).Scan(
		&reg.ProvinceID, &reg.CityID, &reg.ManufacturerID,
		&reg.TerminalType, &reg.TerminalID, &reg.PlateColor, &reg.PlateNo)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("device not found: %s", deviceID)
	}
	if err != nil {
		return nil, err
	}

	return reg, nil
}

// UpdateDeviceStatus 更新设备状态
func (s *MySQLStorage) UpdateDeviceStatus(ctx context.Context, deviceID string, status core.DeviceStatus) error {
	query := `UPDATE devices SET status = ? WHERE id = ?`
	_, err := s.db.ExecContext(ctx, query, status, deviceID)
	return err
}

// Close 关闭存储连接
func (s *MySQLStorage) Close() error {
	return s.db.Close()
}

// Ping 检查连接健康
func (s *MySQLStorage) Ping() error {
	return s.db.Ping()
}
