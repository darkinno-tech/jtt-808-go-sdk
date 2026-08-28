package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/im10furry/jtt-808-go-sdk/core"
	_ "github.com/lib/pq"
)

// PostgresStorage PostgreSQL存储实现
type PostgresStorage struct {
	db *sql.DB
}

// Config PostgreSQL配置
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string
}

// NewPostgresStorage 创建PostgreSQL存储
func NewPostgresStorage(config *Config) (*PostgresStorage, error) {
	if config.SSLMode == "" {
		config.SSLMode = "disable"
	}

	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.Host, config.Port, config.User, config.Password, config.Database, config.SSLMode)

	db, err := sql.Open("postgres", dsn)
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

	return &PostgresStorage{db: db}, nil
}

// createTables 创建表
func createTables(db *sql.DB) error {
	// 设备信息表
	deviceTable := `
	CREATE TABLE IF NOT EXISTS devices (
		id VARCHAR(64) PRIMARY KEY,
		province_id INTEGER,
		city_id INTEGER,
		manufacturer_id VARCHAR(32),
		terminal_type VARCHAR(64),
		terminal_id VARCHAR(32),
		plate_color SMALLINT,
		plate_no VARCHAR(32),
		status SMALLINT DEFAULT 0,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_devices_terminal_id ON devices(terminal_id);
	CREATE INDEX IF NOT EXISTS idx_devices_plate_no ON devices(plate_no);`

	// 位置信息表
	locationTable := `
	CREATE TABLE IF NOT EXISTS locations (
		id BIGSERIAL PRIMARY KEY,
		device_id VARCHAR(64) NOT NULL,
		alarm_flag BIGINT,
		status BIGINT,
		latitude DOUBLE PRECISION,
		longitude DOUBLE PRECISION,
		altitude INTEGER,
		speed INTEGER,
		direction INTEGER,
		report_time TIMESTAMP,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_locations_device_id ON locations(device_id);
	CREATE INDEX IF NOT EXISTS idx_locations_report_time ON locations(report_time);`

	// 报警信息表
	alarmTable := `
	CREATE TABLE IF NOT EXISTS alarms (
		id BIGSERIAL PRIMARY KEY,
		device_id VARCHAR(64) NOT NULL,
		alarm_flag BIGINT,
		status BIGINT,
		latitude DOUBLE PRECISION,
		longitude DOUBLE PRECISION,
		altitude INTEGER,
		speed INTEGER,
		direction INTEGER,
		alarm_type SMALLINT,
		report_time TIMESTAMP,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_alarms_device_id ON alarms(device_id);
	CREATE INDEX IF NOT EXISTS idx_alarms_report_time ON alarms(report_time);`

	tables := []string{deviceTable, locationTable, alarmTable}
	for _, table := range tables {
		if _, err := db.Exec(table); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	return nil
}

// SaveLocation 保存位置信息
func (s *PostgresStorage) SaveLocation(ctx context.Context, loc *core.LocationReport) error {
	if loc == nil {
		return fmt.Errorf("location is nil")
	}

	deviceID, _ := ctx.Value("deviceID").(string)
	if deviceID == "" {
		return fmt.Errorf("deviceID not found in context")
	}

	query := `INSERT INTO locations (device_id, alarm_flag, status, latitude, longitude, altitude, speed, direction, report_time)
			  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

	_, err := s.db.ExecContext(ctx, query,
		deviceID, loc.AlarmFlag, loc.Status,
		loc.Latitude, loc.Longitude, loc.Altitude,
		loc.Speed, loc.Direction, loc.Time)

	return err
}

// GetLocations 获取位置信息
func (s *PostgresStorage) GetLocations(ctx context.Context, deviceID string, start, end time.Time) ([]*core.LocationReport, error) {
	query := `SELECT alarm_flag, status, latitude, longitude, altitude, speed, direction, report_time
			  FROM locations WHERE device_id = $1 AND report_time BETWEEN $2 AND $3 ORDER BY report_time`

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
func (s *PostgresStorage) SaveAlarm(ctx context.Context, alarm *core.AlarmReport) error {
	if alarm == nil {
		return fmt.Errorf("alarm is nil")
	}

	deviceID, _ := ctx.Value("deviceID").(string)
	if deviceID == "" {
		return fmt.Errorf("deviceID not found in context")
	}

	query := `INSERT INTO alarms (device_id, alarm_flag, status, latitude, longitude, altitude, speed, direction, alarm_type, report_time)
			  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`

	_, err := s.db.ExecContext(ctx, query,
		deviceID, alarm.AlarmFlag, alarm.Status,
		alarm.Latitude, alarm.Longitude, alarm.Altitude,
		alarm.Speed, alarm.Direction, alarm.AlarmType, alarm.Time)

	return err
}

// GetAlarms 获取报警信息
func (s *PostgresStorage) GetAlarms(ctx context.Context, deviceID string, start, end time.Time) ([]*core.AlarmReport, error) {
	query := `SELECT alarm_flag, status, latitude, longitude, altitude, speed, direction, alarm_type, report_time
			  FROM alarms WHERE device_id = $1 AND report_time BETWEEN $2 AND $3 ORDER BY report_time`

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
func (s *PostgresStorage) SaveDevice(ctx context.Context, deviceID string, info *core.TerminalRegister) error {
	if info == nil {
		return fmt.Errorf("device info is nil")
	}

	query := `INSERT INTO devices (id, province_id, city_id, manufacturer_id, terminal_type, terminal_id, plate_color, plate_no, status)
			  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			  ON CONFLICT (id) DO UPDATE SET
			  province_id = EXCLUDED.province_id,
			  city_id = EXCLUDED.city_id,
			  manufacturer_id = EXCLUDED.manufacturer_id,
			  terminal_type = EXCLUDED.terminal_type,
			  terminal_id = EXCLUDED.terminal_id,
			  plate_color = EXCLUDED.plate_color,
			  plate_no = EXCLUDED.plate_no,
			  status = EXCLUDED.status`

	_, err := s.db.ExecContext(ctx, query,
		deviceID, info.ProvinceID, info.CityID,
		info.ManufacturerID, info.TerminalType, info.TerminalID,
		info.PlateColor, info.PlateNo, core.DeviceStatusOnline)

	return err
}

// GetDevice 获取设备信息
func (s *PostgresStorage) GetDevice(ctx context.Context, deviceID string) (*core.TerminalRegister, error) {
	query := `SELECT province_id, city_id, manufacturer_id, terminal_type, terminal_id, plate_color, plate_no
			  FROM devices WHERE id = $1`

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
func (s *PostgresStorage) UpdateDeviceStatus(ctx context.Context, deviceID string, status core.DeviceStatus) error {
	query := `UPDATE devices SET status = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2`
	_, err := s.db.ExecContext(ctx, query, status, deviceID)
	return err
}

// Close 关闭存储连接
func (s *PostgresStorage) Close() error {
	return s.db.Close()
}

// Ping 检查连接健康
func (s *PostgresStorage) Ping() error {
	return s.db.Ping()
}
