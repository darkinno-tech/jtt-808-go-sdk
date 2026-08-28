package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/darkinno-tech/jtt-808-go-sdk/core"
	"github.com/go-redis/redis/v8"
)

// RedisStorage Redis存储实现
type RedisStorage struct {
	client *redis.Client
}

// Config Redis配置
type Config struct {
	Host     string
	Port     int
	Password string
	DB       int
}

// NewRedisStorage 创建Redis存储
func NewRedisStorage(config *Config) (*RedisStorage, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", config.Host, config.Port),
		Password: config.Password,
		DB:       config.DB,
	})

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	return &RedisStorage{client: client}, nil
}

// SaveLocation 保存位置信息
func (s *RedisStorage) SaveLocation(ctx context.Context, loc *core.LocationReport) error {
	if loc == nil {
		return fmt.Errorf("location is nil")
	}

	deviceID, _ := ctx.Value("deviceID").(string)
	if deviceID == "" {
		return fmt.Errorf("deviceID not found in context")
	}

	// 序列化位置信息
	data, err := json.Marshal(loc)
	if err != nil {
		return err
	}

	// 保存到有序集合，按时间排序
	key := fmt.Sprintf("location:%s", deviceID)
	score := float64(loc.Time.Unix())

	pipe := s.client.Pipeline()
	pipe.ZAdd(ctx, key, &redis.Z{
		Score:  score,
		Member: string(data),
	})
	// 限制每个设备最多保存1000条位置信息
	pipe.ZRemRangeByRank(ctx, key, 0, -1001)
	// 设置过期时间（7天）
	pipe.Expire(ctx, key, 7*24*time.Hour)

	_, err = pipe.Exec(ctx)
	return err
}

// GetLocations 获取位置信息
func (s *RedisStorage) GetLocations(ctx context.Context, deviceID string, start, end time.Time) ([]*core.LocationReport, error) {
	key := fmt.Sprintf("location:%s", deviceID)

	// 按时间范围查询
	opt := &redis.ZRangeBy{
		Min: fmt.Sprintf("%d", start.Unix()),
		Max: fmt.Sprintf("%d", end.Unix()),
	}

	results, err := s.client.ZRangeByScore(ctx, key, opt).Result()
	if err != nil {
		return nil, err
	}

	var locations []*core.LocationReport
	for _, result := range results {
		loc := &core.LocationReport{}
		if err := json.Unmarshal([]byte(result), loc); err != nil {
			return nil, err
		}
		locations = append(locations, loc)
	}

	return locations, nil
}

// SaveAlarm 保存报警信息
func (s *RedisStorage) SaveAlarm(ctx context.Context, alarm *core.AlarmReport) error {
	if alarm == nil {
		return fmt.Errorf("alarm is nil")
	}

	deviceID, _ := ctx.Value("deviceID").(string)
	if deviceID == "" {
		return fmt.Errorf("deviceID not found in context")
	}

	// 序列化报警信息
	data, err := json.Marshal(alarm)
	if err != nil {
		return err
	}

	// 保存到有序集合，按时间排序
	key := fmt.Sprintf("alarm:%s", deviceID)
	score := float64(alarm.Time.Unix())

	pipe := s.client.Pipeline()
	pipe.ZAdd(ctx, key, &redis.Z{
		Score:  score,
		Member: string(data),
	})
	// 限制每个设备最多保存1000条报警信息
	pipe.ZRemRangeByRank(ctx, key, 0, -1001)
	// 设置过期时间（30天）
	pipe.Expire(ctx, key, 30*24*time.Hour)

	_, err = pipe.Exec(ctx)
	return err
}

// GetAlarms 获取报警信息
func (s *RedisStorage) GetAlarms(ctx context.Context, deviceID string, start, end time.Time) ([]*core.AlarmReport, error) {
	key := fmt.Sprintf("alarm:%s", deviceID)

	// 按时间范围查询
	opt := &redis.ZRangeBy{
		Min: fmt.Sprintf("%d", start.Unix()),
		Max: fmt.Sprintf("%d", end.Unix()),
	}

	results, err := s.client.ZRangeByScore(ctx, key, opt).Result()
	if err != nil {
		return nil, err
	}

	var alarms []*core.AlarmReport
	for _, result := range results {
		alarm := &core.AlarmReport{}
		if err := json.Unmarshal([]byte(result), alarm); err != nil {
			return nil, err
		}
		alarms = append(alarms, alarm)
	}

	return alarms, nil
}

// SaveDevice 保存设备信息
func (s *RedisStorage) SaveDevice(ctx context.Context, deviceID string, info *core.TerminalRegister) error {
	if info == nil {
		return fmt.Errorf("device info is nil")
	}

	data, err := json.Marshal(info)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("device:%s", deviceID)
	return s.client.Set(ctx, key, string(data), 0).Err()
}

// GetDevice 获取设备信息
func (s *RedisStorage) GetDevice(ctx context.Context, deviceID string) (*core.TerminalRegister, error) {
	key := fmt.Sprintf("device:%s", deviceID)

	data, err := s.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, fmt.Errorf("device not found: %s", deviceID)
	}
	if err != nil {
		return nil, err
	}

	reg := &core.TerminalRegister{}
	if err := json.Unmarshal([]byte(data), reg); err != nil {
		return nil, err
	}

	return reg, nil
}

// UpdateDeviceStatus 更新设备状态
func (s *RedisStorage) UpdateDeviceStatus(ctx context.Context, deviceID string, status core.DeviceStatus) error {
	key := fmt.Sprintf("device_status:%s", deviceID)
	return s.client.Set(ctx, key, int(status), 0).Err()
}

// Close 关闭存储连接
func (s *RedisStorage) Close() error {
	return s.client.Close()
}

// Ping 检查连接健康
func (s *RedisStorage) Ping() error {
	return s.client.Ping(context.Background()).Err()
}
