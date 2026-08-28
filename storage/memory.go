package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/im10furry/jtt-808-go-sdk/core"
)

// MemoryStorage 内存存储实现
type MemoryStorage struct {
	mu        sync.RWMutex
	devices   map[string]*core.TerminalRegister
	status    map[string]core.DeviceStatus
	locations map[string][]*core.LocationReport
	alarms    map[string][]*core.AlarmReport
}

// NewMemoryStorage 创建内存存储
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		devices:   make(map[string]*core.TerminalRegister),
		status:    make(map[string]core.DeviceStatus),
		locations: make(map[string][]*core.LocationReport),
		alarms:    make(map[string][]*core.AlarmReport),
	}
}

// SaveLocation 保存位置信息
func (s *MemoryStorage) SaveLocation(ctx context.Context, loc *core.LocationReport) error {
	if loc == nil {
		return fmt.Errorf("location is nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// 从context中获取设备ID
	deviceID, _ := ctx.Value("deviceID").(string)
	if deviceID == "" {
		return fmt.Errorf("deviceID not found in context")
	}

	s.locations[deviceID] = append(s.locations[deviceID], loc)

	// 限制每个设备最多保存1000条位置信息
	if len(s.locations[deviceID]) > 1000 {
		s.locations[deviceID] = s.locations[deviceID][len(s.locations[deviceID])-1000:]
	}

	return nil
}

// GetLocations 获取位置信息
func (s *MemoryStorage) GetLocations(ctx context.Context, deviceID string, start, end time.Time) ([]*core.LocationReport, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	locations, exists := s.locations[deviceID]
	if !exists {
		return nil, nil
	}

	var result []*core.LocationReport
	for _, loc := range locations {
		if !loc.Time.Before(start) && !loc.Time.After(end) {
			result = append(result, loc)
		}
	}

	return result, nil
}

// SaveAlarm 保存报警信息
func (s *MemoryStorage) SaveAlarm(ctx context.Context, alarm *core.AlarmReport) error {
	if alarm == nil {
		return fmt.Errorf("alarm is nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	deviceID, _ := ctx.Value("deviceID").(string)
	if deviceID == "" {
		return fmt.Errorf("deviceID not found in context")
	}

	s.alarms[deviceID] = append(s.alarms[deviceID], alarm)

	// 限制每个设备最多保存1000条报警信息
	if len(s.alarms[deviceID]) > 1000 {
		s.alarms[deviceID] = s.alarms[deviceID][len(s.alarms[deviceID])-1000:]
	}

	return nil
}

// GetAlarms 获取报警信息
func (s *MemoryStorage) GetAlarms(ctx context.Context, deviceID string, start, end time.Time) ([]*core.AlarmReport, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	alarms, exists := s.alarms[deviceID]
	if !exists {
		return nil, nil
	}

	var result []*core.AlarmReport
	for _, alarm := range alarms {
		if !alarm.Time.Before(start) && !alarm.Time.After(end) {
			result = append(result, alarm)
		}
	}

	return result, nil
}

// SaveDevice 保存设备信息
func (s *MemoryStorage) SaveDevice(ctx context.Context, deviceID string, info *core.TerminalRegister) error {
	if info == nil {
		return fmt.Errorf("device info is nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.devices[deviceID] = info
	s.status[deviceID] = core.DeviceStatusOnline

	return nil
}

// GetDevice 获取设备信息
func (s *MemoryStorage) GetDevice(ctx context.Context, deviceID string) (*core.TerminalRegister, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	device, exists := s.devices[deviceID]
	if !exists {
		return nil, fmt.Errorf("device not found: %s", deviceID)
	}

	return device, nil
}

// UpdateDeviceStatus 更新设备状态
func (s *MemoryStorage) UpdateDeviceStatus(ctx context.Context, deviceID string, status core.DeviceStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.status[deviceID] = status

	return nil
}

// Close 关闭存储连接
func (s *MemoryStorage) Close() error {
	return nil
}

// Ping 检查连接健康
func (s *MemoryStorage) Ping() error {
	return nil
}

// GetStats 获取统计信息
func (s *MemoryStorage) GetStats() MemoryStorageStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := MemoryStorageStats{
		DeviceCount: len(s.devices),
	}

	for _, locations := range s.locations {
		stats.LocationCount += len(locations)
	}

	for _, alarms := range s.alarms {
		stats.AlarmCount += len(alarms)
	}

	return stats
}

// MemoryStorageStats 内存存储统计信息
type MemoryStorageStats struct {
	DeviceCount   int
	LocationCount int
	AlarmCount    int
}
