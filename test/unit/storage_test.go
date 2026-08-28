package unit

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/im10furry/jtt-808-go-sdk/core"
	"github.com/im10furry/jtt-808-go-sdk/storage"
)

// TestMemoryStorageCreation 测试内存存储创建
func TestMemoryStorageCreation(t *testing.T) {
	store := storage.NewMemoryStorage()
	if store == nil {
		t.Fatal("Failed to create memory storage")
	}
}

// TestMemoryStorageSaveLocation 测试保存位置信息
func TestMemoryStorageSaveLocation(t *testing.T) {
	store := storage.NewMemoryStorage()

	t.Run("SaveValidLocation", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), "deviceID", "device-001")

		loc := &core.LocationReport{
			AlarmFlag: 0,
			Status:    2,
			Latitude:  39.9042,
			Longitude: 116.4074,
			Altitude:  50,
			Speed:     60,
			Direction: 90,
			Time:      time.Now(),
		}

		err := store.SaveLocation(ctx, loc)
		if err != nil {
			t.Fatalf("Failed to save location: %v", err)
		}
	})

	t.Run("SaveNilLocation", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), "deviceID", "device-001")

		err := store.SaveLocation(ctx, nil)
		if err == nil {
			t.Error("Expected error when saving nil location")
		}
	})

	t.Run("SaveWithoutDeviceID", func(t *testing.T) {
		ctx := context.Background()

		loc := &core.LocationReport{
			AlarmFlag: 0,
			Status:    2,
			Latitude:  39.9042,
			Longitude: 116.4074,
			Altitude:  50,
			Speed:     60,
			Direction: 90,
			Time:      time.Now(),
		}

		err := store.SaveLocation(ctx, loc)
		if err == nil {
			t.Error("Expected error when saving without device ID")
		}
	})

	t.Run("SaveMultipleLocations", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), "deviceID", "device-002")

		for i := 0; i < 10; i++ {
			loc := &core.LocationReport{
				AlarmFlag: 0,
				Status:    2,
				Latitude:  39.9042 + float64(i)*0.0001,
				Longitude: 116.4074 + float64(i)*0.0001,
				Altitude:  uint16(50 + i),
				Speed:     uint16(60 + i),
				Direction: uint16(90 + i),
				Time:      time.Now().Add(time.Duration(i) * time.Minute),
			}

			err := store.SaveLocation(ctx, loc)
			if err != nil {
				t.Fatalf("Failed to save location %d: %v", i, err)
			}
		}
	})
}

// TestMemoryStorageGetLocations 测试获取位置信息
func TestMemoryStorageGetLocations(t *testing.T) {
	store := storage.NewMemoryStorage()

	t.Run("GetFromEmptyStorage", func(t *testing.T) {
		start := time.Now().Add(-1 * time.Hour)
		end := time.Now()

		locations, err := store.GetLocations(context.Background(), "nonexistent", start, end)
		if err != nil {
			t.Fatalf("Failed to get locations: %v", err)
		}

		if locations != nil {
			t.Errorf("Expected nil locations, got %d", len(locations))
		}
	})

	t.Run("GetLocationsInTimeRange", func(t *testing.T) {
		deviceID := "device-003"
		ctx := context.WithValue(context.Background(), "deviceID", deviceID)

		now := time.Now()

		// 保存位置信息
		for i := 0; i < 5; i++ {
			loc := &core.LocationReport{
				AlarmFlag: 0,
				Status:    2,
				Latitude:  39.9042,
				Longitude: 116.4074,
				Altitude:  50,
				Speed:     60,
				Direction: 90,
				Time:      now.Add(time.Duration(i) * time.Minute),
			}

			err := store.SaveLocation(ctx, loc)
			if err != nil {
				t.Fatalf("Failed to save location: %v", err)
			}
		}

		// 获取所有位置
		start := now.Add(-1 * time.Minute)
		end := now.Add(10 * time.Minute)

		locations, err := store.GetLocations(context.Background(), deviceID, start, end)
		if err != nil {
			t.Fatalf("Failed to get locations: %v", err)
		}

		if len(locations) != 5 {
			t.Errorf("Expected 5 locations, got %d", len(locations))
		}
	})

	t.Run("GetLocationsOutsideTimeRange", func(t *testing.T) {
		deviceID := "device-004"
		ctx := context.WithValue(context.Background(), "deviceID", deviceID)

		now := time.Now()

		// 保存位置信息
		loc := &core.LocationReport{
			AlarmFlag: 0,
			Status:    2,
			Latitude:  39.9042,
			Longitude: 116.4074,
			Altitude:  50,
			Speed:     60,
			Direction: 90,
			Time:      now,
		}

		err := store.SaveLocation(ctx, loc)
		if err != nil {
			t.Fatalf("Failed to save location: %v", err)
		}

		// 获取不在时间范围内的位置
		start := now.Add(1 * time.Hour)
		end := now.Add(2 * time.Hour)

		locations, err := store.GetLocations(context.Background(), deviceID, start, end)
		if err != nil {
			t.Fatalf("Failed to get locations: %v", err)
		}

		if len(locations) != 0 {
			t.Errorf("Expected 0 locations, got %d", len(locations))
		}
	})
}

// TestMemoryStorageLocationLimit 测试位置信息限制
func TestMemoryStorageLocationLimit(t *testing.T) {
	store := storage.NewMemoryStorage()
	deviceID := "device-limit"
	ctx := context.WithValue(context.Background(), "deviceID", deviceID)

	// 保存超过限制的位置信息
	for i := 0; i < 1100; i++ {
		loc := &core.LocationReport{
			AlarmFlag: 0,
			Status:    2,
			Latitude:  39.9042,
			Longitude: 116.4074,
			Altitude:  50,
			Speed:     60,
			Direction: 90,
			Time:      time.Now().Add(time.Duration(i) * time.Second),
		}

		err := store.SaveLocation(ctx, loc)
		if err != nil {
			t.Fatalf("Failed to save location %d: %v", i, err)
		}
	}

	// 验证限制
	stats := store.GetStats()
	if stats.LocationCount > 1000 {
		t.Errorf("Expected location count <= 1000, got %d", stats.LocationCount)
	}
}

// TestMemoryStorageSaveAlarm 测试保存报警信息
func TestMemoryStorageSaveAlarm(t *testing.T) {
	store := storage.NewMemoryStorage()

	t.Run("SaveValidAlarm", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), "deviceID", "device-001")

		alarm := &core.AlarmReport{
			AlarmFlag: 1,
			Status:    2,
			Latitude:  39.9042,
			Longitude: 116.4074,
			Altitude:  50,
			Speed:     60,
			Direction: 90,
			Time:      time.Now(),
			AlarmType: 1,
		}

		err := store.SaveAlarm(ctx, alarm)
		if err != nil {
			t.Fatalf("Failed to save alarm: %v", err)
		}
	})

	t.Run("SaveNilAlarm", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), "deviceID", "device-001")

		err := store.SaveAlarm(ctx, nil)
		if err == nil {
			t.Error("Expected error when saving nil alarm")
		}
	})

	t.Run("SaveWithoutDeviceID", func(t *testing.T) {
		ctx := context.Background()

		alarm := &core.AlarmReport{
			AlarmFlag: 1,
			Status:    2,
			Latitude:  39.9042,
			Longitude: 116.4074,
			Altitude:  50,
			Speed:     60,
			Direction: 90,
			Time:      time.Now(),
			AlarmType: 1,
		}

		err := store.SaveAlarm(ctx, alarm)
		if err == nil {
			t.Error("Expected error when saving without device ID")
		}
	})
}

// TestMemoryStorageGetAlarms 测试获取报警信息
func TestMemoryStorageGetAlarms(t *testing.T) {
	store := storage.NewMemoryStorage()

	t.Run("GetFromEmptyStorage", func(t *testing.T) {
		start := time.Now().Add(-1 * time.Hour)
		end := time.Now()

		alarms, err := store.GetAlarms(context.Background(), "nonexistent", start, end)
		if err != nil {
			t.Fatalf("Failed to get alarms: %v", err)
		}

		if alarms != nil {
			t.Errorf("Expected nil alarms, got %d", len(alarms))
		}
	})

	t.Run("GetAlarmsInTimeRange", func(t *testing.T) {
		deviceID := "device-005"
		ctx := context.WithValue(context.Background(), "deviceID", deviceID)

		now := time.Now()

		// 保存报警信息
		for i := 0; i < 5; i++ {
			alarm := &core.AlarmReport{
				AlarmFlag: uint32(i),
				Status:    2,
				Latitude:  39.9042,
				Longitude: 116.4074,
				Altitude:  50,
				Speed:     60,
				Direction: 90,
				Time:      now.Add(time.Duration(i) * time.Minute),
				AlarmType: uint8(i),
			}

			err := store.SaveAlarm(ctx, alarm)
			if err != nil {
				t.Fatalf("Failed to save alarm: %v", err)
			}
		}

		// 获取所有报警
		start := now.Add(-1 * time.Minute)
		end := now.Add(10 * time.Minute)

		alarms, err := store.GetAlarms(context.Background(), deviceID, start, end)
		if err != nil {
			t.Fatalf("Failed to get alarms: %v", err)
		}

		if len(alarms) != 5 {
			t.Errorf("Expected 5 alarms, got %d", len(alarms))
		}
	})
}

// TestMemoryStorageAlarmLimit 测试报警信息限制
func TestMemoryStorageAlarmLimit(t *testing.T) {
	store := storage.NewMemoryStorage()
	deviceID := "device-alarm-limit"
	ctx := context.WithValue(context.Background(), "deviceID", deviceID)

	// 保存超过限制的报警信息
	for i := 0; i < 1100; i++ {
		alarm := &core.AlarmReport{
			AlarmFlag: 1,
			Status:    2,
			Latitude:  39.9042,
			Longitude: 116.4074,
			Altitude:  50,
			Speed:     60,
			Direction: 90,
			Time:      time.Now().Add(time.Duration(i) * time.Second),
			AlarmType: 1,
		}

		err := store.SaveAlarm(ctx, alarm)
		if err != nil {
			t.Fatalf("Failed to save alarm %d: %v", i, err)
		}
	}

	// 验证限制
	stats := store.GetStats()
	if stats.AlarmCount > 1000 {
		t.Errorf("Expected alarm count <= 1000, got %d", stats.AlarmCount)
	}
}

// TestMemoryStorageSaveDevice 测试保存设备信息
func TestMemoryStorageSaveDevice(t *testing.T) {
	store := storage.NewMemoryStorage()

	t.Run("SaveValidDevice", func(t *testing.T) {
		deviceID := "device-001"
		info := &core.TerminalRegister{
			ProvinceID:     1,
			CityID:         1,
			ManufacturerID: "TEST0",
			TerminalType:   "TEST_TERMINAL",
			TerminalID:     "1234567",
			PlateColor:     1,
			PlateNo:        "A12345",
		}

		err := store.SaveDevice(context.Background(), deviceID, info)
		if err != nil {
			t.Fatalf("Failed to save device: %v", err)
		}
	})

	t.Run("SaveNilDevice", func(t *testing.T) {
		deviceID := "device-002"

		err := store.SaveDevice(context.Background(), deviceID, nil)
		if err == nil {
			t.Error("Expected error when saving nil device")
		}
	})

	t.Run("OverwriteDevice", func(t *testing.T) {
		deviceID := "device-003"

		info1 := &core.TerminalRegister{
			ProvinceID:     1,
			CityID:         1,
			ManufacturerID: "TEST1",
			TerminalType:   "TYPE1",
			TerminalID:     "1111111",
			PlateColor:     1,
			PlateNo:        "A11111",
		}

		info2 := &core.TerminalRegister{
			ProvinceID:     2,
			CityID:         2,
			ManufacturerID: "TEST2",
			TerminalType:   "TYPE2",
			TerminalID:     "2222222",
			PlateColor:     2,
			PlateNo:        "B22222",
		}

		err := store.SaveDevice(context.Background(), deviceID, info1)
		if err != nil {
			t.Fatalf("Failed to save device: %v", err)
		}

		err = store.SaveDevice(context.Background(), deviceID, info2)
		if err != nil {
			t.Fatalf("Failed to overwrite device: %v", err)
		}

		// 验证设备信息被覆盖
		device, err := store.GetDevice(context.Background(), deviceID)
		if err != nil {
			t.Fatalf("Failed to get device: %v", err)
		}

		if device.ManufacturerID != "TEST2" {
			t.Errorf("Expected manufacturer ID 'TEST2', got '%s'", device.ManufacturerID)
		}
	})
}

// TestMemoryStorageGetDevice 测试获取设备信息
func TestMemoryStorageGetDevice(t *testing.T) {
	store := storage.NewMemoryStorage()

	t.Run("GetExistingDevice", func(t *testing.T) {
		deviceID := "device-001"
		info := &core.TerminalRegister{
			ProvinceID:     1,
			CityID:         1,
			ManufacturerID: "TEST0",
			TerminalType:   "TEST_TERMINAL",
			TerminalID:     "1234567",
			PlateColor:     1,
			PlateNo:        "A12345",
		}

		err := store.SaveDevice(context.Background(), deviceID, info)
		if err != nil {
			t.Fatalf("Failed to save device: %v", err)
		}

		device, err := store.GetDevice(context.Background(), deviceID)
		if err != nil {
			t.Fatalf("Failed to get device: %v", err)
		}

		if device.ManufacturerID != "TEST0" {
			t.Errorf("Expected manufacturer ID 'TEST0', got '%s'", device.ManufacturerID)
		}

		if device.PlateNo != "A12345" {
			t.Errorf("Expected plate no 'A12345', got '%s'", device.PlateNo)
		}
	})

	t.Run("GetNonExistentDevice", func(t *testing.T) {
		_, err := store.GetDevice(context.Background(), "nonexistent")
		if err == nil {
			t.Error("Expected error when getting non-existent device")
		}
	})
}

// TestMemoryStorageDeviceStatus 测试设备状态
func TestMemoryStorageDeviceStatus(t *testing.T) {
	store := storage.NewMemoryStorage()

	deviceID := "device-001"
	info := &core.TerminalRegister{
		ProvinceID:     1,
		CityID:         1,
		ManufacturerID: "TEST0",
		TerminalType:   "TEST_TERMINAL",
		TerminalID:     "1234567",
		PlateColor:     1,
		PlateNo:        "A12345",
	}

	// 保存设备
	err := store.SaveDevice(context.Background(), deviceID, info)
	if err != nil {
		t.Fatalf("Failed to save device: %v", err)
	}

	// 更新状态为离线
	err = store.UpdateDeviceStatus(context.Background(), deviceID, core.DeviceStatusOffline)
	if err != nil {
		t.Fatalf("Failed to update device status: %v", err)
	}

	// 更新状态为在线
	err = store.UpdateDeviceStatus(context.Background(), deviceID, core.DeviceStatusOnline)
	if err != nil {
		t.Fatalf("Failed to update device status: %v", err)
	}

	// 更新状态为认证中
	err = store.UpdateDeviceStatus(context.Background(), deviceID, core.DeviceStatusAuthenticating)
	if err != nil {
		t.Fatalf("Failed to update device status: %v", err)
	}
}

// TestMemoryStorageClose 测试关闭存储
func TestMemoryStorageClose(t *testing.T) {
	store := storage.NewMemoryStorage()

	err := store.Close()
	if err != nil {
		t.Fatalf("Failed to close storage: %v", err)
	}
}

// TestMemoryStoragePing 测试存储健康检查
func TestMemoryStoragePing(t *testing.T) {
	store := storage.NewMemoryStorage()

	err := store.Ping()
	if err != nil {
		t.Fatalf("Failed to ping storage: %v", err)
	}
}

// TestMemoryStorageStats 测试存储统计
func TestMemoryStorageStats(t *testing.T) {
	store := storage.NewMemoryStorage()

	// 初始统计
	stats := store.GetStats()
	if stats.DeviceCount != 0 {
		t.Errorf("Expected 0 devices, got %d", stats.DeviceCount)
	}

	if stats.LocationCount != 0 {
		t.Errorf("Expected 0 locations, got %d", stats.LocationCount)
	}

	if stats.AlarmCount != 0 {
		t.Errorf("Expected 0 alarms, got %d", stats.AlarmCount)
	}

	// 添加数据后统计
	deviceID := "device-stats"
	ctx := context.WithValue(context.Background(), "deviceID", deviceID)

	// 保存设备
	info := &core.TerminalRegister{
		ProvinceID:     1,
		CityID:         1,
		ManufacturerID: "TEST0",
		TerminalType:   "TEST_TERMINAL",
		TerminalID:     "1234567",
		PlateColor:     1,
		PlateNo:        "A12345",
	}
	store.SaveDevice(context.Background(), deviceID, info)

	// 保存位置
	for i := 0; i < 5; i++ {
		loc := &core.LocationReport{
			AlarmFlag: 0,
			Status:    2,
			Latitude:  39.9042,
			Longitude: 116.4074,
			Altitude:  50,
			Speed:     60,
			Direction: 90,
			Time:      time.Now(),
		}
		store.SaveLocation(ctx, loc)
	}

	// 保存报警
	for i := 0; i < 3; i++ {
		alarm := &core.AlarmReport{
			AlarmFlag: 1,
			Status:    2,
			Latitude:  39.9042,
			Longitude: 116.4074,
			Altitude:  50,
			Speed:     60,
			Direction: 90,
			Time:      time.Now(),
			AlarmType: 1,
		}
		store.SaveAlarm(ctx, alarm)
	}

	stats = store.GetStats()
	if stats.DeviceCount != 1 {
		t.Errorf("Expected 1 device, got %d", stats.DeviceCount)
	}

	if stats.LocationCount != 5 {
		t.Errorf("Expected 5 locations, got %d", stats.LocationCount)
	}

	if stats.AlarmCount != 3 {
		t.Errorf("Expected 3 alarms, got %d", stats.AlarmCount)
	}
}

// TestMemoryStorageConcurrency 测试并发安全性
func TestMemoryStorageConcurrency(t *testing.T) {
	store := storage.NewMemoryStorage()

	var wg sync.WaitGroup
	numGoroutines := 10

	// 并发保存设备
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			deviceID := fmt.Sprintf("device-%d", id)
			info := &core.TerminalRegister{
				ProvinceID:     uint16(id),
				CityID:         uint16(id),
				ManufacturerID: fmt.Sprintf("MFR%d", id),
				TerminalType:   fmt.Sprintf("TYPE%d", id),
				TerminalID:     fmt.Sprintf("%07d", id),
				PlateColor:     uint8(id % 10),
				PlateNo:        fmt.Sprintf("A%05d", id),
			}
			store.SaveDevice(context.Background(), deviceID, info)
		}(i)
	}

	// 并发保存位置
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			deviceID := fmt.Sprintf("device-%d", id)
			ctx := context.WithValue(context.Background(), "deviceID", deviceID)
			loc := &core.LocationReport{
				AlarmFlag: 0,
				Status:    2,
				Latitude:  39.9042,
				Longitude: 116.4074,
				Altitude:  50,
				Speed:     60,
				Direction: 90,
				Time:      time.Now(),
			}
			store.SaveLocation(ctx, loc)
		}(i)
	}

	// 并发保存报警
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			deviceID := fmt.Sprintf("device-%d", id)
			ctx := context.WithValue(context.Background(), "deviceID", deviceID)
			alarm := &core.AlarmReport{
				AlarmFlag: 1,
				Status:    2,
				Latitude:  39.9042,
				Longitude: 116.4074,
				Altitude:  50,
				Speed:     60,
				Direction: 90,
				Time:      time.Now(),
				AlarmType: 1,
			}
			store.SaveAlarm(ctx, alarm)
		}(i)
	}

	wg.Wait()

	// 验证统计
	stats := store.GetStats()
	if stats.DeviceCount != numGoroutines {
		t.Errorf("Expected %d devices, got %d", numGoroutines, stats.DeviceCount)
	}
}
