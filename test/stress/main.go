package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/im10furry/jtt-808-go-sdk/protocol"
	"github.com/im10furry/jtt-808-go-sdk/protocol/types"
)

var (
	concurrency int
	duration    int
	serverAddr  string
	messageRate int
	scenario    string
	batchSize   int

	totalConnections  int64
	activeConnections int64
	totalMessages     int64
	successMessages   int64
	failedMessages    int64
	connectionErrors  int64
	totalLatency      int64
	peakConnections   int64
	startTime         time.Time
	codec             *protocol.Codec
)

func main() {
	flag.IntVar(&concurrency, "c", 1000, "并发连接数")
	flag.IntVar(&duration, "d", 30, "测试持续时间（秒）")
	flag.StringVar(&serverAddr, "s", "127.0.0.1:8080", "服务器地址")
	flag.IntVar(&messageRate, "r", 10, "每个连接每秒发送消息数")
	flag.StringVar(&scenario, "t", "location", "测试场景: location, register, heartbeat, mixed")
	flag.IntVar(&batchSize, "b", 500, "批次连接大小")
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())

	// 初始化编解码器
	codec = protocol.NewCodec()

	fmt.Printf("=== JT808 压力测试工具 ===\n")
	fmt.Printf("服务器地址: %s\n", serverAddr)
	fmt.Printf("并发连接数: %d\n", concurrency)
	fmt.Printf("测试时长: %d秒\n", duration)
	fmt.Printf("消息频率: %d/秒/连接\n", messageRate)
	fmt.Printf("测试场景: %s\n", scenario)
	fmt.Printf("批次大小: %d\n", batchSize)
	fmt.Printf("CPU核数: %d\n\n", runtime.NumCPU())

	startTime = time.Now()
	go printStats()

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	// 批次创建连接，避免瞬时连接风暴
	for batch := 0; batch < concurrency; batch += batchSize {
		end := batch + batchSize
		if end > concurrency {
			end = concurrency
		}
		for i := batch; i < end; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				runWorker(id, stopCh)
			}(i)
		}
		// 批次间隔
		if batch+batchSize < concurrency {
			time.Sleep(50 * time.Millisecond)
		}
	}

	time.Sleep(time.Duration(duration) * time.Second)
	close(stopCh)
	wg.Wait()
	printFinalResults()
}

func runWorker(id int, stopCh <-chan struct{}) {
	conn, err := net.DialTimeout("tcp", serverAddr, 10*time.Second)
	if err != nil {
		atomic.AddInt64(&connectionErrors, 1)
		atomic.AddInt64(&failedMessages, 1)
		return
	}
	defer conn.Close()

	atomic.AddInt64(&totalConnections, 1)
	atomic.AddInt64(&activeConnections, 1)
	defer atomic.AddInt64(&activeConnections, -1)

	// 更新峰值
	for {
		cur := atomic.LoadInt64(&activeConnections)
		peak := atomic.LoadInt64(&peakConnections)
		if cur <= peak || atomic.CompareAndSwapInt64(&peakConnections, peak, cur) {
			break
		}
	}

	switch scenario {
	case "location":
		runLocationTest(id, conn, stopCh)
	case "register":
		runRegisterTest(id, conn, stopCh)
	case "heartbeat":
		runHeartbeatTest(id, conn, stopCh)
	case "mixed":
		runMixedTest(id, conn, stopCh)
	default:
		runLocationTest(id, conn, stopCh)
	}
}

func runLocationTest(id int, conn net.Conn, stopCh <-chan struct{}) {
	deviceID := fmt.Sprintf("DEVICE_%06d", id)
	phoneNo := fmt.Sprintf("138%08d", id)

	if err := sendRegister(conn, deviceID, phoneNo); err != nil {
		fmt.Printf("[DEBUG] 设%d sendRegister 失败: %v\n", id, err)
		atomic.AddInt64(&failedMessages, 1)
		return
	}
	if err := readResponse(conn); err != nil {
		fmt.Printf("[DEBUG] 设%d readResponse 失败: %v\n", id, err)
		atomic.AddInt64(&failedMessages, 1)
		return
	}

	ticker := time.NewTicker(time.Second / time.Duration(messageRate))
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			start := time.Now()
			if err := sendLocation(conn, deviceID, phoneNo); err != nil {
				atomic.AddInt64(&failedMessages, 1)
				return
			}
			if err := readResponse(conn); err != nil {
				atomic.AddInt64(&failedMessages, 1)
				return
			}
			atomic.AddInt64(&totalLatency, int64(time.Since(start)))
			atomic.AddInt64(&successMessages, 1)
			atomic.AddInt64(&totalMessages, 1)
		}
	}
}

func runRegisterTest(id int, conn net.Conn, stopCh <-chan struct{}) {
	deviceID := fmt.Sprintf("DEVICE_%06d", id)
	phoneNo := fmt.Sprintf("138%08d", id)

	ticker := time.NewTicker(time.Second / time.Duration(messageRate))
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			start := time.Now()
			if err := sendRegister(conn, deviceID, phoneNo); err != nil {
				atomic.AddInt64(&failedMessages, 1)
				return
			}
			if err := readResponse(conn); err != nil {
				atomic.AddInt64(&failedMessages, 1)
				return
			}
			atomic.AddInt64(&totalLatency, int64(time.Since(start)))
			atomic.AddInt64(&successMessages, 1)
			atomic.AddInt64(&totalMessages, 1)
		}
	}
}

func runHeartbeatTest(id int, conn net.Conn, stopCh <-chan struct{}) {
	deviceID := fmt.Sprintf("DEVICE_%06d", id)
	phoneNo := fmt.Sprintf("138%08d", id)

	if err := sendRegister(conn, deviceID, phoneNo); err != nil {
		return
	}
	readResponse(conn)

	ticker := time.NewTicker(time.Second / time.Duration(messageRate))
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			start := time.Now()
			if err := sendHeartbeat(conn, phoneNo); err != nil {
				atomic.AddInt64(&failedMessages, 1)
				return
			}
			if err := readResponse(conn); err != nil {
				atomic.AddInt64(&failedMessages, 1)
				return
			}
			atomic.AddInt64(&totalLatency, int64(time.Since(start)))
			atomic.AddInt64(&successMessages, 1)
			atomic.AddInt64(&totalMessages, 1)
		}
	}
}

func runMixedTest(id int, conn net.Conn, stopCh <-chan struct{}) {
	deviceID := fmt.Sprintf("DEVICE_%06d", id)
	phoneNo := fmt.Sprintf("138%08d", id)

	if err := sendRegister(conn, deviceID, phoneNo); err != nil {
		return
	}
	readResponse(conn)

	locationTicker := time.NewTicker(time.Second / time.Duration(messageRate))
	heartbeatTicker := time.NewTicker(30 * time.Second)
	defer locationTicker.Stop()
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-locationTicker.C:
			start := time.Now()
			if err := sendLocation(conn, deviceID, phoneNo); err != nil {
				atomic.AddInt64(&failedMessages, 1)
				return
			}
			if err := readResponse(conn); err != nil {
				atomic.AddInt64(&failedMessages, 1)
				return
			}
			atomic.AddInt64(&totalLatency, int64(time.Since(start)))
			atomic.AddInt64(&successMessages, 1)
			atomic.AddInt64(&totalMessages, 1)
		case <-heartbeatTicker.C:
			if err := sendHeartbeat(conn, phoneNo); err != nil {
				return
			}
		}
	}
}

func sendRegister(conn net.Conn, deviceID, phoneNo string) error {
	body := make([]byte, 53)
	binary.BigEndian.PutUint16(body[0:2], 1100)
	binary.BigEndian.PutUint16(body[2:4], 1101)
	copy(body[4:9], "TEST0")
	copy(body[9:39], "JT808-G-2024")
	copy(body[39:46], deviceID[:7])
	body[46] = 1
	copy(body[47:], "STRESS"+phoneNo[len(phoneNo)-4:])

	msg, err := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID:     types.MsgIDTerminalRegister,
			PhoneNo:   phoneNo,
			MsgFlowNo: 1,
		},
		Body: body,
	})
	if err != nil {
		return err
	}
	_, err = conn.Write(msg)
	return err
}

func sendLocation(conn net.Conn, deviceID, phoneNo string) error {
	body := make([]byte, 28)
	binary.BigEndian.PutUint32(body[0:4], 0)
	binary.BigEndian.PutUint32(body[4:8], 0x02)
	binary.BigEndian.PutUint32(body[8:12], 3990420)
	binary.BigEndian.PutUint32(body[12:16], 11640740)
	binary.BigEndian.PutUint16(body[16:18], 50)
	binary.BigEndian.PutUint16(body[18:20], 600)
	binary.BigEndian.PutUint16(body[20:22], 90)

	now := time.Now()
	body[22] = byte((now.Year()%100)/10)<<4 | byte((now.Year()%100)%10)
	body[23] = byte(int(now.Month())/10)<<4 | byte(int(now.Month())%10)
	body[24] = byte(now.Day()/10)<<4 | byte(now.Day()%10)
	body[25] = byte(now.Hour()/10)<<4 | byte(now.Hour()%10)
	body[26] = byte(now.Minute()/10)<<4 | byte(now.Minute()%10)
	body[27] = byte(now.Second()/10)<<4 | byte(now.Second()%10)

	msg, err := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID:     types.MsgIDLocationReport,
			PhoneNo:   phoneNo,
			MsgFlowNo: 2,
		},
		Body: body,
	})
	if err != nil {
		return err
	}
	_, err = conn.Write(msg)
	return err
}

func sendHeartbeat(conn net.Conn, phoneNo string) error {
	msg, err := codec.Encode(&protocol.Message{
		Header: &protocol.MessageHeader{
			MsgID:     types.MsgIDTerminalHeartbeat,
			PhoneNo:   phoneNo,
			MsgFlowNo: 3,
		},
		Body: []byte{},
	})
	if err != nil {
		return err
	}
	_, err = conn.Write(msg)
	return err
}

func readResponse(conn net.Conn) error {
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 4096)
	_, err := conn.Read(buf)
	return err
}

func buildMessage(msgID uint16, phoneNo string, flowNo uint16, body []byte) []byte {
	header := make([]byte, 12)
	binary.BigEndian.PutUint16(header[0:2], msgID)
	binary.BigEndian.PutUint16(header[2:4], uint16(len(body)))
	copy(header[4:10], encodeBCD(phoneNo, 6))
	binary.BigEndian.PutUint16(header[10:12], flowNo)

	var checksum byte
	for _, b := range header {
		checksum ^= b
	}
	for _, b := range body {
		checksum ^= b
	}

	msg := make([]byte, 0, len(header)+len(body)+3)
	msg = append(msg, 0x7E)
	msg = append(msg, escape(header)...)
	msg = append(msg, escape(body)...)
	msg = append(msg, checksum)
	msg = append(msg, 0x7E)
	return msg
}

func encodeBCD(s string, length int) []byte {
	result := make([]byte, length)
	for i := 0; i < length && i*2 < len(s); i++ {
		high, low := byte(0), byte(0)
		if i*2 < len(s) {
			high = s[i*2] - '0'
		}
		if i*2+1 < len(s) {
			low = s[i*2+1] - '0'
		}
		result[i] = (high << 4) | low
	}
	return result
}

func escape(data []byte) []byte {
	var result []byte
	for _, b := range data {
		switch b {
		case 0x7E:
			result = append(result, 0x7D, 0x02)
		case 0x7D:
			result = append(result, 0x7D, 0x01)
		default:
			result = append(result, b)
		}
	}
	return result
}

func printStats() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		total := atomic.LoadInt64(&totalMessages)
		success := atomic.LoadInt64(&successMessages)
		failed := atomic.LoadInt64(&failedMessages)
		active := atomic.LoadInt64(&activeConnections)
		connErr := atomic.LoadInt64(&connectionErrors)
		latency := atomic.LoadInt64(&totalLatency)
		elapsed := time.Since(startTime)

		var avgLatency time.Duration
		if total > 0 {
			avgLatency = time.Duration(latency / total)
		}

		fmt.Printf("\r[%s] Conn: %d/%d | Msg: %d | OK: %d | Fail: %d | ConnErr: %d | Rate: %.0f/s | Lat: %v",
			elapsed.Truncate(time.Second),
			active, atomic.LoadInt64(&totalConnections),
			total, success, failed, connErr,
			float64(total)/elapsed.Seconds(),
			avgLatency,
		)
	}
}

func printFinalResults() {
	total := atomic.LoadInt64(&totalMessages)
	success := atomic.LoadInt64(&successMessages)
	failed := atomic.LoadInt64(&failedMessages)
	latency := atomic.LoadInt64(&totalLatency)
	elapsed := time.Since(startTime)
	connErr := atomic.LoadInt64(&connectionErrors)

	var avgLatency time.Duration
	if total > 0 {
		avgLatency = time.Duration(latency / total)
	}

	fmt.Printf("\n\n=== 压力测试结果 ===\n")
	fmt.Printf("测试时长:       %v\n", elapsed)
	fmt.Printf("总连接数:       %d\n", atomic.LoadInt64(&totalConnections))
	fmt.Printf("峰值连接数:     %d\n", atomic.LoadInt64(&peakConnections))
	fmt.Printf("连接错误:       %d\n", connErr)
	fmt.Printf("总消息数:       %d\n", total)
	fmt.Printf("成功消息:       %d\n", success)
	fmt.Printf("失败消息:       %d\n", failed)
	fmt.Printf("消息速率:       %.2f msg/s\n", float64(total)/elapsed.Seconds())
	fmt.Printf("平均延迟:       %v\n", avgLatency)
	if total > 0 {
		fmt.Printf("成功率:         %.2f%%\n", float64(success)/float64(total)*100)
	}
	fmt.Printf("CPU核数:        %d\n", runtime.NumCPU())

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("内存使用:       %.2f MB\n", float64(m.Alloc)/1024/1024)
}
