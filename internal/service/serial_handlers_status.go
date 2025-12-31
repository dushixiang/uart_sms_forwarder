package service

import (
	"encoding/json"

	"go.uber.org/zap"
)

type StatusData struct {
	CellularEnabled bool   `json:"cellular_enabled"`
	Type            string `json:"type"`
	Version         string `json:"version"` // Lua 脚本版本
	Mobile          struct {
		IsRegistered bool    `json:"is_registered"`
		IsRoaming    bool    `json:"is_roaming"`
		Iccid        string  `json:"iccid"`
		SignalDesc   string  `json:"signal_desc"`
		SignalLevel  int     `json:"signal_level"`
		SimReady     bool    `json:"sim_ready"`
		Rssi         int     `json:"rssi"`
		Csq          int     `json:"csq"`      // CSQ 信号强度 (0-31)
		Rsrp         int     `json:"rsrp"`     // 参考信号接收功率 (-44 到 -140)
		Rsrq         float64 `json:"rsrq"`     // 参考信号发送功率 (-3 到 -19.5)
		Imsi         string  `json:"imsi"`     // SIM 卡 IMSI
		Number       string  `json:"number"`   // 手机号
		Operator     string  `json:"operator"` // 运营商名称
		Uptime       string  `json:"uptime"`   // 模块开机时长（可能有溢出风险）
	} `json:"mobile"`
	Timestamp int    `json:"timestamp"`
	MemKb     int    `json:"mem_kb"`
	PortName  string `json:"port_name"` // 串口名称
	Connected bool   `json:"connected"` // 连接状态
}

func (s *SerialService) handleStatusResponse(msg *ParsedMessage) {
	var statusData StatusData
	if err := json.Unmarshal([]byte(msg.JSON), &statusData); err != nil {
		s.logger.Error("JSON解析失败", zap.Error(err), zap.String("data", msg.JSON))
		return
	}
	imsi := statusData.Mobile.Imsi
	if len(imsi) > 5 {
		plmn := imsi[:5]
		statusData.Mobile.Operator = func() string {
			if v, ok := OperData[plmn]; ok {
				return v
			}
			return plmn
		}()
	}
	s.deviceCache.Set(CacheKeyDeviceStatus, &statusData, CacheTTL)
	s.logger.Debug("设备状态缓存已更新")
}

func (s *SerialService) handleSystemReady(msg *ParsedMessage) {
	if message, ok := msg.Payload["message"].(string); ok {
		s.logger.Info("系统就绪", zap.String("message", message))
	}
}

func (s *SerialService) handleHeartbeat(msg *ParsedMessage) {
	timestamp, _ := msg.Payload["timestamp"].(float64)
	memoryUsage, _ := msg.Payload["memory_usage"].(float64)
	bufferSize, _ := msg.Payload["buffer_size"].(float64)

	s.logger.Debug("设备心跳",
		zap.Int64("timestamp", int64(timestamp)),
		zap.Float64("memory_usage", memoryUsage),
		zap.Int("buffer_size", int(bufferSize)))
}

func (s *SerialService) handleCellularControlResponse(msg *ParsedMessage) {
	s.logger.Debug("收到蜂窝网络控制响应", zap.Any("data", msg.Payload))
}

func (s *SerialService) handlePhoneNumberResponse(msg *ParsedMessage) {
	s.logger.Debug("收到电话号码响应", zap.Any("data", msg.Payload))
}

func (s *SerialService) handleCommandResponse(msg *ParsedMessage) {
	if action, ok := msg.Payload["action"].(string); ok {
		s.logger.Info("命令响应", zap.String("action", action), zap.Any("result", msg.Payload["result"]))
	}
}

func (s *SerialService) handleSIMEvent(msg *ParsedMessage) {
	status, _ := msg.Payload["status"].(string)
	s.logger.Info("SIM卡事件", zap.String("status", status))
}

func (s *SerialService) handleWarningMessage(msg *ParsedMessage) {
	if warnMsg, ok := msg.Payload["msg"].(string); ok {
		s.logger.Warn("设备警告", zap.String("message", warnMsg))
	}
}

func (s *SerialService) handleErrorMessage(msg *ParsedMessage) {
	if errMsg, ok := msg.Payload["msg"].(string); ok {
		s.logger.Error("设备错误", zap.String("message", errMsg))
	}
}
