package service

import "go.uber.org/zap"

type messageHandler func(*ParsedMessage)

func (s *SerialService) initMessageHandlers() {
	s.handlers = map[string]messageHandler{
		"incoming_sms":              s.handleIncomingSMS,
		"system_ready":              s.handleSystemReady,
		"heartbeat":                 s.handleHeartbeat,
		"status_response":           s.handleStatusResponse,
		"cellular_control_response": s.handleCellularControlResponse,
		"phone_number_response":     s.handlePhoneNumberResponse,
		"cmd_response":              s.handleCommandResponse,
		"sms_send_result":           s.handleSMSSendResult,
		"sim_event":                 s.handleSIMEvent,
		"warning":                   s.handleWarningMessage,
		"error":                     s.handleErrorMessage,
		"incoming_call":             s.handleIncomingCall,
		"call_disconnected":         s.handleCallDisconnected,
	}
}

func (s *SerialService) routeMessage(msg *ParsedMessage) {
	handler, ok := s.handlers[msg.Type]
	if !ok {
		s.logger.Debug("未知消息类型", zap.String("type", msg.Type))
		return
	}

	handler(msg)
}
