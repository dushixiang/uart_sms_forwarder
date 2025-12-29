package service

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/valyala/fasttemplate"
	"go.uber.org/zap"
	"gopkg.in/gomail.v2"
)

// Notifier 告警通知服务
type Notifier struct {
	logger *zap.Logger
}

func NewNotifier(logger *zap.Logger) *Notifier {
	return &Notifier{
		logger: logger,
	}
}

// NotificationMessage 通用通知消息（支持短信、来电等）
type NotificationMessage struct {
	Type      string // "sms" 或 "call"
	From      string
	Content   string // 短信内容（来电时为空）
	Timestamp int64
}

func (m NotificationMessage) String() string {
	timestamp := time.Unix(m.Timestamp, 0)
	switch m.Type {
	case "call":
		return fmt.Sprintf(`来电通知
----
来电号码: %s
时间: %s
`,
			m.From,
			timestamp.Format(time.DateTime),
		)
	default: // "sms"
		return fmt.Sprintf(`%s
----
来自: %s
时间: %s
`,
			m.Content,
			m.From,
			timestamp.Format(time.DateTime),
		)
	}
}

// sendDingTalk 发送钉钉通知
func (n *Notifier) sendDingTalk(ctx context.Context, webhook, secret, message string) error {
	// 构造钉钉消息体
	body := map[string]interface{}{
		"msgtype": "text",
		"text": map[string]string{
			"content": message,
		},
	}

	// 如果有加签密钥，计算签名
	timestamp := time.Now().UnixMilli()
	if secret != "" {
		sign := n.calculateDingTalkSign(timestamp, secret)
		webhook = fmt.Sprintf("%s&timestamp=%d&sign=%s", webhook, timestamp, sign)
	}
	_, err := n.sendJSONRequest(ctx, webhook, body)
	if err != nil {
		return err
	}
	return nil
}

// calculateDingTalkSign 计算钉钉加签
func (n *Notifier) calculateDingTalkSign(timestamp int64, secret string) string {
	stringToSign := fmt.Sprintf("%d\n%s", timestamp, secret)
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(stringToSign))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

type WeComResult struct {
	Errcode   int    `json:"errcode"`
	Errmsg    string `json:"errmsg"`
	Type      string `json:"type"`
	MediaId   string `json:"media_id"`
	CreatedAt string `json:"created_at"`
}

// sendWeCom 发送企业微信通知
func (n *Notifier) sendWeCom(ctx context.Context, webhook, message string) error {
	body := map[string]interface{}{
		"msgtype": "text",
		"text": map[string]string{
			"content": message,
		},
	}
	result, err := n.sendJSONRequest(ctx, webhook, body)
	if err != nil {
		return err
	}
	var weComResult WeComResult
	if err := json.Unmarshal(result, &weComResult); err != nil {
		return err
	}
	if weComResult.Errcode != 0 {
		return fmt.Errorf("%s", weComResult.Errmsg)
	}
	return nil
}

// sendFeishu 发送飞书通知
func (n *Notifier) sendFeishu(ctx context.Context, webhook, signSecret, message string) error {
	body := map[string]interface{}{
		"msg_type": "text",
		"content": map[string]string{
			"text": message,
		},
	}

	// 如果有加签密钥，计算签名
	if signSecret != "" {
		timestamp := time.Now().Unix()
		stringToSign := fmt.Sprintf("%v", timestamp) + "\n" + signSecret
		var data []byte
		h := hmac.New(sha256.New, []byte(stringToSign))
		_, err := h.Write(data)
		if err != nil {
			return err
		}
		signature := base64.StdEncoding.EncodeToString(h.Sum(nil))

		// 将签名和时间戳加入请求头
		body["timestamp"] = fmt.Sprintf("%v", timestamp)
		body["sign"] = signature
	}

	_, err := n.sendJSONRequest(ctx, webhook, body)
	if err != nil {
		return err
	}
	return nil
}

// 导出方法
func (n *Notifier) SendTelegramByConfig(ctx context.Context, config map[string]interface{}, message string) error {
	return n.sendTelegramByConfig(ctx, config, message)
}

func (n *Notifier) sendTelegramByConfig(ctx context.Context, config map[string]interface{}, message string) error {
	n.logger.Info("config:", zap.Any("config", config))
	apitoken := config["apiToken"].(string)
	userid := config["userid"].(string)
	proxyEnabled := config["proxyEnabled"].(bool)
	proxyUrl := config["proxyUrl"].(string)
	proxyUsername := config["proxyUsername"].(string)
	proxyPassword := config["proxyPassword"].(string)

	// 构建发送消息的URL
	baseURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", apitoken)
	body := map[string]interface{}{
		"chat_id": userid,
		"text":    message,
		//"parse_mode": "markdown",
	}

	if proxyEnabled {
		proxyFullUrl, err := buildProxyURL(proxyUrl, proxyUsername, proxyPassword)
		if err != nil {
			n.logger.Error("代理配置错误", zap.Error(err))
			return err
		}
		_, err = n.sendJSONRequestWithProxy(ctx, baseURL, proxyFullUrl, body)
		if err != nil {
			return err
		}
	} else {
		_, err := n.sendJSONRequest(ctx, baseURL, body)
		if err != nil {
			return err
		}
	}
	return nil
}

// sendCustomWebhook 发送自定义Webhook
func (n *Notifier) sendCustomWebhook(ctx context.Context, config map[string]interface{}, msg NotificationMessage) error {
	// 解析配置
	webhookURL, ok := config["url"].(string)
	if !ok || webhookURL == "" {
		return fmt.Errorf("自定义Webhook配置缺少 url")
	}

	// 获取请求方法，默认 POST
	method := "POST"
	if m, ok := config["method"].(string); ok && m != "" {
		method = strings.ToUpper(m)
	}

	// 获取自定义请求头
	headers := make(map[string]string)
	if h, ok := config["headers"].(map[string]interface{}); ok {
		for k, v := range h {
			if strVal, ok := v.(string); ok {
				headers[k] = strVal
			}
		}
	}

	customBody, ok := config["body"].(string)
	if !ok || customBody == "" {
		return fmt.Errorf("自定义Webhook配置缺少 body")
	}

	// 使用 fasttemplate 进行变量替换
	t := fasttemplate.New(customBody, "{{", "}}")
	escape := func(s string) string {
		b, _ := json.Marshal(s)
		// json.Marshal 会返回带双引号的字符串，例如 "hello\nworld"
		// 模板中不需要外层双引号，所以去掉
		return string(b[1 : len(b)-1])
	}

	bodyStr := t.ExecuteFuncString(func(w io.Writer, tag string) (int, error) {
		var v string

		switch tag {
		case "from":
			v = msg.From
		case "content":
			v = msg.Content
		case "type":
			v = msg.Type
		case "timestamp":
			timestamp := time.Unix(msg.Timestamp, 0).Format(time.DateTime)
			v = timestamp
		default:
			return w.Write([]byte("{{" + tag + "}}"))
		}

		// 写入 JSON 安全转义后的值
		return w.Write([]byte(escape(v)))
	})
	n.logger.Sugar().Debugf("自定义Webhook请求体: %s", bodyStr)
	var reqBody = strings.NewReader(bodyStr)
	var contentType = "text/plain"

	// 创建请求
	req, err := http.NewRequestWithContext(ctx, method, webhookURL, reqBody)
	if err != nil {
		return fmt.Errorf("创建请求失败: %w", err)
	}

	// 设置 Content-Type
	req.Header.Set("Content-Type", contentType)

	// 设置自定义请求头
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	// 发送请求
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("发送请求失败: %w", err)
	}
	defer resp.Body.Close()

	// 读取响应
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("请求失败，状态码: %d, 响应: %s", resp.StatusCode, string(respBody))
	}

	n.logger.Info("自定义Webhook发送成功",
		zap.String("url", webhookURL),
		zap.String("method", method),
		zap.String("response", string(respBody)),
	)

	return nil
}

// sendJSONRequest 发送JSON请求
func (n *Notifier) sendJSONRequest(ctx context.Context, url string, body interface{}) ([]byte, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("序列化请求体失败: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("发送请求失败: %w", err)
	}
	defer resp.Body.Close()

	// 读取响应
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("请求失败，状态码: %d, 响应: %s", resp.StatusCode, string(respBody))
	}

	n.logger.Info("通知发送成功", zap.String("url", url), zap.String("response", string(respBody)))
	return respBody, nil
}

func (n *Notifier) sendJSONRequestWithProxy(ctx context.Context, url string, proxyUrl *url.URL, body interface{}) ([]byte, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("序列化请求体失败: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	transport := &http.Transport{}
	transport.Proxy = http.ProxyURL(proxyUrl)

	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: transport,
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("发送请求失败: %w", err)
	}
	defer resp.Body.Close()

	// 读取响应
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("请求失败，状态码: %d, 响应: %s", resp.StatusCode, string(respBody))
	}

	n.logger.Info("通知发送成功", zap.String("url", url), zap.String("response", string(respBody)))
	return respBody, nil
}

// sendDingTalkByConfig 根据配置发送钉钉通知
func (n *Notifier) sendDingTalkByConfig(ctx context.Context, config map[string]interface{}, message string) error {
	secretKey, ok := config["secretKey"].(string)
	if !ok || secretKey == "" {
		return fmt.Errorf("钉钉配置缺少 secretKey")
	}

	// 构造 Webhook URL
	webhook := fmt.Sprintf("https://oapi.dingtalk.com/robot/send?access_token=%s", secretKey)

	// 检查是否有加签密钥
	signSecret, _ := config["signSecret"].(string)

	return n.sendDingTalk(ctx, webhook, signSecret, message)
}

// sendWeComByConfig 根据配置发送企业微信通知
func (n *Notifier) sendWeComByConfig(ctx context.Context, config map[string]interface{}, message string) error {
	secretKey, ok := config["secretKey"].(string)
	if !ok || secretKey == "" {
		return fmt.Errorf("企业微信配置缺少 secretKey")
	}

	// 构造 Webhook URL
	webhook := fmt.Sprintf("https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=%s", secretKey)

	return n.sendWeCom(ctx, webhook, message)
}

// sendFeishuByConfig 根据配置发送飞书通知
func (n *Notifier) sendFeishuByConfig(ctx context.Context, config map[string]interface{}, message string) error {
	secretKey, ok := config["secretKey"].(string)
	if !ok || secretKey == "" {
		return fmt.Errorf("飞书配置缺少 secretKey")
	}

	// 构造 Webhook URL
	webhook := fmt.Sprintf("https://open.feishu.cn/open-apis/bot/v2/hook/%s", secretKey)

	// 检查是否有加签密钥
	signSecret, _ := config["signSecret"].(string)

	return n.sendFeishu(ctx, webhook, signSecret, message)
}

// SendDingTalkByConfig 导出方法供外部调用
func (n *Notifier) SendDingTalkByConfig(ctx context.Context, config map[string]interface{}, message string) error {
	return n.sendDingTalkByConfig(ctx, config, message)
}

// SendWeComByConfig 导出方法供外部调用
func (n *Notifier) SendWeComByConfig(ctx context.Context, config map[string]interface{}, message string) error {
	return n.sendWeComByConfig(ctx, config, message)
}

// SendFeishuByConfig 导出方法供外部调用
func (n *Notifier) SendFeishuByConfig(ctx context.Context, config map[string]interface{}, message string) error {
	return n.sendFeishuByConfig(ctx, config, message)
}

// SendWebhookByConfig 导出方法供外部调用
func (n *Notifier) SendWebhookByConfig(ctx context.Context, config map[string]interface{}, msg NotificationMessage) error {
	return n.sendCustomWebhook(ctx, config, msg)
}

// sendEmail 发送邮件通知
func (n *Notifier) sendEmail(ctx context.Context, config map[string]interface{}, msg NotificationMessage) error {
	// 解析配置
	smtpHost, ok := config["smtpHost"].(string)
	if !ok || smtpHost == "" {
		return fmt.Errorf("邮件配置缺少 smtpHost")
	}

	smtpPortStr, ok := config["smtpPort"].(string)
	if !ok || smtpPortStr == "" {
		smtpPortStr = "587"
	}

	// 转换端口为整数
	smtpPort, err := strconv.Atoi(smtpPortStr)
	if err != nil {
		return fmt.Errorf("无效的 SMTP 端口: %s", smtpPortStr)
	}

	username, ok := config["username"].(string)
	if !ok || username == "" {
		return fmt.Errorf("邮件配置缺少 username")
	}

	password, ok := config["password"].(string)
	if !ok || password == "" {
		return fmt.Errorf("邮件配置缺少 password")
	}

	from, ok := config["from"].(string)
	if !ok || from == "" {
		return fmt.Errorf("邮件配置缺少 from")
	}

	to, ok := config["to"].(string)
	if !ok || to == "" {
		return fmt.Errorf("邮件配置缺少 to")
	}

	subject, ok := config["subject"].(string)
	if !ok || subject == "" {
		if msg.Type == "call" {
			subject = "来电通知 - {{from}}"
		} else {
			subject = "收到新短信 - {{from}}"
		}
	}

	// 模板变量替换函数
	replaceVars := func(template string) string {
		t := fasttemplate.New(template, "{{", "}}")
		return t.ExecuteFuncString(func(w io.Writer, tag string) (int, error) {
			var v string
			switch tag {
			case "from":
				v = msg.From
			case "content":
				v = msg.Content
			case "type":
				v = msg.Type
			case "timestamp":
				v = time.Unix(msg.Timestamp, 0).Format(time.DateTime)
			default:
				return w.Write([]byte("{{" + tag + "}}"))
			}
			return w.Write([]byte(v))
		})
	}

	// 替换主题中的变量
	subject = replaceVars(subject)

	// 构造邮件内容
	body := msg.String()

	// 分隔多个收件人
	toList := strings.Split(to, ",")
	for i, addr := range toList {
		toList[i] = strings.TrimSpace(addr)
	}

	// 使用 gomail 创建邮件
	m := gomail.NewMessage()
	m.SetHeader("From", from)
	m.SetHeader("To", toList...)
	m.SetHeader("Subject", subject)
	m.SetBody("text/plain", body)

	// 创建 SMTP 拨号器
	d := gomail.NewDialer(smtpHost, smtpPort, username, password)

	// 发送邮件
	if err := d.DialAndSend(m); err != nil {
		return fmt.Errorf("发送邮件失败: %w", err)
	}

	n.logger.Info("邮件发送成功",
		zap.String("from", from),
		zap.String("to", to),
		zap.String("subject", subject),
	)

	return nil
}

// sendEmailByConfig 根据配置发送邮件通知（用于测试）
func (n *Notifier) sendEmailByConfig(ctx context.Context, config map[string]interface{}, message string) error {
	// 构造一个临时的 NotificationMessage 对象用于测试
	msg := NotificationMessage{
		Type:      "sms",
		From:      "测试发送方",
		Content:   message,
		Timestamp: time.Now().Unix(),
	}
	return n.sendEmail(ctx, config, msg)
}

// SendEmailByConfig 导出方法供外部调用（用于测试）
func (n *Notifier) SendEmailByConfig(ctx context.Context, config map[string]interface{}, message string) error {
	return n.sendEmailByConfig(ctx, config, message)
}

// SendEmail 发送邮件通知（通用方法）
func (n *Notifier) SendEmail(ctx context.Context, config map[string]interface{}, msg NotificationMessage) error {
	return n.sendEmail(ctx, config, msg)
}

func buildProxyURL(rawProxyURL string, username string, password string) (*url.URL, error) {
	u, err := url.Parse(rawProxyURL)
	if err != nil {
		return nil, err
	}

	if username != "" {
		u.User = url.UserPassword(username, password)
	}
	return u, nil
}
