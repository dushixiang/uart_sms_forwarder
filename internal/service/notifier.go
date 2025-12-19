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
func (n *Notifier) sendFeishu(ctx context.Context, webhook, message string) error {
	body := map[string]interface{}{
		"msg_type": "text",
		"content": map[string]string{
			"text": message,
		},
	}

	_, err := n.sendJSONRequest(ctx, webhook, body)
	if err != nil {
		return err
	}
	return nil
}

// sendCustomWebhook 发送自定义Webhook
func (n *Notifier) sendCustomWebhook(ctx context.Context, config map[string]interface{}, sms IncomingSMS) error {
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
			v = sms.From
		case "content":
			v = sms.Content
		case "timestamp":
			timestamp := time.Unix(sms.Timestamp, 0).Format(time.DateTime)
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

	return n.sendFeishu(ctx, webhook, message)
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

// SendWebhookByConfig 导出方法供外部调用（测试用）
func (n *Notifier) SendWebhookByConfig(ctx context.Context, config map[string]interface{}, sms IncomingSMS) error {
	return n.sendCustomWebhook(ctx, config, sms)
}

// sendEmail 发送邮件通知
func (n *Notifier) sendEmail(ctx context.Context, config map[string]interface{}, sms IncomingSMS) error {
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
		subject = "收到新短信 - {{from}}"
	}

	// 模板变量替换函数
	replaceVars := func(template string) string {
		t := fasttemplate.New(template, "{{", "}}")
		return t.ExecuteFuncString(func(w io.Writer, tag string) (int, error) {
			var v string
			switch tag {
			case "from":
				v = sms.From
			case "content":
				v = sms.Content
			case "timestamp":
				v = time.Unix(sms.Timestamp, 0).Format(time.DateTime)
			default:
				return w.Write([]byte("{{" + tag + "}}"))
			}
			return w.Write([]byte(v))
		})
	}

	// 替换主题中的变量
	subject = replaceVars(subject)

	// 构造邮件内容
	body := sms.String()

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

// sendEmailByConfig 根据配置发送邮件通知
func (n *Notifier) sendEmailByConfig(ctx context.Context, config map[string]interface{}, message string) error {
	// 构造一个临时的 IncomingSMS 对象用于测试
	sms := IncomingSMS{
		From:      "测试发送方",
		Content:   message,
		Timestamp: time.Now().Unix(),
	}
	return n.sendEmail(ctx, config, sms)
}

// SendEmailByConfig 导出方法供外部调用
func (n *Notifier) SendEmailByConfig(ctx context.Context, config map[string]interface{}, message string) error {
	return n.sendEmailByConfig(ctx, config, message)
}

// SendEmailBySMS 根据短信内容发送邮件（用于实际短信转发）
func (n *Notifier) SendEmailBySMS(ctx context.Context, config map[string]interface{}, sms IncomingSMS) error {
	return n.sendEmail(ctx, config, sms)
}
