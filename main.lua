-- =================================================================================
-- PROJECT: UART SMS Forwarder
-- DEVICE:  Air780EHV
-- VERSION: 1.0.1
-- 协议说明：
--   上行（MCU -> 模块）：CMD_START:{json}:CMD_END
--   下行（模块 -> MCU）：SMS_START:{json}:SMS_END
-- =================================================================================

PROJECT = "uart_sms_forwarder"
VERSION = "1.0.1"

log.info("main", PROJECT, VERSION)

-- 1. 引入必要库
sys = require("sys")

-- 2. 全局配置与变量
-- [注意] 如果接单片机物理引脚，通常是 uart.UART_1；如果是USB调试，用 uart.VUART_0
local uartid = uart.VUART_0
local max_buffer_size = 50
local msg_buffer = {}
local uart_recv_buffer = ""
local cellular_enabled = true
local call_ring_count = 0  -- 来电响铃计数

-- ========== 关键：禁用自动数据连接 ==========
mobile.setAuto(0)

-- 3. 看门狗
if wdt then
    wdt.init(9000)
    sys.timerLoopStart(wdt.feed, 3000)
end

uart.setup(uartid, 115200, 8, 1)
log.info("System", "UART 初始化成功")

-- =================================================================================
-- 工具函数区
-- =================================================================================

function get_mobile_info()
    local info = {}
    -- 使用 status 判断：0=未注册 1=已注册 2=搜索中 3=拒绝 5=漫游注册
    local net_stat = mobile.status()
    local iccid = mobile.iccid()
    info.sim_ready = (iccid ~= nil and iccid ~= "" and iccid ~= "unknown")
    info.iccid = iccid or "unknown"
    info.imsi = mobile.imsi() or "unknown"
    info.number = mobile.number(0) or ""  -- 获取手机号，可能为空

    -- 获取信号强度指标
    local csq = mobile.csq() or 0 -- 范围 0-31，越大越好
    info.csq = csq
    info.rssi = mobile.rssi() or -113  -- 范围 0到-114，值越大越好
    info.rsrp = mobile.rsrp() or -140  -- 范围 -44到-140，值越大越好 (4G模块)
    info.rsrq = mobile.rsrq() or -20   -- 范围 -3到-19.5，值越大越好 (4G模块)

    -- 根据 CSQ 判断信号等级（仅供参考，4G模块应参考rsrp/rsrq）
    if csq == 0 or csq == 99 then
        info.signal_level = 0
        info.signal_desc = "无信号"
    else
        info.signal_level = csq
        info.signal_desc = csq >= 20 and "强" or (csq >= 10 and "中" or "弱")
    end

    info.is_registered = (net_stat == 1 or net_stat == 5)
    info.is_roaming = net_stat == 5

    local ticks = mcu.ticks()
    local total_sec = ticks / 1000
    local h = math.floor(total_sec / 3600)
    local m = math.floor((total_sec % 3600) / 60)
    local s = math.floor(total_sec % 60)
    info.uptime = string.format("%02d时%02d分%02d秒", h, m, s) -- 格式化时:分:秒

    return info
end

function send_to_uart(data)
    local ok, json_str = pcall(json.encode, data)
    if ok and json_str then
        uart.write(uartid, "SMS_START:" .. json_str .. ":SMS_END\r\n")
        return true
    else
        log.error("UART", "JSON Encode Failed", json_str)
        return false
    end
end

function process_uart_command(cmd_data)
    if not cmd_data.action then
        send_to_uart({type = "error", msg = "missing action"})
        return
    end

    if cmd_data.action == "send_sms" and cmd_data.to and cmd_data.content then
        local request_id = cmd_data.request_id or os.time()
        local to = cmd_data.to
        local content = cmd_data.content
        -- 在协程中同步发送短信
        sys.taskInit(function()
            log.info("CMD", "发送短信 ->", to)
            local result = sms.sendLong(to, content).wait()
            send_to_uart({
                type = "sms_send_result",
                success = result == true,
                request_id = request_id,
                to = to,
                timestamp = os.time()
            })
        end)

    elseif cmd_data.action == "get_status" then
        send_to_uart({
            type = "status_response",
            timestamp = os.time(),
            mem_kb = math.floor(collectgarbage("count")),
            cellular_enabled = cellular_enabled,
            version = VERSION,
            mobile = get_mobile_info()
        })

    elseif cmd_data.action == "set_cellular" and cmd_data.enabled ~= nil then
        -- 规范化为布尔值：兼容 true/false、1/0、"true"/"false"
        -- Lua 中 0 也是真值，必须显式转换
        local enabled = (cmd_data.enabled == true or cmd_data.enabled == 1 or
                         cmd_data.enabled == "true" or cmd_data.enabled == "1")

        if enabled then
            mobile.flymode(0)
            mobile.setAuto(0) -- 再次确保不自动拨号
            cellular_enabled = true
        else
            mobile.flymode(1)
            cellular_enabled = false
        end
        send_to_uart({
            type = "cmd_response",
            action = "set_cellular",
            result = "ok",
            enabled = cellular_enabled
        })

    elseif cmd_data.action == "reset_stack" then
        log.info("CMD", "重启协议栈")
        mobile.reset()
        mobile.setAuto(0)
        send_to_uart({type = "cmd_response", action = "reset_stack", result = "ok"})

    elseif cmd_data.action == "reboot_mcu" then
        log.info("CMD", "重启模块")
        send_to_uart({type = "cmd_response", action = "reboot_mcu", result = "ok"})
        pm.reboot()
    else
        send_to_uart({type = "error", msg = "unknown command"})
    end
end

-- =================================================================================
-- 事件监听区
-- =================================================================================

sys.subscribe("SMS_INC", function(phone, content)
    log.info("Event", "收到短信:", phone)
    local msg = {
        type = "incoming_sms",
        timestamp = os.time(),
        from = phone,
        content = content
    }
    table.insert(msg_buffer, msg)
    if #msg_buffer > max_buffer_size then
        table.remove(msg_buffer, 1) -- 移除旧的
    end
    sys.publish("NEW_MSG_IN_BUFFER")
end)

sys.subscribe("SIM_IND", function(status)
    send_to_uart({type = "sim_event", status = status})
end)

-- 来电事件处理
sys.subscribe("CC_IND", function(state)
    if state == "READY" then
        log.info("Call", "通话准备完成")

    elseif state == "INCOMINGCALL" then
        -- 有电话呼入
        if call_ring_count == 0 then
            log.info("Call", "检测到来电")
            local phone_num = cc.lastNum()
            log.info("Call", "来电号码:", phone_num or "unknown")

            -- 转发来电通知到 UART
            send_to_uart({
                type = "incoming_call",
                timestamp = os.time(),
                from = phone_num or "unknown"
            })
        end

        call_ring_count = call_ring_count + 1

        -- 响4声后自动挂断（可根据需求调整）
        if call_ring_count > 3 then
            log.info("Call", "自动挂断来电")
            cc.hangUp()
        end

    elseif state == "DISCONNECTED" then
        -- 电话被挂断
        log.info("Call", "通话结束")
        call_ring_count = 0
        send_to_uart({
            type = "call_disconnected",
            timestamp = os.time()
        })
    end
end)

-- =================================================================================
-- 任务循环区
-- =================================================================================

uart.on(uartid, "receive", function(id, len)
    local chunk = uart.read(id, len)
    if not chunk then return end

    uart_recv_buffer = uart_recv_buffer .. chunk

    -- 使用与下行一致的包围标志：CMD_START:{json}:CMD_END
    while true do
        local start_pos = uart_recv_buffer:find("CMD_START:", 1, true)
        if not start_pos then break end

        local end_pos = uart_recv_buffer:find(":CMD_END", start_pos + 10, true)
        if not end_pos then break end  -- 数据未接收完整，等待下次

        -- 提取 JSON 部分
        local json_str = uart_recv_buffer:sub(start_pos + 10, end_pos - 1)
        -- 移除已处理的数据
        uart_recv_buffer = uart_recv_buffer:sub(end_pos + 8)

        if #json_str > 0 then
            local success, cmd = pcall(json.decode, json_str)
            if success and cmd then
                process_uart_command(cmd)
            else
                log.warn("UART", "JSON解析失败:", json_str:sub(1, 50))
                send_to_uart({type="error", msg="Invalid JSON"})
            end
        end
    end

    -- 溢出保护：如果缓冲区过大且找不到有效包，清空
    if #uart_recv_buffer > 4096 then
        log.error("UART", "Buffer Overflow - 清空缓冲区")
        uart_recv_buffer = ""
        send_to_uart({type="error", msg="Buffer overflow, cleared"})
    end
end)

sys.taskInit(function()
    while true do
        if #msg_buffer == 0 then
            sys.waitUntil("NEW_MSG_IN_BUFFER")
        end
        while #msg_buffer > 0 do
            local msg = table.remove(msg_buffer, 1)
            if msg then
                send_to_uart(msg)
                sys.wait(50)
            end
        end
        if collectgarbage("count") > 1024 then
            collectgarbage("collect")
        end
    end
end)

sys.taskInit(function()
    sys.wait(5000)
    send_to_uart({
        type = "system_ready",
        project = PROJECT,
        version = VERSION,
        data_disabled = true
    })
    while true do
        sys.wait(60000)
        local info = get_mobile_info()
        send_to_uart({
            type = "heartbeat",
            rssi = info.rssi,
            signal_level = info.signal_level,
            signal_desc = info.signal_desc,
            net_reg = info.is_registered,
            cellular_enabled = cellular_enabled,
            sim_ready = info.sim_ready,
            mem = math.floor(collectgarbage("count"))
        })
    end
end)

sys.run()
