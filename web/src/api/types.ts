// 短信记录
export interface TextMessage {
    id: string;
    from: string;
    to: string;
    content: string;
    type: 'incoming' | 'outgoing';
    status: 'received' | 'sending' | 'sent' | 'failed';
    timestamp: number;
    createdAt: number;
    updatedAt: number;
}

// 查询结果
export interface ListResult {
    total: number;
    items: TextMessage[];
}

// 统计信息
export interface Stats {
    totalCount: number;
    incomingCount: number;
    outgoingCount: number;
    todayCount: number;
}

// 发送短信请求
export interface SendSMSRequest {
    to: string;
    content: string;
}

// 设置蜂窝网络请求
export interface SetCellularRequest {
    enabled: boolean;
}

// 移动网络信息（来自 Lua 脚本的 get_mobile_info 函数）
export interface MobileInfo {
    sim_ready: boolean;          // SIM卡是否就绪
    iccid: string;               // SIM卡 ICCID
    imsi: string;                // IMSI
    rssi: number;                // 信号强度 (dBm)
    signal_level: number;        // 信号等级 (0-31)
    signal_desc: string;         // 信号描述 (强/中/弱/无信号)
    is_registered: boolean;      // 是否已注册网络
    is_roaming: boolean;         // 是否漫游
    operator: string;            // 运营商英文简称
    csq: number;
    rsrp: number;
    rsrq: number;
    number: number;
    uptime: string;              // 开机时长
}

// 设备状态响应（来自 Lua 脚本的 status_response）
export interface DeviceStatus {
    type: string;                // 消息类型: "status_response"
    timestamp: number;           // 时间戳
    mem_kb: number;              // 内存使用 (KB)
    cellular_enabled: boolean;   // 蜂窝网络是否启用
    mobile: MobileInfo;          // 移动网络信息
    port_name: string;           // 串口名称
    connected: boolean;          // 串口连接状态
    version: string;             // Lua 版本
}

// 手机号码响应
export interface PhoneNumberResponse {
    type: string;
    timestamp: number;
    phone_number: string;
}

// 会话信息
export interface Conversation {
    peer: string;              // 对方号码
    lastMessage: TextMessage;  // 最后一条消息
    messageCount: number;      // 消息总数
    unreadCount: number;       // 未读数量
}
