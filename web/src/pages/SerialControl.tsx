import {useState} from 'react';
import {Activity, RotateCcw, Send, Signal, Wifi} from 'lucide-react';
import {toast} from 'sonner';
import {useMutation, useQuery} from '@tanstack/react-query';
import * as serialApi from '../api/serial';
import {Input} from '@/components/ui/input';
import {Textarea} from '@/components/ui/textarea';
import {Button} from '@/components/ui/button';
import {Card, CardContent, CardHeader, CardTitle} from '@/components/ui/card';
import type {DeviceStatus} from '@/api/types';

export default function SerialControl() {
    const [to, setTo] = useState('');
    const [content, setContent] = useState('');

    // 获取设备状态（包含移动网络信息）- 每 30 秒自动刷新
    const {data: deviceStatus, isFetching} = useQuery({
        queryKey: ['deviceStatus'],
        queryFn: async () => {
            const res = await serialApi.getStatus();
            return res as DeviceStatus;
        },
        refetchInterval: 10000, // 每 10 秒自动刷新
    });

    // 发送短信 Mutation
    const sendSMSMutation = useMutation({
        mutationFn: (data: { to: string; content: string }) => serialApi.sendSMS(data),
        onSuccess: () => {
            toast.success('短信下发成功，等待确认...');
            setTo('');
            setContent('');
        },
        onError: (error) => {
            console.error('发送失败:', error);
            toast.error('发送失败');
        },
    });

    // 重启协议栈 Mutation
    const resetStackMutation = useMutation({
        mutationFn: () => serialApi.resetStack(),
        onSuccess: () => {
            toast.success('协议栈已重启');
        },
        onError: (error) => {
            console.error('操作失败:', error);
            toast.error('操作失败');
        },
    });

    // 重启模块 Mutation
    const rebootMcuMutation = useMutation({
        mutationFn: () => serialApi.rebootMcu(),
        onSuccess: () => {
            toast.success('模块重启命令已发送');
        },
        onError: (error) => {
            console.error('操作失败:', error);
            toast.error('操作失败');
        },
    });

    const handleSendSMS = async (e: React.FormEvent) => {
        e.preventDefault();
        if (!to || !content) {
            toast.warning('请输入手机号和短信内容');
            return;
        }
        sendSMSMutation.mutate({to, content});
    };

    // 从设备状态中获取移动网络信息
    const mobile = deviceStatus?.mobile;

    return (
        <div className="flex flex-col overflow-hidden">
            {/* 顶部标题 */}
            <div className="mb-6">
                <h1 className="text-2xl font-bold text-gray-900">串口控制</h1>
            </div>

            {/* 主内容区 - 三列布局 */}
            <div className="flex-1 grid grid-cols-1 lg:grid-cols-3 gap-4 min-h-0">
                {/* 左侧：移动网络信息 */}
                <Card className="flex flex-col min-h-0">
                    <CardHeader className="pb-3">
                        <CardTitle className="flex items-center gap-2 text-base">
                            <Signal className="w-4 h-4 text-blue-600"/>
                            移动网络信息
                        </CardTitle>
                    </CardHeader>
                    <CardContent className="flex-1 overflow-y-auto">
                        {mobile ? (
                            <div className="space-y-3">
                                <div className="flex justify-between items-center pb-2 border-b">
                                    <span className="text-xs text-gray-500">SIM 状态</span>
                                    <span className="text-sm font-medium">
                    {mobile.sim_ready ? (
                        <span className="text-green-600 flex items-center gap-1">
                        <div className="w-1.5 h-1.5 rounded-full bg-green-600"></div>
                        正常
                      </span>
                    ) : (
                        <span className="text-red-600 flex items-center gap-1">
                        <div className="w-1.5 h-1.5 rounded-full bg-red-600"></div>
                        未就绪
                      </span>
                    )}
                  </span>
                                </div>
                                <div className="flex justify-between items-center pb-2 border-b">
                                    <span className="text-xs text-gray-500">运营商</span>
                                    <span className="text-sm font-medium">
                                    {mobile.operator}
                                  </span>
                                </div>
                                <div className="flex justify-between items-center pb-2 border-b">
                                    <span className="text-xs text-gray-500">CSQ</span>
                                    <span className="text-sm font-medium">
                    {mobile.csq || mobile.signal_level} <span className="text-xs text-gray-400">({mobile.signal_desc})</span>
                  </span>
                                </div>
                                <div className="flex justify-between items-center pb-2 border-b">
                                    <span className="text-xs text-gray-500">RSSI</span>
                                    <span className="text-sm font-medium">{mobile.rssi} <span
                                        className="text-xs text-gray-400">dBm</span></span>
                                </div>
                                <div className="flex justify-between items-center pb-2 border-b">
                                    <span className="text-xs text-gray-500">RSRP</span>
                                    <span className="text-sm font-medium">{mobile.rsrp || 'N/A'} <span
                                        className="text-xs text-gray-400">dBm</span></span>
                                </div>
                                <div className="flex justify-between items-center pb-2 border-b">
                                    <span className="text-xs text-gray-500">RSRQ</span>
                                    <span className="text-sm font-medium">{mobile.rsrq || 'N/A'} <span
                                        className="text-xs text-gray-400">dB</span></span>
                                </div>
                                <div className="flex justify-between items-center pb-2 border-b">
                                    <span className="text-xs text-gray-500">网络注册</span>
                                    <span className="text-sm font-medium">
                                        {!mobile.is_registered ? (
                                            <span className="text-red-600">未注册</span>
                                        ) : mobile.is_roaming ? (
                                            <span className="text-yellow-600">已注册（漫游）</span>
                                        ) : (
                                            <span className="text-green-600">已注册</span>
                                        )}

                  </span>
                                </div>
                                <div className="pt-1">
                                    <div className="text-xs text-gray-500 mb-1">ICCID</div>
                                    <div
                                        className="font-mono text-xs bg-gray-50 p-1.5 rounded break-all">{mobile.iccid}</div>
                                </div>
                                <div className="pt-1">
                                    <div className="text-xs text-gray-500 mb-1">IMSI</div>
                                    <div
                                        className="font-mono text-xs bg-gray-50 p-1.5 rounded break-all">{mobile.imsi}</div>
                                </div>
                                {mobile.number && (
                                    <div className="pt-1">
                                        <div className="text-xs text-gray-500 mb-1">手机号</div>
                                        <div
                                            className="font-mono text-xs bg-gray-50 p-1.5 rounded break-all">{mobile.number}</div>
                                    </div>
                                )}

                            </div>
                        ) : (
                            <div className="flex flex-col items-center justify-center h-full text-gray-400">
                                <Wifi className="w-12 h-12 mb-2 opacity-30 animate-pulse"/>
                                <p className="text-sm">加载中...</p>
                            </div>
                        )}
                    </CardContent>
                </Card>

                {/* 中间：发送短信 */}
                <Card className="flex flex-col min-h-0">
                    <CardHeader className="pb-3">
                        <CardTitle className="flex items-center gap-2 text-base">
                            <Send className="w-4 h-4 text-green-600"/>
                            发送短信
                        </CardTitle>
                    </CardHeader>
                    <CardContent className="flex-1 flex flex-col">
                        <form onSubmit={handleSendSMS} className="flex flex-col h-full space-y-3">
                            <div>
                                <label className="block text-xs font-medium text-gray-700 mb-1.5">
                                    目标手机号
                                </label>
                                <Input
                                    type="tel"
                                    value={to}
                                    onChange={(e) => setTo(e.target.value)}
                                    placeholder="请输入手机号"
                                    className="h-9"
                                    required
                                />
                            </div>
                            <div className="flex-1 flex flex-col min-h-0">
                                <label className="block text-xs font-medium text-gray-700 mb-1.5">
                                    短信内容
                                </label>
                                <Textarea
                                    value={content}
                                    onChange={(e) => setContent(e.target.value)}
                                    placeholder="请输入短信内容"
                                    className="flex-1 resize-none"
                                    required
                                />
                            </div>
                            <Button
                                type="submit"
                                disabled={sendSMSMutation.isPending}
                                className="w-full bg-green-600 hover:bg-green-700 h-9"
                            >
                                <Send className="w-3.5 h-3.5 mr-2"/>
                                {sendSMSMutation.isPending ? '发送中...' : '发送短信'}
                            </Button>
                        </form>
                    </CardContent>
                </Card>

                {/* 右侧：设备状态 + 控制 */}
                <div className="flex flex-col gap-4 min-h-0">
                    {/* 设备状态 */}
                    {deviceStatus && (
                        <Card className="flex-1 flex flex-col min-h-0">
                            <CardHeader className="pb-3">
                                <CardTitle className="flex items-center gap-2 text-base">
                                    <Activity className="w-4 h-4 text-purple-600"/>
                                    设备状态
                                </CardTitle>
                            </CardHeader>
                            <CardContent className="flex-1 overflow-y-auto">
                                <div className="space-y-2">
                                    <div className="flex justify-between items-center pb-2 border-b">
                                        <span className="text-xs text-gray-500">串口连接</span>
                                        <span className="text-sm font-medium">
                                            {deviceStatus.connected ? (
                                                <span className="text-green-600 flex items-center gap-1">
                                                    <div className="w-1.5 h-1.5 rounded-full bg-green-600"></div>
                                                    已连接
                                                </span>
                                            ) : (
                                                <span className="text-red-600 flex items-center gap-1">
                                                    <div className="w-1.5 h-1.5 rounded-full bg-red-600"></div>
                                                    未连接
                                                </span>
                                            )}
                                        </span>
                                    </div>
                                    {deviceStatus.port_name && (
                                        <div className="flex justify-between items-center pb-2 border-b">
                                            <span className="text-xs text-gray-500">串口名称</span>
                                            <span className="text-sm font-medium font-mono">{deviceStatus.port_name}</span>
                                        </div>
                                    )}
                                    {deviceStatus.version && (
                                        <div className="flex justify-between items-center pb-2 border-b">
                                            <span className="text-xs text-gray-500">固件版本</span>
                                            <span className="text-sm font-medium font-mono text-blue-600">{deviceStatus.version}</span>
                                        </div>
                                    )}
                                    <div className="flex justify-between items-center pb-2 border-b">
                                        <span className="text-xs text-gray-500">时间戳</span>
                                        <span className="text-sm font-medium">
                                            {new Date(deviceStatus.timestamp * 1000).toLocaleString('zh-CN')}
                                        </span>
                                    </div>
                                    <div className="flex justify-between items-center pb-2 border-b">
                                        <span className="text-xs text-gray-500">开机时长</span>
                                        <span className="text-sm font-medium">
                                            {mobile.uptime}
                                        </span>
                                    </div>
                                    <div className="flex justify-between items-center pb-2 border-b">
                                        <span className="text-xs text-gray-500">内存使用</span>
                                        <span className="text-sm font-medium">{deviceStatus.mem_kb.toFixed(2)} KB</span>
                                    </div>
                                    <div className="flex justify-between items-center pb-2 border-b">
                                        <span className="text-xs text-gray-500">蜂窝网络</span>
                                        <span className="text-sm font-medium">
                                            {deviceStatus.cellular_enabled ? (
                                                <span className="text-green-600">已启用</span>
                                            ) : (
                                                <span className="text-orange-600">已禁用</span>
                                            )}
                                        </span>
                                    </div>
                                </div>
                            </CardContent>
                        </Card>
                    )}

                    {/* 设备控制 */}
                    <Card>
                        <CardHeader className="pb-3">
                            <CardTitle className="flex items-center gap-2 text-base">
                                <RotateCcw className="w-4 h-4 text-orange-600"/>
                                设备控制
                            </CardTitle>
                        </CardHeader>
                        <CardContent>
                            <div className="space-y-2">
                                <p className="text-xs text-gray-600">
                                    如果设备网络出现异常，可以尝试重启协议栈来恢复连接
                                </p>
                                <Button
                                    onClick={() => resetStackMutation.mutate()}
                                    disabled={resetStackMutation.isPending || isFetching}
                                    variant="outline"
                                    className="w-full border-orange-300 text-orange-700 hover:bg-orange-50 h-9"
                                >
                                    <RotateCcw className="w-3.5 h-3.5 mr-2"/>
                                    重启协议栈
                                </Button>
                                <Button
                                    onClick={() => rebootMcuMutation.mutate()}
                                    disabled={rebootMcuMutation.isPending || isFetching}
                                    variant="outline"
                                    className="w-full border-orange-300 text-orange-700 hover:bg-orange-50 h-9"
                                >
                                    <RotateCcw className="w-3.5 h-3.5 mr-2"/>
                                    重启模块
                                </Button>
                            </div>
                        </CardContent>
                    </Card>
                </div>
            </div>
        </div>
    );
}
