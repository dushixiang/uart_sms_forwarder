import type {ReactNode} from 'react';
import {Activity, Loader2, MessageSquareText, RotateCcw, Signal} from 'lucide-react';
import {Link} from 'react-router-dom';
import {toast} from 'sonner';
import {useMutation, useQuery} from '@tanstack/react-query';
import * as serialApi from '../api/serial';
import {Button} from '@/components/ui/button';
import {Card, CardContent, CardHeader, CardTitle} from '@/components/ui/card';
import type {DeviceStatus} from '@/api/types';
import {formatUptime} from '@/utils/utils.ts';
import {PageHeader} from '@/components/PageHeader';
import {cn} from '@/lib/utils';

interface InfoRowProps {
    label: string;
    value: ReactNode;
    mono?: boolean;
}

function InfoRow({label, value, mono = false}: InfoRowProps) {
    return (
        <div className="flex min-w-0 items-center justify-between gap-4 border-b border-slate-100 py-2.5 last:border-b-0">
            <dt className="shrink-0 text-xs font-medium text-slate-500">{label}</dt>
            <dd className={cn('min-w-0 truncate text-right text-xs font-semibold text-slate-800', mono && 'font-mono')}>
                {value}
            </dd>
        </div>
    );
}

interface StatusTileProps {
    label: string;
    value: string;
    tone?: 'blue' | 'green' | 'amber' | 'slate';
}

function StatusTile({label, value, tone = 'slate'}: StatusTileProps) {
    const toneClass = {
        blue: 'border-blue-100 bg-blue-50 text-blue-700',
        green: 'border-emerald-100 bg-emerald-50 text-emerald-700',
        amber: 'border-amber-200 bg-amber-50 text-amber-700',
        slate: 'border-slate-200 bg-slate-50 text-slate-600',
    }[tone];

    return (
        <div className={cn('rounded-xl border px-3.5 py-3', toneClass)}>
            <p className="text-[10px] font-semibold tracking-wide opacity-70">{label}</p>
            <p className="mt-1 truncate text-sm font-bold">{value}</p>
        </div>
    );
}

export default function SerialControl() {
    const {
        data: deviceStatus,
        isFetching,
        isLoading,
        refetch: refetchStatus,
    } = useQuery({
        queryKey: ['deviceStatus'],
        queryFn: async () => {
            const res = await serialApi.getStatus();
            return res as DeviceStatus;
        },
        refetchInterval: 10000,
    });

    const setFlymodeMutation = useMutation({
        mutationFn: (enabled: boolean) => serialApi.setFlymode(enabled),
        onSuccess: () => {
            toast.success('设置成功');
            refetchStatus();
        },
        onError: (error) => {
            console.error('操作失败:', error);
            toast.error('操作失败');
        },
    });

    const rebootMcuMutation = useMutation({
        mutationFn: () => serialApi.rebootMcu(),
        onSuccess: () => {
            toast.success('模块重启命令已发送');
            refetchStatus();
        },
        onError: (error) => {
            console.error('操作失败:', error);
            toast.error('操作失败');
        },
    });

    const mobile = deviceStatus?.mobile;
    const connected = Boolean(deviceStatus?.connected);
    const unavailable = '—';
    const displaySignal = (value?: number, unit = '') => connected && value ? `${value}${unit}` : unavailable;
    const registrationText = !connected
        ? unavailable
        : mobile?.is_registered
            ? mobile.is_roaming ? '已注册 · 漫游' : '已注册'
            : '未注册';

    const signalMetrics = [
        {label: 'CSQ', value: displaySignal(mobile?.csq || mobile?.signal_level)},
        {label: 'RSSI', value: displaySignal(mobile?.rssi, ' dBm')},
        {label: 'RSRP', value: displaySignal(mobile?.rsrp, ' dBm')},
        {label: 'RSRQ', value: displaySignal(mobile?.rsrq, ' dB')},
    ];

    return (
        <div>
            <PageHeader
                title="串口控制"
                description="查看移动网络与模块状态，或执行设备控制命令。"
            />

            <div className="mt-6 grid items-start gap-4 xl:grid-cols-[minmax(0,1.35fr)_minmax(320px,0.65fr)]">
                <Card className="gap-0 overflow-hidden py-0">
                    <CardHeader className="border-b border-slate-100 py-5">
                        <div className="flex items-center justify-between gap-4">
                            <div>
                                <CardTitle className="flex items-center gap-2 text-base">
                                    <Signal className="size-4 text-blue-600"/>
                                    网络与设备状态
                                </CardTitle>
                                <p className="mt-1.5 text-xs text-slate-500">蜂窝网络、信号参数与串口模块信息</p>
                            </div>
                            <span className={cn(
                                'inline-flex shrink-0 items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs font-semibold',
                                connected
                                    ? 'border-emerald-200 bg-emerald-50 text-emerald-700'
                                    : 'border-rose-200 bg-rose-50 text-rose-700',
                            )}>
                                <span className={cn('size-1.5 rounded-full', connected ? 'bg-emerald-500' : 'bg-rose-500')}/>
                                {connected ? '设备在线' : '设备离线'}
                            </span>
                        </div>
                    </CardHeader>

                    <CardContent className="py-5">
                        {isLoading ? (
                            <div className="flex min-h-80 items-center justify-center gap-2 text-sm text-slate-500">
                                <Loader2 className="size-4 animate-spin text-blue-600"/>
                                正在读取设备状态
                            </div>
                        ) : (
                            <div>
                                <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
                                    <StatusTile
                                        label="SIM 卡"
                                        value={connected ? mobile?.sim_ready ? '已就绪' : '未就绪' : unavailable}
                                        tone={connected && mobile?.sim_ready ? 'green' : 'slate'}
                                    />
                                    <StatusTile
                                        label="网络注册"
                                        value={registrationText}
                                        tone={connected && mobile?.is_registered ? 'blue' : 'slate'}
                                    />
                                    <StatusTile
                                        label="运营商"
                                        value={connected ? mobile?.operator || unavailable : unavailable}
                                        tone={connected ? 'blue' : 'slate'}
                                    />
                                    <StatusTile
                                        label="飞行模式"
                                        value={connected ? deviceStatus?.flymode ? '已开启' : '已关闭' : unavailable}
                                        tone={connected && deviceStatus?.flymode ? 'amber' : connected ? 'green' : 'slate'}
                                    />
                                </div>

                                <section className="mt-5">
                                    <div className="mb-2.5 flex items-center justify-between">
                                        <h3 className="text-xs font-bold text-slate-800">信号参数</h3>
                                        <span className="text-[10px] font-medium text-slate-400">
                                            {connected ? mobile?.signal_desc || '实时数据' : '等待设备连接'}
                                        </span>
                                    </div>
                                    <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
                                        {signalMetrics.map((metric) => (
                                            <div key={metric.label} className="rounded-lg border border-blue-100 bg-blue-50/60 px-3 py-2.5">
                                                <p className="font-mono text-sm font-bold text-blue-950">{metric.value}</p>
                                                <p className="mt-1 text-[9px] font-semibold tracking-wider text-blue-500">{metric.label}</p>
                                            </div>
                                        ))}
                                    </div>
                                </section>

                                <section className="mt-5 border-t border-slate-100 pt-4">
                                    <h3 className="mb-1 text-xs font-bold text-slate-800">模块信息</h3>
                                    <dl className="grid gap-x-8 sm:grid-cols-2">
                                        <InfoRow label="串口设备" value={connected ? deviceStatus?.port_name || unavailable : unavailable} mono/>
                                        <InfoRow label="固件版本" value={connected ? deviceStatus?.version || unavailable : unavailable} mono/>
                                        <InfoRow
                                            label="设备时间"
                                            value={connected && deviceStatus?.timestamp
                                                ? new Date(deviceStatus.timestamp * 1000).toLocaleString('zh-CN')
                                                : unavailable}
                                        />
                                        <InfoRow
                                            label="开机时长"
                                            value={connected && mobile?.uptime ? formatUptime(mobile.uptime) : unavailable}
                                        />
                                        <InfoRow
                                            label="内存使用"
                                            value={connected && deviceStatus ? `${deviceStatus.mem_kb.toFixed(2)} KB` : unavailable}
                                        />
                                        <InfoRow
                                            label="本机号码"
                                            value={connected ? mobile?.number || unavailable : unavailable}
                                            mono
                                        />
                                    </dl>

                                    <div className="mt-3 grid gap-2 sm:grid-cols-2">
                                        {[
                                            ['ICCID', connected ? mobile?.iccid || unavailable : unavailable],
                                            ['IMSI', connected ? mobile?.imsi || unavailable : unavailable],
                                        ].map(([label, value]) => (
                                            <div key={label} className="min-w-0 rounded-lg bg-slate-50 px-3.5 py-3">
                                                <p className="text-[10px] font-semibold text-slate-400">{label}</p>
                                                <p className="mt-1 truncate font-mono text-xs font-semibold text-slate-700" title={String(value)}>{value}</p>
                                            </div>
                                        ))}
                                    </div>
                                </section>
                            </div>
                        )}
                    </CardContent>
                </Card>

                <Card className="gap-0 py-0 xl:sticky xl:top-[84px]">
                    <CardHeader className="border-b border-slate-100 py-5">
                        <CardTitle className="flex items-center gap-2 text-base">
                            <Activity className="size-4 text-blue-600"/>
                            设备控制
                        </CardTitle>
                        <p className="mt-1.5 text-xs text-slate-500">切换蜂窝网络状态或重启模块</p>
                    </CardHeader>
                    <CardContent className="py-5">
                        <dl className="mb-4 rounded-xl border border-slate-200 bg-slate-50 px-3.5">
                            <InfoRow
                                label="串口连接"
                                value={<span className={connected ? 'text-emerald-600' : 'text-rose-600'}>{connected ? '已连接' : '未连接'}</span>}
                            />
                            <InfoRow
                                label="飞行模式"
                                value={connected ? deviceStatus?.flymode ? '已开启' : '已关闭' : unavailable}
                            />
                        </dl>
                        <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-1 2xl:grid-cols-2">
                            <Button
                                onClick={() => setFlymodeMutation.mutate(!deviceStatus?.flymode)}
                                disabled={!connected || setFlymodeMutation.isPending || isFetching}
                                className="h-10 bg-blue-600 text-white hover:bg-blue-700"
                            >
                                {setFlymodeMutation.isPending ? <Loader2 className="size-4 animate-spin"/> : <Signal className="size-4"/>}
                                {deviceStatus?.flymode ? '关闭飞行模式' : '开启飞行模式'}
                            </Button>
                            <Button
                                onClick={() => rebootMcuMutation.mutate()}
                                disabled={!connected || rebootMcuMutation.isPending || isFetching}
                                variant="outline"
                                className="h-10 border-rose-200 text-rose-600 hover:bg-rose-50 hover:text-rose-700"
                            >
                                {rebootMcuMutation.isPending ? <Loader2 className="size-4 animate-spin"/> : <RotateCcw className="size-4"/>}
                                重启模块
                            </Button>
                        </div>
                        {!connected && <p className="mt-3 text-xs leading-5 text-slate-400">设备连接后才能执行控制操作。</p>}

                        <div className="mt-5 border-t border-slate-100 pt-4">
                            <p className="mb-3 text-xs leading-5 text-slate-500">发送和回复短信已统一移动到短信中心。</p>
                            <Button variant="outline" className="h-10 w-full border-blue-200 text-blue-700 hover:bg-blue-50" asChild>
                                <Link to="/messages?compose=1">
                                    <MessageSquareText className="size-4"/>
                                    前往短信中心
                                </Link>
                            </Button>
                        </div>
                    </CardContent>
                </Card>
            </div>
        </div>
    );
}
