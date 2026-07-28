import {useEffect, useState} from 'react';
import {Link} from 'react-router-dom';
import {Cable, Clock3, MessageSquareText, Radio, RefreshCw, Send, Signal, Smartphone} from 'lucide-react';
import {useQuery} from '@tanstack/react-query';
import {getStats} from '../api/messages';
import type {DeviceStatus, Stats} from '../api/types';
import {StatCard} from '@/components/StatsCard.tsx';
import {getStatus} from '@/api/serial.ts';
import {cn} from '@/lib/utils.ts';
import {PageHeader} from '@/components/PageHeader';

const describeSignal = (rsrp?: number) => {
    if (!rsrp) return '等待数据';
    if (rsrp >= -80) return '优秀';
    if (rsrp >= -90) return '良好';
    if (rsrp >= -100) return '一般';
    if (rsrp >= -110) return '较弱';
    return '很弱';
};

export default function Dashboard() {
    const [stats, setStats] = useState<Stats | null>(null);
    const [statsLoading, setStatsLoading] = useState(true);

    useEffect(() => {
        const loadStats = async () => {
            try {
                setStats(await getStats());
            } catch (error) {
                console.error('获取统计信息失败:', error);
            } finally {
                setStatsLoading(false);
            }
        };

        loadStats();
        const interval = window.setInterval(loadStats, 30000);
        return () => window.clearInterval(interval);
    }, []);

    const {data: deviceStatus, dataUpdatedAt, isFetching} = useQuery<DeviceStatus>({
        queryKey: ['deviceStatus'],
        queryFn: async () => getStatus() as Promise<DeviceStatus>,
        refetchInterval: 10000,
    });

    const mobile = deviceStatus?.mobile;
    const connected = Boolean(deviceStatus?.connected);
    const signalPercentage = connected && mobile?.rsrp
        ? Math.max(0, Math.min(100, Math.round(((mobile.rsrp + 140) / 96) * 100)))
        : 0;
    const signalDescription = connected ? describeSignal(mobile?.rsrp) : '等待设备连接';
    const unavailable = '—';

    const deviceDetails = [
        {label: '串口设备', value: connected ? deviceStatus?.port_name || unavailable : unavailable},
        {label: '固件版本', value: connected ? deviceStatus?.version || unavailable : unavailable},
        {label: 'SIM 卡', value: connected ? mobile?.sim_ready ? '已就绪' : '未就绪' : unavailable},
        {label: '网络注册', value: connected ? mobile?.is_registered ? '已注册' : '未注册' : unavailable},
        {label: '飞行模式', value: connected ? deviceStatus?.flymode ? '已开启' : '已关闭' : unavailable},
        {label: '运行内存', value: connected && deviceStatus?.mem_kb ? `${deviceStatus.mem_kb.toFixed(1)} KB` : unavailable},
    ];

    if (statsLoading) {
        return (
            <div className="grid min-h-[55vh] place-items-center">
                <div className="flex items-center gap-3 rounded-xl border border-blue-100 bg-blue-50 px-5 py-3 text-sm font-medium text-blue-700">
                    <RefreshCw className="size-4 animate-spin"/>
                    正在同步设备数据
                </div>
            </div>
        );
    }

    return (
        <div>
            <PageHeader
                title="设备概览"
                description="查看通信链路、蜂窝网络与短信服务的实时状态"
                action={<div className="flex flex-wrap items-center gap-2">
                    <span className="mr-1 hidden text-xs text-slate-400 md:inline">
                        {isFetching ? '正在同步' : `更新于 ${dataUpdatedAt ? new Date(dataUpdatedAt).toLocaleTimeString('zh-CN', {hour: '2-digit', minute: '2-digit'}) : '—'}`}
                    </span>
                    <Link to="/messages" className="inline-flex h-9 items-center gap-2 rounded-lg border border-slate-200 bg-white px-3.5 text-xs font-semibold text-slate-700 transition-colors hover:border-blue-200 hover:bg-blue-50 hover:text-blue-700">
                        <MessageSquareText className="size-4"/>短信记录
                    </Link>
                    <Link to="/serial" className="inline-flex h-9 items-center gap-2 rounded-lg bg-blue-600 px-3.5 text-xs font-semibold text-white transition-colors hover:bg-blue-700">
                        <Send className="size-4"/>发送短信
                    </Link>
                </div>}
            />

            <section className="mt-6 grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
                <StatCard
                    label="连接状态"
                    value={connected ? '在线' : '离线'}
                    icon={Cable}
                    colorClass={connected ? 'bg-blue-50 text-blue-700' : 'bg-rose-50 text-rose-600'}
                    subValue={connected ? deviceStatus?.port_name || '串口已连接' : '等待串口连接'}
                />
                <StatCard
                    label="信号质量"
                    value={connected ? signalPercentage : unavailable}
                    unit={connected ? '%' : undefined}
                    icon={Signal}
                    colorClass="bg-blue-50 text-blue-700"
                    subValue={connected ? `${signalDescription} · ${mobile?.rsrp || unavailable} dBm` : signalDescription}
                />
                <StatCard
                    label="短信总量"
                    value={stats?.totalCount || 0}
                    icon={MessageSquareText}
                    colorClass="bg-blue-50 text-blue-700"
                    subValue={`接收 ${stats?.incomingCount || 0} · 发出 ${stats?.outgoingCount || 0}`}
                />
                <StatCard
                    label="今日短信"
                    value={stats?.todayCount || 0}
                    icon={Clock3}
                    colorClass="bg-blue-600 text-white"
                    subValue="今日累计收发"
                />
            </section>

            <section className="mt-4 grid items-start gap-4 lg:grid-cols-[minmax(0,1.55fr)_minmax(280px,0.65fr)]">
                <article className="overflow-hidden rounded-2xl border border-slate-200 bg-white">
                    <div className="flex items-center justify-between border-b border-slate-100 px-5 py-3.5">
                        <div className="flex items-center gap-3">
                            <span className="flex size-9 items-center justify-center rounded-xl bg-blue-50 text-blue-700">
                                <Radio className="size-[18px]"/>
                            </span>
                            <div>
                                <h2 className="text-sm font-bold text-slate-900">移动网络与设备</h2>
                                <p className="text-xs text-slate-400">{connected ? mobile?.operator || '等待运营商信息' : '设备当前未连接'}</p>
                            </div>
                        </div>
                        <span className={cn(
                            'rounded-full px-2.5 py-1 text-[11px] font-semibold',
                            connected && mobile?.is_registered ? 'bg-blue-50 text-blue-700' : 'bg-slate-100 text-slate-500',
                        )}>
                            {connected && mobile?.is_registered ? '网络已注册' : '未注册'}
                        </span>
                    </div>

                    <div className="grid gap-5 p-5 md:grid-cols-[0.85fr_1.15fr]">
                        <div className="rounded-xl border border-blue-100 bg-blue-50/70 p-4">
                            <div className="flex items-start justify-between">
                                <div>
                                    <p className="text-xs font-semibold text-blue-700">实时信号</p>
                                    <div className="mt-1.5 flex items-end gap-2">
                                        <span className="text-3xl font-bold tracking-[-0.05em] text-blue-950">{connected ? signalPercentage : unavailable}</span>
                                        {connected && <span className="mb-0.5 text-sm font-semibold text-blue-500">%</span>}
                                    </div>
                                    <p className="mt-1 text-xs font-medium text-blue-600">{signalDescription}</p>
                                </div>
                                <Signal className="size-5 text-blue-500"/>
                            </div>

                            <div className="mt-5">
                                <div className="flex h-1.5 overflow-hidden rounded-full bg-blue-100">
                                    <div className="rounded-full bg-blue-600 transition-all" style={{width: `${signalPercentage}%`}}/>
                                </div>
                                <div className="mt-4 grid grid-cols-3 divide-x divide-blue-200 text-center">
                                    {[
                                        ['RSRP', connected ? mobile?.rsrp || unavailable : unavailable],
                                        ['RSRQ', connected ? mobile?.rsrq || unavailable : unavailable],
                                        ['CSQ', connected ? mobile?.csq || unavailable : unavailable],
                                    ].map(([label, value]) => (
                                        <div key={label}>
                                            <p className="font-mono text-sm font-bold text-blue-950">{value}</p>
                                            <p className="mt-0.5 text-[9px] font-semibold tracking-wider text-blue-400">{label}</p>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        </div>

                        <dl className="grid content-start grid-cols-2 gap-x-6">
                            {deviceDetails.map((item) => (
                                <div key={item.label} className="min-w-0 border-b border-slate-100 py-2.5 first:pt-0 [&:nth-child(2)]:pt-0">
                                    <dt className="text-[11px] font-medium text-slate-400">{item.label}</dt>
                                    <dd className="mt-1 truncate font-mono text-xs font-semibold text-slate-800">{item.value}</dd>
                                </div>
                            ))}
                            {connected && mobile?.number && (
                                <div className="col-span-2 mt-3 flex items-center gap-3 rounded-lg bg-slate-50 px-3.5 py-2.5">
                                    <Smartphone className="size-4 text-blue-600"/>
                                    <div className="min-w-0">
                                        <p className="text-[10px] text-slate-400">本机号码</p>
                                        <p className="truncate font-mono text-xs font-bold text-slate-800">{mobile.number}</p>
                                    </div>
                                </div>
                            )}
                        </dl>
                    </div>
                </article>

                <article className="rounded-2xl border border-slate-200 bg-white p-5">
                    <div className="flex items-center justify-between">
                        <div>
                            <h2 className="text-sm font-bold text-slate-900">服务状态</h2>
                            <p className="mt-1 text-xs text-slate-400">通信服务实时检查</p>
                        </div>
                        <span className={cn('size-2 rounded-full', connected ? 'bg-emerald-500' : 'bg-slate-300')}/>
                    </div>
                    <div className="mt-4 divide-y divide-slate-100">
                        {[
                            ['短信监听服务', connected],
                            ['移动网络注册', Boolean(connected && mobile?.is_registered)],
                            ['SIM 卡状态', Boolean(connected && mobile?.sim_ready)],
                        ].map(([label, healthy]) => (
                            <div key={String(label)} className="flex items-center justify-between py-3 text-xs first:pt-0 last:pb-0">
                                <span className="text-slate-500">{label}</span>
                                <span className={cn('flex items-center gap-1.5 font-semibold', healthy ? 'text-emerald-600' : 'text-slate-400')}>
                                    <span className={cn('size-1.5 rounded-full', healthy ? 'bg-emerald-500' : 'bg-slate-300')}/>
                                    {healthy ? '正常' : '等待'}
                                </span>
                            </div>
                        ))}
                    </div>
                </article>
            </section>
        </div>
    );
}
