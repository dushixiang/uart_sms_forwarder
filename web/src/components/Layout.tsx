import {useEffect, useState} from 'react';
import {Link, Outlet, useLocation, useNavigate} from 'react-router-dom';
import {
    Bell,
    ChevronRight,
    Clock3,
    LayoutDashboard,
    LogOut,
    Menu,
    MessageSquareText,
    Plane,
    RadioTower,
    Smartphone,
    X,
} from 'lucide-react';
import {useQuery} from '@tanstack/react-query';
import {toast} from 'sonner';
import {getVersion} from '@/api/property.ts';
import {getStatus} from '@/api/serial.ts';
import type {DeviceStatus} from '@/api/types.ts';
import {cn} from '@/lib/utils.ts';

const navigation = [
    {name: '概览', description: '运行状态与数据', href: '/', icon: LayoutDashboard},
    {name: '短信中心', description: '短信收发与历史', href: '/messages', icon: MessageSquareText},
    {name: '串口控制', description: '设备与短信下发', href: '/serial', icon: Smartphone},
    {name: '通知渠道', description: '管理消息推送', href: '/notifications', icon: Bell},
    {name: '计划任务', description: '自动执行任务', href: '/scheduled-tasks', icon: Clock3},
    {name: '自动飞行', description: '飞行模式策略', href: '/auto-flymode', icon: Plane},
];

export default function Layout() {
    const location = useLocation();
    const navigate = useNavigate();
    const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

    const versionQuery = useQuery({
        queryKey: ['version'],
        queryFn: getVersion,
    });

    const {data: deviceStatus} = useQuery<DeviceStatus>({
        queryKey: ['deviceStatus'],
        queryFn: async () => getStatus() as Promise<DeviceStatus>,
        refetchInterval: 10000,
    });

    useEffect(() => {
        document.body.style.overflow = mobileMenuOpen ? 'hidden' : '';
        return () => {
            document.body.style.overflow = '';
        };
    }, [mobileMenuOpen]);

    const activeItem = navigation.find((item) =>
        item.href === '/' ? location.pathname === '/' : location.pathname.startsWith(item.href),
    ) ?? navigation[0];

    const handleLogout = () => {
        localStorage.removeItem('token');
        localStorage.removeItem('username');
        toast.success('已安全退出');
        navigate('/login');
    };

    const sidebar = (
        <div className="flex h-full flex-col bg-[#0b2a55] text-white">
            <div className="flex h-[72px] items-center gap-3 border-b border-white/[0.07] px-4">
                <div className="relative flex size-10 items-center justify-center rounded-xl bg-blue-400/10 ring-1 ring-blue-300/20">
                    <img src="/logo.png" alt="" className="size-7 object-contain"/>
                    <span className="absolute -right-0.5 -top-0.5 size-2.5 rounded-full border-2 border-[#0b2a55] bg-emerald-400"/>
                </div>
                <div className="min-w-0">
                    <div className="truncate text-sm font-semibold tracking-wide">UART 短信转发器</div>
                    <div className="mt-0.5 text-[10px] font-medium tracking-[0.12em] text-blue-100/70">设备控制台</div>
                </div>
                <button
                    type="button"
                    aria-label="关闭导航"
                    onClick={() => setMobileMenuOpen(false)}
                    className="ml-auto flex size-9 items-center justify-center rounded-xl text-slate-400 transition hover:bg-white/10 hover:text-white lg:hidden"
                >
                    <X className="size-5"/>
                </button>
            </div>

            <div className="flex-1 overflow-y-auto px-3 py-5">
                <p className="mb-2.5 px-2.5 text-[10px] font-semibold tracking-[0.14em] text-blue-100/60">控制中心</p>
                <nav className="space-y-1" aria-label="主导航">
                    {navigation.map((item) => {
                        const Icon = item.icon;
                        const active = activeItem.href === item.href;
                        return (
                            <Link
                                key={item.href}
                                to={item.href}
                                onClick={() => setMobileMenuOpen(false)}
                                aria-current={active ? 'page' : undefined}
                                className={cn(
                                    'group flex items-center gap-2.5 rounded-xl px-2.5 py-2 transition-colors duration-200',
                                    active
                                        ? 'bg-white text-slate-950'
                                        : 'text-blue-50/90 hover:bg-white/[0.08] hover:text-white',
                                )}
                            >
                                <span className={cn(
                                    'flex size-8 shrink-0 items-center justify-center rounded-lg transition-colors',
                                    active ? 'bg-blue-50 text-blue-700' : 'bg-white/[0.07] text-blue-100/80 group-hover:text-white',
                                )}>
                                    <Icon className="size-4"/>
                                </span>
                                <span className="min-w-0 flex-1">
                                    <span className="block text-sm font-semibold">{item.name}</span>
                                    <span className={cn('mt-0.5 block truncate text-[10px]', active ? 'text-slate-500' : 'text-blue-100/60')}>
                                        {item.description}
                                    </span>
                                </span>
                                <ChevronRight className={cn('size-4 transition', active ? 'text-slate-400' : 'opacity-0 group-hover:opacity-100')}/>
                            </Link>
                        );
                    })}
                </nav>
            </div>

            <div className="space-y-2 border-t border-white/[0.07] p-3">
                <div className="rounded-xl bg-white/[0.055] px-3 py-2.5 ring-1 ring-white/[0.06]">
                    <div className="flex items-center justify-between gap-3">
                        <div className="flex items-center gap-2">
                            <span className={cn(
                                'relative flex size-2.5 rounded-full',
                                deviceStatus?.connected ? 'bg-emerald-400' : 'bg-rose-400',
                            )}>
                                {deviceStatus?.connected && <span className="absolute inset-0 animate-ping rounded-full bg-emerald-400 opacity-50"/>}
                            </span>
                            <div>
                                <p className="text-[11px] font-semibold text-slate-100">
                                    {deviceStatus?.connected ? '设备运行中' : '设备未连接'}
                                </p>
                                <p className="mt-0.5 max-w-[130px] truncate font-mono text-[9px] text-blue-100/60">
                                    {deviceStatus?.port_name || '等待串口连接'}
                                </p>
                            </div>
                        </div>
                        <RadioTower className={cn('size-4', deviceStatus?.connected ? 'text-blue-300' : 'text-blue-200/25')}/>
                    </div>
                </div>
                <div className="flex items-center justify-between px-1">
                    <span className="text-[10px] text-blue-100/55">{versionQuery.data?.version ? `v${versionQuery.data.version}` : 'DEV'}</span>
                    <button
                        type="button"
                        onClick={handleLogout}
                        className="flex items-center gap-1.5 rounded-lg px-2 py-1.5 text-xs font-medium text-slate-400 transition hover:bg-white/[0.06] hover:text-white"
                    >
                        <LogOut className="size-3.5"/>
                        退出登录
                    </button>
                </div>
            </div>
        </div>
    );

    return (
        <div className="min-h-screen bg-[#f3f6f9] text-slate-900">
            <aside className="fixed inset-y-0 left-0 z-40 hidden w-[248px] lg:block">{sidebar}</aside>

            {mobileMenuOpen && (
                <div className="fixed inset-0 z-50 lg:hidden">
                    <button
                        type="button"
                        aria-label="关闭导航遮罩"
                        onClick={() => setMobileMenuOpen(false)}
                        className="absolute inset-0 bg-slate-950/55 backdrop-blur-sm"
                    />
                    <aside className="absolute inset-y-0 left-0 w-[min(86vw,320px)] border-r border-white/10">{sidebar}</aside>
                </div>
            )}

            <div className="min-h-screen lg:pl-[248px]">
                <header className="sticky top-0 z-30 border-b border-slate-200/80 bg-white/85 backdrop-blur-xl">
                    <div className="flex h-15 items-center gap-3 px-4 sm:px-6 lg:px-8 xl:px-10">
                        <button
                            type="button"
                            aria-label="打开导航"
                            onClick={() => setMobileMenuOpen(true)}
                            className="flex size-10 items-center justify-center rounded-xl border border-slate-200 bg-white text-slate-700 transition hover:border-slate-300 hover:bg-slate-50 lg:hidden"
                        >
                            <Menu className="size-5"/>
                        </button>
                        <div className="flex min-w-0 items-center gap-2 text-xs font-medium text-slate-400">
                            <span>控制中心</span>
                            <ChevronRight className="size-3"/>
                            <span className="text-slate-700">{activeItem.name}</span>
                        </div>
                        <div className="ml-auto flex items-center gap-3">
                            <div className={cn(
                                'hidden items-center gap-2 rounded-full border px-3 py-1.5 text-xs font-semibold sm:flex',
                                deviceStatus?.connected
                                    ? 'border-emerald-200 bg-emerald-50 text-emerald-700'
                                    : 'border-rose-200 bg-rose-50 text-rose-700',
                            )}>
                                <span className={cn('size-1.5 rounded-full', deviceStatus?.connected ? 'bg-emerald-500' : 'bg-rose-500')}/>
                                {deviceStatus?.connected ? '链路正常' : '链路中断'}
                            </div>
                            <div className="flex size-8 items-center justify-center rounded-full bg-[#0b2a55] text-[11px] font-bold text-white">
                                {(localStorage.getItem('username') || 'U').slice(0, 1).toUpperCase()}
                            </div>
                        </div>
                    </div>
                </header>

                <main className={cn(
                    'app-content mx-auto w-full max-w-[1320px] px-4 py-6 sm:px-6 lg:px-8 xl:px-10',
                    location.pathname === '/' && 'lg:h-[calc(100vh-60px)] lg:overflow-hidden',
                )}>
                    <Outlet/>
                </main>
            </div>
        </div>
    );
}
