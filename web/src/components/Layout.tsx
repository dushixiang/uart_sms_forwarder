import {Link, Outlet, useLocation, useNavigate} from 'react-router-dom';
import {Bell, Clock, LayoutDashboard, LogOut, MessageSquare, Plane, Smartphone} from 'lucide-react';
import {Button} from "@/components/ui/button.tsx";
import {useQuery} from "@tanstack/react-query";
import {getVersion} from "@/api/property.ts";
import {getStatus} from "@/api/serial.ts";
import type {DeviceStatus} from "@/api/types.ts";
import {cn} from "@/lib/utils.ts";
import {toast} from 'sonner';

export default function Layout() {
    const location = useLocation();
    const navigate = useNavigate();

    const navigation = [
        {name: '统计面板', href: '/', icon: LayoutDashboard},
        {name: '短信记录', href: '/messages', icon: MessageSquare},
        {name: '串口控制', href: '/serial', icon: Smartphone},
        {name: '通知渠道', href: '/notifications', icon: Bell},
        {name: '计划任务', href: '/scheduled-tasks', icon: Clock},
        {name: '自动飞行', href: '/auto-flymode', icon: Plane},
    ];

    // 获取版本信息
    const versionQuery = useQuery({
        queryKey: ['version'],
        queryFn: getVersion,
    });

    // 获取设备状态 - 每 10 秒自动刷新
    const {data: deviceStatus} = useQuery<DeviceStatus>({
        queryKey: ['deviceStatus'],
        queryFn: async () => {
            const res = await getStatus();
            return res as DeviceStatus;
        },
        refetchInterval: 10000,
    });

    const isActive = (path: string) => {
        if (path === '/') {
            return location.pathname === '/';
        }
        return location.pathname.startsWith(path);
    };

    const handleLogout = () => {
        // 清除 localStorage
        localStorage.removeItem('token');
        localStorage.removeItem('username');

        toast.success('已退出登录');
        navigate('/login');
    };

    // 计算信号强度百分比（信号等级 0-31 转换为 0-100%）
    const getSignalPercentage = () => {
        if (!deviceStatus?.mobile?.signal_level) return 0;
        return Math.round((deviceStatus.mobile.signal_level / 31) * 100);
    };

    // 获取信号强度颜色
    const getSignalColor = () => {
        const percentage = getSignalPercentage();
        if (percentage >= 70) return 'text-green-600';
        if (percentage >= 40) return 'text-yellow-600';
        return 'text-red-600';
    };

    // 缩短串口名称显示
    const getShortPortName = (portName: string) => {
        if (!portName) return '未连接';
        // 如果是 /dev/ttyUSB0 这样的路径，只显示最后部分
        const parts = portName.split('/');
        return parts[parts.length - 1];
    };

    return (
        <div className="min-h-screen bg-gradient-to-br from-gray-50 to-gray-100 flex flex-col">
            {/* 顶部导航栏 */}
            <nav className="bg-white/95 backdrop-blur-sm border-b border-gray-200 sticky top-0 z-50">
                <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
                    <div className="flex justify-between h-16">
                        {/* 左侧：Logo 和导航 */}
                        <div className="flex items-center space-x-4 lg:space-x-8">
                            {/* Logo */}
                            <div className="flex items-center space-x-2 lg:space-x-3 flex-shrink-0">
                                <img src={'/logo.png'} alt="UART 短信转发器" className="w-6 h-6"/>
                                <div className="hidden sm:flex flex-col">
                                    <h1 className="text-base lg:text-lg font-bold leading-tight bg-gradient-to-r from-gray-900 to-gray-700 bg-clip-text text-transparent">
                                        UART 短信转发器
                                    </h1>
                                </div>
                            </div>

                            {/* 桌面端导航 */}
                            <div className="hidden md:flex items-center space-x-1">
                                {navigation.map((item) => {
                                    const Icon = item.icon;
                                    const active = isActive(item.href);
                                    return (
                                        <Link
                                            key={item.name}
                                            to={item.href}
                                            className={`px-2 lg:px-3 xl:px-4 py-2 flex items-center space-x-1 lg:space-x-2 rounded-lg transition-all duration-200 font-medium text-xs lg:text-sm whitespace-nowrap ${
                                                active
                                                    ? 'bg-blue-50 text-blue-600'
                                                    : 'text-gray-500 hover:bg-gray-100 hover:text-gray-900'
                                            }`}
                                        >
                                            <Icon className="w-4 h-4 flex-shrink-0"/>
                                            <span className="hidden lg:inline">{item.name}</span>
                                        </Link>
                                    );
                                })}
                            </div>
                        </div>

                        {/* 右侧：设备状态和用户信息 */}
                        <div className="hidden md:flex items-center space-x-2 lg:space-x-4">
                            <div className="flex flex-col">
                                <div className="flex items-center gap-2">
                                    <div className={cn(
                                        "w-2 h-2  rounded-full animate-pulse",
                                        deviceStatus?.connected ? 'bg-green-500' : 'bg-red-500',
                                    )}/>
                                    <div className={'text-xs font-medium text-gray-600'}>
                                        {deviceStatus?.connected ? '设备在线' : '设备离线'}
                                    </div>
                                </div>
                                <div className="text-[10px] text-gray-400 font-mono mt-0.5">
                                    {deviceStatus?.port_name}
                                </div>
                            </div>

                            {/* 登出按钮 */}
                            <Button
                                variant="ghost"
                                size="sm"
                                onClick={handleLogout}
                                className="text-gray-600 hover:text-gray-900 hover:bg-gray-100 rounded-lg"
                            >
                                <LogOut className="w-4 h-4 mr-2"/>
                                登出
                            </Button>
                        </div>

                        {/* 移动端用户菜单 */}
                        <div className="flex md:hidden items-center space-x-2">
                            {deviceStatus?.connected && (
                                <div className="flex items-center space-x-1 px-2 py-1 bg-green-50 rounded-lg">
                                    <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse"/>
                                    <span className="text-xs font-medium text-green-700">在线</span>
                                </div>
                            )}
                            <Button
                                variant="ghost"
                                size="sm"
                                onClick={handleLogout}
                                className="text-gray-600"
                            >
                                <LogOut className="w-4 h-4"/>
                            </Button>
                        </div>
                    </div>
                </div>

                {/* 移动端底部导航 */}
                <div className="md:hidden border-t border-gray-200 bg-white">
                    <div className="flex overflow-x-auto py-2">
                        {navigation.map((item) => {
                            const Icon = item.icon;
                            const active = isActive(item.href);
                            return (
                                <Link
                                    key={item.name}
                                    to={item.href}
                                    className={`flex min-w-[72px] flex-1 flex-col items-center px-2 py-2 text-xs font-medium transition-all duration-200 ${
                                        active ? 'text-blue-600' : 'text-gray-500'
                                    }`}
                                >
                                    <Icon className={`w-6 h-6 mb-1 transition-transform ${active ? 'scale-110' : ''}`}/>
                                    <span className={active ? 'font-semibold' : ''}>{item.name}</span>
                                </Link>
                            );
                        })}
                    </div>
                </div>
            </nav>

            {/* 主要内容区域 */}
            <main className="flex-1 max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6 w-full">
                <Outlet/>
            </main>

            {/* 页脚 */}
            <footer className="bg-white/80 backdrop-blur-sm border-t border-gray-200 mt-auto">
                <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
                    <div className="text-center text-xs text-gray-500">
                        <p>UART 短信转发器 © 2025 版权所有 dushixiang · 版本 {versionQuery.data?.version}</p>
                    </div>
                </div>
            </footer>
        </div>
    );
}
