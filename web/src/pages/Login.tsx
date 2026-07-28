import {type FormEvent, useEffect, useState} from 'react';
import {useNavigate} from 'react-router-dom';
import {ArrowRight, KeyRound, LoaderCircle, Lock, RadioTower, ShieldCheck, User, Zap} from 'lucide-react';
import {Button} from '@/components/ui/button';
import {Input} from '@/components/ui/input';
import {getAuthConfig, getOIDCAuthURL, login as loginApi, type AuthConfig} from '@/api/auth';
import {toast} from 'sonner';

const highlights = [
    {icon: RadioTower, text: '持续监控设备与蜂窝网络状态'},
    {icon: Zap, text: '实时转发短信和来电通知'},
    {icon: ShieldCheck, text: '统一管理自动化与通知渠道'},
];

export default function Login() {
    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');
    const [loading, setLoading] = useState(false);
    const [authConfig, setAuthConfig] = useState<AuthConfig | null>(null);
    const [configLoading, setConfigLoading] = useState(true);
    const navigate = useNavigate();

    useEffect(() => {
        getAuthConfig()
            .then(setAuthConfig)
            .catch((error) => {
                console.error('获取认证配置失败', error);
                toast.error('获取认证配置失败');
            })
            .finally(() => setConfigLoading(false));
    }, []);

    const handleLogin = async (event: FormEvent<HTMLFormElement>) => {
        event.preventDefault();
        if (loading || !username || !password) return;

        setLoading(true);
        try {
            const response = await loginApi({username, password});
            localStorage.setItem('token', response.token);
            localStorage.setItem('username', response.username);
            toast.success('登录成功');
            navigate('/');
        } catch (error) {
            toast.error('登录失败：' + (error instanceof Error ? error.message : '未知错误'));
        } finally {
            setLoading(false);
        }
    };

    const handleOIDCLogin = async () => {
        try {
            setLoading(true);
            const {authUrl} = await getOIDCAuthURL();
            window.location.href = authUrl;
        } catch {
            toast.error('获取 OIDC 认证 URL 失败');
            setLoading(false);
        }
    };

    return (
        <main className="min-h-screen bg-[#f4f7f9] p-3 sm:p-5 lg:p-6">
            <div className="mx-auto grid min-h-[calc(100vh-24px)] max-w-[1440px] overflow-hidden rounded-[28px] border border-slate-200/80 bg-white shadow-none sm:min-h-[calc(100vh-40px)] lg:min-h-[calc(100vh-48px)] lg:grid-cols-[1.03fr_0.97fr]">
                <section className="login-visual relative hidden overflow-hidden bg-[#0b2a55] p-10 text-white lg:flex lg:flex-col xl:p-14">
                    <div className="relative z-10 flex items-center gap-3">
                        <div className="flex size-11 items-center justify-center rounded-2xl bg-blue-300/10 ring-1 ring-blue-300/20">
                            <img src="/logo.png" alt="" className="size-8"/>
                        </div>
                        <div>
                            <p className="text-sm font-semibold tracking-wide">UART Messenger</p>
                            <p className="mt-0.5 text-[10px] uppercase tracking-[0.2em] text-slate-500">Device Console</p>
                        </div>
                    </div>

                    <div className="relative z-10 my-auto max-w-xl py-14">
                        <span className="inline-flex items-center gap-2 rounded-full border border-blue-300/20 bg-blue-300/10 px-3 py-1.5 text-xs font-semibold text-blue-200">
                            <span className="size-1.5 rounded-full bg-blue-300"/> 可靠的设备通信中枢
                        </span>
                        <h1 className="mt-7 text-5xl font-semibold leading-[1.08] tracking-[-0.045em] xl:text-[58px]">
                            每一条消息，<br/><span className="text-blue-300">都准时抵达。</span>
                        </h1>
                        <p className="mt-6 max-w-lg text-base leading-7 text-slate-400">
                            在一个清晰、安全的控制台内管理蜂窝模块，掌握短信收发、串口链路与自动化任务。
                        </p>
                        <div className="mt-10 grid gap-4">
                            {highlights.map(({icon: Icon, text}) => (
                                <div key={text} className="flex items-center gap-3 text-sm text-slate-300">
                                    <span className="flex size-8 items-center justify-center rounded-lg bg-white/[0.06] text-blue-300 ring-1 ring-white/[0.07]">
                                        <Icon className="size-4"/>
                                    </span>
                                    {text}
                                </div>
                            ))}
                        </div>
                    </div>

                    <div className="relative z-10 flex items-center justify-between text-[11px] text-slate-600">
                        <span>UART SMS FORWARDER</span>
                        <span>SECURE ACCESS</span>
                    </div>
                </section>

                <section className="relative flex items-center justify-center px-6 py-12 sm:px-12 lg:px-16 xl:px-24">
                    <div className="w-full max-w-[430px]">
                        <div className="mb-10 flex items-center gap-3 lg:hidden">
                            <div className="flex size-11 items-center justify-center rounded-2xl bg-slate-900">
                                <img src="/logo.png" alt="" className="size-8"/>
                            </div>
                            <div>
                                <p className="text-sm font-bold text-slate-900">UART Messenger</p>
                                <p className="text-[10px] uppercase tracking-[0.17em] text-slate-400">Device Console</p>
                            </div>
                        </div>

                        <div>
                            <p className="text-xs font-semibold uppercase tracking-[0.16em] text-blue-700">欢迎回来</p>
                            <h2 className="mt-3 text-3xl font-bold tracking-[-0.04em] text-slate-950 sm:text-4xl">登录控制台</h2>
                            <p className="mt-3 text-sm leading-6 text-slate-500">验证身份后即可访问设备状态和消息服务。</p>
                        </div>

                        <div className="mt-9">
                            {configLoading ? (
                                <div className="flex min-h-56 flex-col items-center justify-center gap-3 rounded-2xl border border-slate-200 bg-slate-50/60">
                                    <LoaderCircle className="size-6 animate-spin text-blue-700"/>
                                    <p className="text-sm font-medium text-slate-500">正在加载登录方式...</p>
                                </div>
                            ) : !authConfig?.passwordEnabled && !authConfig?.oidcEnabled ? (
                                <div className="rounded-2xl border border-amber-200 bg-amber-50 p-6 text-center">
                                    <KeyRound className="mx-auto size-7 text-amber-600"/>
                                    <p className="mt-3 text-sm font-semibold text-amber-900">尚未配置登录方式</p>
                                    <p className="mt-1 text-xs leading-5 text-amber-700">请先在服务端启用密码或 OIDC 登录。</p>
                                </div>
                            ) : (
                                <div className="space-y-6">
                                    {authConfig?.passwordEnabled && (
                                        <form onSubmit={handleLogin} className="space-y-5">
                                            <div className="space-y-2">
                                                <label htmlFor="username" className="block text-sm font-semibold text-slate-700">用户名</label>
                                                <div className="relative">
                                                    <User className="pointer-events-none absolute left-4 top-1/2 size-[18px] -translate-y-1/2 text-slate-400"/>
                                                    <Input
                                                        id="username"
                                                        type="text"
                                                        placeholder="请输入用户名"
                                                        value={username}
                                                        onChange={(event) => setUsername(event.target.value)}
                                                        className="h-12 rounded-xl border-slate-200 bg-slate-50/60 pl-11 shadow-none transition focus-visible:bg-white focus-visible:ring-blue-600/15"
                                                        disabled={loading}
                                                        required
                                                        autoComplete="username"
                                                        autoFocus
                                                    />
                                                </div>
                                            </div>

                                            <div className="space-y-2">
                                                <label htmlFor="password" className="block text-sm font-semibold text-slate-700">密码</label>
                                                <div className="relative">
                                                    <Lock className="pointer-events-none absolute left-4 top-1/2 size-[18px] -translate-y-1/2 text-slate-400"/>
                                                    <Input
                                                        id="password"
                                                        type="password"
                                                        placeholder="请输入密码"
                                                        value={password}
                                                        onChange={(event) => setPassword(event.target.value)}
                                                        className="h-12 rounded-xl border-slate-200 bg-slate-50/60 pl-11 shadow-none transition focus-visible:bg-white focus-visible:ring-blue-600/15"
                                                        disabled={loading}
                                                        required
                                                        autoComplete="current-password"
                                                    />
                                                </div>
                                            </div>

                                            <Button
                                                type="submit"
                                                className="group h-12 w-full cursor-pointer rounded-xl bg-slate-900 text-sm font-semibold text-white shadow-none transition hover:bg-slate-800"
                                                disabled={loading || !username || !password}
                                            >
                                                {loading ? <><LoaderCircle className="size-4 animate-spin"/> 正在登录</> : <>登录 <ArrowRight className="size-4 transition-transform group-hover:translate-x-0.5"/></>}
                                            </Button>
                                        </form>
                                    )}

                                    {authConfig?.passwordEnabled && authConfig?.oidcEnabled && (
                                        <div className="flex items-center gap-3">
                                            <span className="h-px flex-1 bg-slate-200"/>
                                            <span className="text-[11px] font-medium uppercase tracking-wider text-slate-400">或使用</span>
                                            <span className="h-px flex-1 bg-slate-200"/>
                                        </div>
                                    )}

                                    {authConfig?.oidcEnabled && (
                                        <Button
                                            type="button"
                                            variant="outline"
                                            className="h-12 w-full cursor-pointer rounded-xl border-slate-200 bg-white text-sm font-semibold text-slate-700 shadow-none hover:bg-slate-50"
                                            onClick={handleOIDCLogin}
                                            disabled={loading}
                                        >
                                            <KeyRound className="size-4"/> 通过 OIDC 登录
                                        </Button>
                                    )}
                                </div>
                            )}
                        </div>

                        <p className="mt-9 text-center text-xs leading-5 text-slate-400">登录即表示你正在访问受保护的设备管理服务。</p>
                    </div>
                </section>
            </div>
        </main>
    );
}
