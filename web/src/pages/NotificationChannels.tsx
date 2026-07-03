import {useEffect, useState} from 'react';
import {Bell, CheckCircle2, Link, Loader2, Mail, MessageSquare, Save, Send, Shield, TestTube} from 'lucide-react';
import {useMutation, useQuery, useQueryClient} from '@tanstack/react-query';
import {toast} from 'sonner';
import {Button} from '@/components/ui/button';
import {Input} from '@/components/ui/input';
import {Card, CardContent, CardDescription, CardHeader, CardTitle} from '@/components/ui/card';
import {Select, SelectContent, SelectItem, SelectTrigger, SelectValue,} from '@/components/ui/select';
import {Textarea} from '@/components/ui/textarea';
import {
    getNotificationChannels,
    type NotificationChannel,
    saveNotificationChannels,
    testNotificationChannel
} from "@/api/property.ts";

interface FormValues {
    // 钉钉
    dingtalkEnabled: boolean;
    dingtalkSecretKey: string;
    dingtalkSignSecret: string;

    // 企业微信
    wecomEnabled: boolean;
    wecomSecretKey: string;

    // 飞书
    feishuEnabled: boolean;
    feishuSecretKey: string;
    feishuSignSecret: string;

    // Webhook
    webhookEnabled: boolean;
    webhookUrl: string;
    webhookMethod: string;
    webhookContentType: string;
    webhookHeaders: string;
    webhookBody: string;

    // 邮件
    emailEnabled: boolean;
    emailSmtpHost: string;
    emailSmtpPort: string;
    emailUsername: string;
    emailPassword: string;
    emailFrom: string;
    emailTo: string;
    emailSubject: string;

    //telegram
    telegramlEnabled: boolean;
    telegramApiToken: string;
    telegramUserid: string;
    telegramProxyEnabled: boolean
    telegramProxyUrl: string
    telegramProxyUsername: string
    telegramProxyPassword: string

    // Bark
    barkEnabled: boolean;
    barkServerUrl: string;
    barkDeviceKey: string;

    // Gotify
    gotifyEnabled: boolean;
    gotifyServerUrl: string;
    gotifyToken: string;
}

export default function NotificationChannels() {
    const queryClient = useQueryClient();
    const [formValues, setFormValues] = useState<FormValues>({
        dingtalkEnabled: false,
        dingtalkSecretKey: '',
        dingtalkSignSecret: '',
        wecomEnabled: false,
        wecomSecretKey: '',
        feishuEnabled: false,
        feishuSecretKey: '',
        feishuSignSecret: '',
        webhookEnabled: false,
        webhookUrl: '',
        webhookMethod: 'POST',
        webhookContentType: 'application/json; charset=utf-8',
        webhookHeaders: '',
        webhookBody: '{"from": "{{from}}", "content": "{{content}}", "timestamp": "{{timestamp}}"}',
        emailEnabled: false,
        emailSmtpHost: '',
        emailSmtpPort: '587',
        emailUsername: '',
        emailPassword: '',
        emailFrom: '',
        emailTo: '',
        emailSubject: '收到新短信 - {{from}}',
        telegramlEnabled: false,
        telegramApiToken: '',
        telegramUserid: '',
        telegramProxyEnabled: false,
        telegramProxyUrl: '',
        telegramProxyUsername: '',
        telegramProxyPassword: '',
        barkEnabled: false,
        barkServerUrl: '',
        barkDeviceKey: '',
        gotifyEnabled: false,
        gotifyServerUrl: '',
        gotifyToken: '',
    });

    // 获取通知渠道列表
    const {data: channels = [], isLoading} = useQuery({
        queryKey: ['notificationChannels'],
        queryFn: getNotificationChannels,
    });

    // 保存 mutation
    const saveMutation = useMutation({
        mutationFn: saveNotificationChannels,
        onSuccess: () => {
            toast.success('保存成功');
            queryClient.invalidateQueries({queryKey: ['notificationChannels']});
        },
        onError: (error: unknown) => {
            console.error('保存失败:', error);
            toast.error('保存失败');
        },
    });

    // 测试 mutation
    const testMutation = useMutation({
        mutationFn: testNotificationChannel,
        onSuccess: () => {
            toast.success('测试通知已发送，请检查对应渠道');
        },
        onError: (error: unknown) => {
            console.error('测试失败:', error);
            toast.error('测试失败，请检查配置');
        },
    });

    // 将渠道数组转换为表单值
    useEffect(() => {
        if (channels.length > 0) {
            const newFormValues: FormValues = {...formValues};

            channels.forEach((channel) => {
                if (channel.type === 'dingtalk') {
                    newFormValues.dingtalkEnabled = channel.enabled;
                    newFormValues.dingtalkSecretKey = (channel.config?.secretKey as string) || '';
                    newFormValues.dingtalkSignSecret = (channel.config?.signSecret as string) || '';
                } else if (channel.type === 'wecom') {
                    newFormValues.wecomEnabled = channel.enabled;
                    newFormValues.wecomSecretKey = (channel.config?.secretKey as string) || '';
                } else if (channel.type === 'feishu') {
                    newFormValues.feishuEnabled = channel.enabled;
                    newFormValues.feishuSecretKey = (channel.config?.secretKey as string) || '';
                    newFormValues.feishuSignSecret = (channel.config?.signSecret as string) || '';
                } else if (channel.type === 'webhook') {
                    newFormValues.webhookEnabled = channel.enabled;
                    newFormValues.webhookUrl = (channel.config?.url as string) || '';
                    newFormValues.webhookMethod = (channel.config?.method as string) || 'POST';
                    newFormValues.webhookContentType = (channel.config?.contentType as string) || 'application/json; charset=utf-8';
                    newFormValues.webhookBody = (channel.config?.body as string) || '{"from": "{{from}}", "content": "{{content}}", "timestamp": "{{timestamp}}"}';

                    // 解析 headers 为 JSON 字符串
                    const headers = channel.config?.headers || {};
                    newFormValues.webhookHeaders = JSON.stringify(headers, null, 2);
                } else if (channel.type === 'email') {
                    newFormValues.emailEnabled = channel.enabled;
                    newFormValues.emailSmtpHost = (channel.config?.smtpHost as string) || '';
                    newFormValues.emailSmtpPort = (channel.config?.smtpPort as string) || '587';
                    newFormValues.emailUsername = (channel.config?.username as string) || '';
                    newFormValues.emailPassword = (channel.config?.password as string) || '';
                    newFormValues.emailFrom = (channel.config?.from as string) || '';
                    newFormValues.emailTo = (channel.config?.to as string) || '';
                    newFormValues.emailSubject = (channel.config?.subject as string) || '收到新短信 - {{from}}';
                } else if (channel.type === 'telegram') {
                    newFormValues.telegramlEnabled = channel.enabled;
                    newFormValues.telegramApiToken = (channel.config?.apiToken as string) || '';
                    newFormValues.telegramUserid = (channel.config?.userid as string) || '';
                    newFormValues.telegramProxyEnabled = (channel.config?.proxyEnabled as boolean)||false;
                    newFormValues.telegramProxyUrl = (channel.config?.proxyUrl as string) || '';
                    newFormValues.telegramProxyUsername = (channel.config?.proxyUsername as string) || '';
                    newFormValues.telegramProxyPassword = (channel.config?.proxyPassword as string) || '';
                } else if (channel.type === 'bark') {
                    newFormValues.barkEnabled = channel.enabled;
                    newFormValues.barkServerUrl = (channel.config?.serverUrl as string) || '';
                    newFormValues.barkDeviceKey = (channel.config?.deviceKey as string) || '';
                } else if (channel.type === 'gotify') {
                    newFormValues.gotifyEnabled = channel.enabled;
                    newFormValues.gotifyServerUrl = (channel.config?.serverUrl as string) || '';
                    newFormValues.gotifyToken = (channel.config?.token as string) || '';
                }
            });

            setFormValues(newFormValues);
        }
    }, [channels]);

    // 更新表单字段
    const updateField = (field: keyof FormValues, value: any) => {
        setFormValues((prev) => ({...prev, [field]: value}));
    };

    // 保存配置
    const handleSave = async () => {
        const newChannels: NotificationChannel[] = [];

        // 钉钉
        if (formValues.dingtalkEnabled || formValues.dingtalkSecretKey) {
            newChannels.push({
                type: 'dingtalk',
                enabled: formValues.dingtalkEnabled,
                config: {
                    secretKey: formValues.dingtalkSecretKey,
                    signSecret: formValues.dingtalkSignSecret,
                },
            });
        }

        // 企业微信
        if (formValues.wecomEnabled || formValues.wecomSecretKey) {
            newChannels.push({
                type: 'wecom',
                enabled: formValues.wecomEnabled,
                config: {
                    secretKey: formValues.wecomSecretKey,
                },
            });
        }

        // 飞书
        if (formValues.feishuEnabled || formValues.feishuSecretKey) {
            newChannels.push({
                type: 'feishu',
                enabled: formValues.feishuEnabled,
                config: {
                    secretKey: formValues.feishuSecretKey,
                    signSecret: formValues.feishuSignSecret,
                },
            });
        }

        // Webhook
        if (formValues.webhookEnabled || formValues.webhookUrl) {
            let headers = {};
            if (formValues.webhookHeaders) {
                try {
                    headers = JSON.parse(formValues.webhookHeaders);
                } catch (err) {
                    toast.error('Webhook Headers JSON 格式错误');
                    return;
                }
            }

            newChannels.push({
                type: 'webhook',
                enabled: formValues.webhookEnabled,
                config: {
                    url: formValues.webhookUrl,
                    method: formValues.webhookMethod,
                    contentType: formValues.webhookContentType,
                    body: formValues.webhookBody,
                    headers: Object.keys(headers).length > 0 ? headers : undefined,
                },
            });
        }

        // 邮件
        if (formValues.emailEnabled || formValues.emailSmtpHost) {
            newChannels.push({
                type: 'email',
                enabled: formValues.emailEnabled,
                config: {
                    smtpHost: formValues.emailSmtpHost,
                    smtpPort: formValues.emailSmtpPort,
                    username: formValues.emailUsername,
                    password: formValues.emailPassword,
                    from: formValues.emailFrom,
                    to: formValues.emailTo,
                    subject: formValues.emailSubject,
                },
            });
        }

        if (formValues.telegramlEnabled||formValues.telegramApiToken) {
            if (formValues.telegramProxyEnabled && !formValues.telegramProxyUrl) {
                toast.error('已启用 HTTP 代理，但未填写代理地址')
                return
            }

            newChannels.push({
                type:'telegram',
                enabled:formValues.telegramlEnabled,
                config: {
                    apiToken: formValues.telegramApiToken,
                    userid: formValues.telegramUserid,
                    proxyEnabled: formValues.telegramProxyEnabled,
                    proxyUrl: formValues.telegramProxyUrl,
                    proxyUsername: formValues.telegramProxyUsername,
                    proxyPassword: formValues.telegramProxyPassword,
                }
            })
        }

        // Bark
        if (formValues.barkEnabled || formValues.barkServerUrl) {
            newChannels.push({
                type: 'bark',
                enabled: formValues.barkEnabled,
                config: {
                    serverUrl: formValues.barkServerUrl,
                    deviceKey: formValues.barkDeviceKey,
                },
            });
        }

        // Gotify
        if (formValues.gotifyEnabled || formValues.gotifyServerUrl) {
            newChannels.push({
                type: 'gotify',
                enabled: formValues.gotifyEnabled,
                config: {
                    serverUrl: formValues.gotifyServerUrl,
                    token: formValues.gotifyToken,
                },
            });
        }

        saveMutation.mutate(newChannels);
    };

    if (isLoading) {
        return (
            <div className="flex justify-center items-center py-20">
                <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
            </div>
        );
    }

    return (
        <div className="space-y-8 animate-in fade-in duration-300">
            <div className="border-b border-gray-200 pb-5">
                <h1 className="text-2xl font-bold bg-gradient-to-r from-gray-900 to-gray-700 bg-clip-text text-transparent flex items-center gap-3">
                    通知渠道管理
                </h1>
                <p className="text-sm text-gray-500 mt-3">配置第三方消息推送渠道，当收到短信或设备异常时自动推送通知</p>
            </div>

            <div className="grid grid-cols-1 gap-6">
                {/* 钉钉通知 */}
                <Card
                    className={`border transition-all ${formValues.dingtalkEnabled ? 'border-blue-200 bg-gradient-to-br from-white to-blue-50/20' : 'border-gray-200 opacity-95'}`}>
                    <CardHeader className="border-b border-gray-100 bg-white/50">
                        <div className="flex items-center justify-between">
                            <div className="flex items-center space-x-3 flex-1">
                                <div
                                    className={`w-12 h-12 rounded-lg flex items-center justify-center ${formValues.dingtalkEnabled ? 'bg-blue-50 text-blue-600' : 'bg-gray-100 text-gray-400'}`}>
                                    <Bell size={24}/>
                                </div>
                                <div className="flex-1">
                                    <div className="flex items-center space-x-2">
                                        <CardTitle className="text-lg font-bold text-gray-800">钉钉通知</CardTitle>
                                        <div
                                            className={`w-2 h-2 rounded-full ${formValues.dingtalkEnabled ? 'bg-green-500' : 'bg-gray-300'}`}></div>
                                        <span
                                            className="text-xs text-gray-500">{formValues.dingtalkEnabled ? '已启用' : '未启用'}</span>
                                    </div>
                                    <CardDescription className="mt-1.5 text-xs">
                                        了解更多：
                                        <a
                                            href="https://open.dingtalk.com/document/robots/custom-robot-access"
                                            target="_blank"
                                            rel="noopener noreferrer"
                                            className="text-blue-600 hover:text-blue-700 hover:underline ml-1 transition-colors font-medium"
                                        >
                                            钉钉自定义机器人接入文档
                                        </a>
                                    </CardDescription>
                                </div>
                            </div>
                            <div className="flex items-center space-x-3">
                                {formValues.dingtalkEnabled && (
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        disabled={testMutation.isPending}
                                        onClick={() => testMutation.mutate('dingtalk')}
                                        className="text-xs bg-gray-100 hover:bg-gray-200 transition-colors border-none cursor-pointer"
                                    >
                                        <TestTube className="w-3.5 h-3.5 mr-1.5"/>
                                        {testMutation.isPending ? '测试中...' : '发送测试'}
                                    </Button>
                                )}
                                <label className="relative inline-flex items-center cursor-pointer">
                                    <input
                                        type="checkbox"
                                        className="sr-only peer"
                                        checked={formValues.dingtalkEnabled}
                                        onChange={(e) => updateField('dingtalkEnabled', e.target.checked)}
                                    />
                                    <div
                                        className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-blue-300 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-blue-600"></div>
                                </label>
                            </div>
                        </div>
                    </CardHeader>

                    {formValues.dingtalkEnabled && (
                        <CardContent className="space-y-4 animate-in slide-in-from-top-2 duration-200">
                            <div>
                                <label
                                    className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">
                                    访问令牌 (Access Token) <span className="text-red-500">*</span>
                                </label>
                                <Input
                                    value={formValues.dingtalkSecretKey}
                                    onChange={(e) => updateField('dingtalkSecretKey', e.target.value)}
                                    placeholder="在钉钉机器人配置中获取的 access_token"
                                    className="bg-gray-50 border-gray-200 focus:bg-white focus:border-blue-500 focus:ring-1 focus:ring-blue-500 transition-all font-mono text-sm"
                                />
                            </div>
                            <div>
                                <label
                                    className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">
                                    加签密钥（可选）
                                </label>
                                <div className="relative">
                                    <Input
                                        type="password"
                                        value={formValues.dingtalkSignSecret}
                                        onChange={(e) => updateField('dingtalkSignSecret', e.target.value)}
                                        placeholder="SEC 开头的加签密钥"
                                        className="bg-gray-50 border-gray-200 focus:bg-white focus:border-blue-500 focus:ring-1 focus:ring-blue-500 transition-all font-mono text-sm pr-10"
                                    />
                                    <Shield size={14}
                                            className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400"/>
                                </div>
                                <p className="text-xs text-gray-400 mt-1.5">如果启用了加签，请填写 SEC 开头的密钥</p>
                            </div>
                        </CardContent>
                    )}
                </Card>

                {/* 企业微信通知 */}
                <Card
                    className={`border transition-all ${formValues.wecomEnabled ? 'border-green-200 bg-gradient-to-br from-white to-green-50/20' : 'border-gray-200 opacity-95'}`}>
                    <CardHeader className="border-b border-gray-100 bg-white/50">
                        <div className="flex items-center justify-between">
                            <div className="flex items-center space-x-3 flex-1">
                                <div
                                    className={`w-12 h-12 rounded-lg flex items-center justify-center ${formValues.wecomEnabled ? 'bg-green-50 text-green-600' : 'bg-gray-100 text-gray-400'}`}>
                                    <MessageSquare size={24}/>
                                </div>
                                <div className="flex-1">
                                    <div className="flex items-center space-x-2">
                                        <CardTitle className="text-lg font-bold text-gray-800">企业微信通知</CardTitle>
                                        <div
                                            className={`w-2 h-2 rounded-full ${formValues.wecomEnabled ? 'bg-green-500' : 'bg-gray-300'}`}></div>
                                        <span
                                            className="text-xs text-gray-500">{formValues.wecomEnabled ? '已启用' : '未启用'}</span>
                                    </div>
                                    <CardDescription className="mt-1.5 text-xs">
                                        了解更多：
                                        <a
                                            href="https://work.weixin.qq.com/api/doc/90000/90136/91770"
                                            target="_blank"
                                            rel="noopener noreferrer"
                                            className="text-blue-600 hover:text-blue-700 hover:underline ml-1 transition-colors font-medium"
                                        >
                                            企业微信群机器人配置说明
                                        </a>
                                    </CardDescription>
                                </div>
                            </div>
                            <div className="flex items-center space-x-3">
                                {formValues.wecomEnabled && (
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        disabled={testMutation.isPending}
                                        onClick={() => testMutation.mutate('wecom')}
                                        className="text-xs bg-gray-100 hover:bg-gray-200 transition-colors border-none cursor-pointer"
                                    >
                                        <TestTube className="w-3.5 h-3.5 mr-1.5"/>
                                        {testMutation.isPending ? '测试中...' : '发送测试'}
                                    </Button>
                                )}
                                <label className="relative inline-flex items-center cursor-pointer">
                                    <input
                                        type="checkbox"
                                        className="sr-only peer"
                                        checked={formValues.wecomEnabled}
                                        onChange={(e) => updateField('wecomEnabled', e.target.checked)}
                                    />
                                    <div
                                        className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-green-300 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-green-600"></div>
                                </label>
                            </div>
                        </div>
                    </CardHeader>

                    {formValues.wecomEnabled && (
                        <CardContent className="space-y-4 animate-in slide-in-from-top-2 duration-200">
                            <div>
                                <label
                                    className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">
                                    Webhook Key <span className="text-red-500">*</span>
                                </label>
                                <Input
                                    value={formValues.wecomSecretKey}
                                    onChange={(e) => updateField('wecomSecretKey', e.target.value)}
                                    placeholder="企业微信群机器人的 Webhook Key"
                                    className="bg-gray-50 border-gray-200 focus:bg-white focus:border-green-500 focus:ring-1 focus:ring-green-500 transition-all font-mono text-sm"
                                />
                            </div>
                        </CardContent>
                    )}
                </Card>

                {/* 飞书通知 */}
                <Card
                    className={`border transition-all ${formValues.feishuEnabled ? 'border-purple-200 bg-gradient-to-br from-white to-purple-50/20' : 'border-gray-200 opacity-95'}`}>
                    <CardHeader className="border-b border-gray-100 bg-white/50">
                        <div className="flex items-center justify-between">
                            <div className="flex items-center space-x-3 flex-1">
                                <div
                                    className={`w-12 h-12 rounded-lg flex items-center justify-center ${formValues.feishuEnabled ? 'bg-purple-50 text-purple-600' : 'bg-gray-100 text-gray-400'}`}>
                                    <Send size={24} className="rotate-45"/>
                                </div>
                                <div className="flex-1">
                                    <div className="flex items-center space-x-2">
                                        <CardTitle className="text-lg font-bold text-gray-800">飞书通知</CardTitle>
                                        <div
                                            className={`w-2 h-2 rounded-full ${formValues.feishuEnabled ? 'bg-green-500' : 'bg-gray-300'}`}></div>
                                        <span
                                            className="text-xs text-gray-500">{formValues.feishuEnabled ? '已启用' : '未启用'}</span>
                                    </div>
                                    <CardDescription className="mt-1.5 text-xs">
                                        了解更多：
                                        <a
                                            href="https://www.feishu.cn/hc/zh-CN/articles/360024984973"
                                            target="_blank"
                                            rel="noopener noreferrer"
                                            className="text-blue-600 hover:text-blue-700 hover:underline ml-1 transition-colors font-medium"
                                        >
                                            在群组中使用机器人
                                        </a>
                                    </CardDescription>
                                </div>
                            </div>
                            <div className="flex items-center space-x-3">
                                {formValues.feishuEnabled && (
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        disabled={testMutation.isPending}
                                        onClick={() => testMutation.mutate('feishu')}
                                        className="text-xs bg-gray-100 hover:bg-gray-200 transition-colors border-none cursor-pointer"
                                    >
                                        <TestTube className="w-3.5 h-3.5 mr-1.5"/>
                                        {testMutation.isPending ? '测试中...' : '发送测试'}
                                    </Button>
                                )}
                                <label className="relative inline-flex items-center cursor-pointer">
                                    <input
                                        type="checkbox"
                                        className="sr-only peer"
                                        checked={formValues.feishuEnabled}
                                        onChange={(e) => updateField('feishuEnabled', e.target.checked)}
                                    />
                                    <div
                                        className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-purple-300 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-purple-600"></div>
                                </label>
                            </div>
                        </div>
                    </CardHeader>

                    {formValues.feishuEnabled && (
                        <CardContent className="space-y-4 animate-in slide-in-from-top-2 duration-200">
                            <div>
                                <label
                                    className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">
                                    Webhook Token <span className="text-red-500">*</span>
                                </label>
                                <Input
                                    value={formValues.feishuSecretKey}
                                    onChange={(e) => updateField('feishuSecretKey', e.target.value)}
                                    placeholder="飞书群机器人的 Webhook Token"
                                    className="bg-gray-50 border-gray-200 focus:bg-white focus:border-purple-500 focus:ring-1 focus:ring-purple-500 transition-all font-mono text-sm"
                                />
                            </div>
                            <div>
                                <label
                                    className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">
                                    签名密钥（可选）
                                </label>
                                <div className="relative">
                                    <Input
                                        type="password"
                                        value={formValues.feishuSignSecret}
                                        onChange={(e) => updateField('feishuSignSecret', e.target.value)}
                                        placeholder="如果启用了签名验证，请填写密钥"
                                        className="bg-gray-50 border-gray-200 focus:bg-white focus:border-purple-500 focus:ring-1 focus:ring-purple-500 transition-all font-mono text-sm pr-10"
                                    />
                                    <Shield size={14}
                                            className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400"/>
                                </div>
                            </div>
                        </CardContent>
                    )}
                </Card>

                {/* 自定义 Webhook */}
                <Card
                    className={`border transition-all ${formValues.webhookEnabled ? 'border-orange-200 bg-gradient-to-br from-white to-orange-50/20' : 'border-gray-200 opacity-95'}`}>
                    <CardHeader className="border-b border-gray-100 bg-white/50">
                        <div className="flex items-center justify-between">
                            <div className="flex items-center space-x-3 flex-1">
                                <div
                                    className={`w-12 h-12 rounded-lg flex items-center justify-center ${formValues.webhookEnabled ? 'bg-orange-50 text-orange-600' : 'bg-gray-100 text-gray-400'}`}>
                                    <Link size={24}/>
                                </div>
                                <div className="flex-1">
                                    <div className="flex items-center space-x-2">
                                        <CardTitle className="text-lg font-bold text-gray-800">自定义
                                            Webhook</CardTitle>
                                        <div
                                            className={`w-2 h-2 rounded-full ${formValues.webhookEnabled ? 'bg-green-500' : 'bg-gray-300'}`}></div>
                                        <span
                                            className="text-xs text-gray-500">{formValues.webhookEnabled ? '已启用' : '未启用'}</span>
                                    </div>
                                    <CardDescription className="mt-1.5 text-xs">
                                        配置自定义 HTTP 回调接口接收短信通知
                                    </CardDescription>
                                </div>
                            </div>
                            <div className="flex items-center space-x-3">
                                {formValues.webhookEnabled && (
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        disabled={testMutation.isPending}
                                        onClick={() => testMutation.mutate('webhook')}
                                        className="text-xs bg-gray-100 hover:bg-gray-200 transition-colors border-none cursor-pointer"
                                    >
                                        <TestTube className="w-3.5 h-3.5 mr-1.5"/>
                                        {testMutation.isPending ? '测试中...' : '发送测试'}
                                    </Button>
                                )}
                                <label className="relative inline-flex items-center cursor-pointer">
                                    <input
                                        type="checkbox"
                                        className="sr-only peer"
                                        checked={formValues.webhookEnabled}
                                        onChange={(e) => updateField('webhookEnabled', e.target.checked)}
                                    />
                                    <div
                                        className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-orange-300 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-orange-600"></div>
                                </label>
                            </div>
                        </div>
                    </CardHeader>

                    {formValues.webhookEnabled && (
                        <CardContent className="space-y-4 animate-in slide-in-from-top-2 duration-200">
                            <div>
                                <label
                                    className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">
                                    Webhook URL <span className="text-red-500">*</span>
                                </label>
                                <Input
                                    value={formValues.webhookUrl}
                                    onChange={(e) => updateField('webhookUrl', e.target.value)}
                                    placeholder="https://your-server.com/webhook"
                                    className="bg-gray-50 border-gray-200 focus:bg-white focus:border-orange-500 focus:ring-1 focus:ring-orange-500 transition-all font-mono text-sm"
                                />
                            </div>

                            <div>
                                <label
                                    className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">
                                    HTTP 方法
                                </label>
                                <Select
                                    value={formValues.webhookMethod}
                                    onValueChange={(value) => updateField('webhookMethod', value)}
                                >
                                    <SelectTrigger
                                        className="bg-gray-50 border-gray-200 focus:bg-white focus:border-orange-500 focus:ring-1 focus:ring-orange-500 transition-all">
                                        <SelectValue/>
                                    </SelectTrigger>
                                    <SelectContent>
                                        <SelectItem value="GET">GET</SelectItem>
                                        <SelectItem value="POST">POST</SelectItem>
                                        <SelectItem value="PUT">PUT</SelectItem>
                                        <SelectItem value="PATCH">PATCH</SelectItem>
                                        <SelectItem value="DELETE">DELETE</SelectItem>
                                    </SelectContent>
                                </Select>
                            </div>

                            <div>
                                <label
                                    className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">
                                    请求类型 <span className="text-red-500">*</span>
                                </label>
                                <Input
                                    value={formValues.webhookContentType || 'application/json; charset=utf-8'}
                                    onChange={(e) => updateField('webhookContentType', e.target.value)}
                                    placeholder="application/json; charset=utf-8"
                                    className="bg-gray-50 border-gray-200 focus:bg-white focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-all font-mono text-sm"
                                />
                            </div>

                            <div>
                                <label
                                    className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">
                                    请求体模板 <span className="text-red-500">*</span>
                                </label>
                                <Textarea
                                    value={formValues.webhookBody}
                                    onChange={(e) => updateField('webhookBody', e.target.value)}
                                    placeholder='{"from": "{{from}}", "content": "{{content}}", "timestamp": "{{timestamp}}"}'
                                    rows={6}
                                    className="bg-gray-50 border-gray-200 focus:bg-white focus:border-orange-500 focus:ring-1 focus:ring-orange-500 transition-all font-mono text-xs"
                                />
                                <p className="text-xs text-gray-400 mt-1.5">
                                    支持模板变量：<code className="bg-gray-200 px-1 py-0.5 rounded">{'{{from}}'}</code>（发送方）、
                                    <code className="bg-gray-200 px-1 py-0.5 rounded">{'{{content}}'}</code>（短信内容）、
                                    <code className="bg-gray-200 px-1 py-0.5 rounded">{'{{timestamp}}'}</code>（时间戳）
                                </p>
                            </div>

                            <div>
                                <label
                                    className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">
                                    自定义请求头 (JSON 格式)
                                </label>
                                <Textarea
                                    value={formValues.webhookHeaders}
                                    onChange={(e) => updateField('webhookHeaders', e.target.value)}
                                    placeholder='{"Authorization": "Bearer token", "Content-Type": "application/json"}'
                                    rows={4}
                                    className="bg-gray-50 border-gray-200 focus:bg-white focus:border-orange-500 focus:ring-1 focus:ring-orange-500 transition-all font-mono text-xs"
                                />
                                <p className="text-xs text-gray-400 mt-1.5">
                                    可选，格式为 JSON 对象，例如: {`{"key": "value"}`}
                                </p>
                            </div>

                            <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
                                <div className="text-xs font-bold text-blue-900 mb-2 flex items-center gap-1.5">
                                    <CheckCircle2 size={14}/>
                                    模板变量说明
                                </div>
                                <div className="text-xs text-blue-800 space-y-2">
                                    <p>请求体支持以下模板变量：</p>
                                    <ul className="list-disc list-inside space-y-1 ml-2">
                                        <li><code className="bg-white px-1.5 py-0.5 rounded border border-blue-200">{'{{from}}'}</code> - 短信发送方手机号</li>
                                        <li><code className="bg-white px-1.5 py-0.5 rounded border border-blue-200">{'{{content}}'}</code> - 短信内容</li>
                                        <li><code className="bg-white px-1.5 py-0.5 rounded border border-blue-200">{'{{timestamp}}'}</code> - 接收时间（格式：2006-01-02 15:04:05）</li>
                                    </ul>
                                    <p className="mt-2">示例模板：</p>
                                    <pre
                                        className="bg-white border border-blue-100 rounded p-3 mt-2 overflow-x-auto text-[11px] leading-relaxed">
{`{
  "from": "{{from}}",
  "content": "{{content}}",
  "timestamp": "{{timestamp}}"
}`}
                    </pre>
                                </div>
                            </div>
                        </CardContent>
                    )}
                </Card>

                {/* 邮件通知 */}
                <Card
                    className={`border transition-all ${formValues.emailEnabled ? 'border-indigo-200 bg-gradient-to-br from-white to-indigo-50/20' : 'border-gray-200 opacity-95'}`}>
                    <CardHeader className="border-b border-gray-100 bg-white/50">
                        <div className="flex items-center justify-between">
                            <div className="flex items-center space-x-3 flex-1">
                                <div
                                    className={`w-12 h-12 rounded-lg flex items-center justify-center ${formValues.emailEnabled ? 'bg-indigo-50 text-indigo-600' : 'bg-gray-100 text-gray-400'}`}>
                                    <Mail size={24}/>
                                </div>
                                <div className="flex-1">
                                    <div className="flex items-center space-x-2">
                                        <CardTitle className="text-lg font-bold text-gray-800">邮件通知</CardTitle>
                                        <div
                                            className={`w-2 h-2 rounded-full ${formValues.emailEnabled ? 'bg-green-500' : 'bg-gray-300'}`}></div>
                                        <span
                                            className="text-xs text-gray-500">{formValues.emailEnabled ? '已启用' : '未启用'}</span>
                                    </div>
                                    <CardDescription className="mt-1.5 text-xs">
                                        通过 SMTP 协议发送邮件通知
                                    </CardDescription>
                                </div>
                            </div>
                            <div className="flex items-center space-x-3">
                                {formValues.emailEnabled && (
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        disabled={testMutation.isPending}
                                        onClick={() => testMutation.mutate('email')}
                                        className="text-xs bg-gray-100 hover:bg-gray-200 transition-colors border-none cursor-pointer"
                                    >
                                        <TestTube className="w-3.5 h-3.5 mr-1.5"/>
                                        {testMutation.isPending ? '测试中...' : '发送测试'}
                                    </Button>
                                )}
                                <label className="relative inline-flex items-center cursor-pointer">
                                    <input
                                        type="checkbox"
                                        className="sr-only peer"
                                        checked={formValues.emailEnabled}
                                        onChange={(e) => updateField('emailEnabled', e.target.checked)}
                                    />
                                    <div
                                        className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-indigo-300 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-indigo-600"></div>
                                </label>
                            </div>
                        </div>
                    </CardHeader>

                    {formValues.emailEnabled && (
                        <CardContent className="space-y-4 animate-in slide-in-from-top-2 duration-200">
                            <div className="grid grid-cols-2 gap-4">
                                <div>
                                    <label
                                        className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">
                                        SMTP 服务器 <span className="text-red-500">*</span>
                                    </label>
                                    <Input
                                        value={formValues.emailSmtpHost}
                                        onChange={(e) => updateField('emailSmtpHost', e.target.value)}
                                        placeholder="smtp.example.com"
                                        className="bg-gray-50 border-gray-200 focus:bg-white focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-all font-mono text-sm"
                                    />
                                </div>
                                <div>
                                    <label
                                        className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">
                                        SMTP 端口 <span className="text-red-500">*</span>
                                    </label>
                                    <Input
                                        value={formValues.emailSmtpPort}
                                        onChange={(e) => updateField('emailSmtpPort', e.target.value)}
                                        placeholder="587"
                                        className="bg-gray-50 border-gray-200 focus:bg-white focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-all font-mono text-sm"
                                    />
                                </div>
                            </div>

                            <div>
                                <label
                                    className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">
                                    用户名 <span className="text-red-500">*</span>
                                </label>
                                <Input
                                    value={formValues.emailUsername}
                                    onChange={(e) => updateField('emailUsername', e.target.value)}
                                    placeholder="your-email@example.com"
                                    className="bg-gray-50 border-gray-200 focus:bg-white focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-all font-mono text-sm"
                                />
                            </div>

                            <div>
                                <label
                                    className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">
                                    密码/授权码 <span className="text-red-500">*</span>
                                </label>
                                <div className="relative">
                                    <Input
                                        type="password"
                                        value={formValues.emailPassword}
                                        onChange={(e) => updateField('emailPassword', e.target.value)}
                                        placeholder="SMTP 密码或授权码"
                                        className="bg-gray-50 border-gray-200 focus:bg-white focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-all font-mono text-sm pr-10"
                                    />
                                    <Shield size={14}
                                            className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400"/>
                                </div>
                                <p className="text-xs text-gray-400 mt-1.5">部分邮箱服务商（如 QQ、163 等）需要使用授权码而非登录密码</p>
                            </div>

                            <div>
                                <label
                                    className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">
                                    发件人地址 <span className="text-red-500">*</span>
                                </label>
                                <Input
                                    value={formValues.emailFrom}
                                    onChange={(e) => updateField('emailFrom', e.target.value)}
                                    placeholder="sender@example.com"
                                    className="bg-gray-50 border-gray-200 focus:bg-white focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-all font-mono text-sm"
                                />
                            </div>

                            <div>
                                <label
                                    className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">
                                    收件人地址 <span className="text-red-500">*</span>
                                </label>
                                <Input
                                    value={formValues.emailTo}
                                    onChange={(e) => updateField('emailTo', e.target.value)}
                                    placeholder="receiver@example.com（多个收件人用逗号分隔）"
                                    className="bg-gray-50 border-gray-200 focus:bg-white focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-all font-mono text-sm"
                                />
                            </div>

                            <div>
                                <label
                                    className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">
                                    邮件主题模板 <span className="text-red-500">*</span>
                                </label>
                                <Input
                                    value={formValues.emailSubject}
                                    onChange={(e) => updateField('emailSubject', e.target.value)}
                                    placeholder="收到新短信 - {{from}}"
                                    className="bg-gray-50 border-gray-200 focus:bg-white focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-all font-mono text-sm"
                                />
                                <p className="text-xs text-gray-400 mt-1.5">
                                    支持模板变量：<code className="bg-gray-200 px-1 py-0.5 rounded">{'{{from}}'}</code>（发送方）、
                                    <code className="bg-gray-200 px-1 py-0.5 rounded">{'{{content}}'}</code>（短信内容）、
                                    <code className="bg-gray-200 px-1 py-0.5 rounded">{'{{timestamp}}'}</code>（时间戳）
                                </p>
                            </div>

                            <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
                                <div className="text-xs font-bold text-blue-900 mb-2 flex items-center gap-1.5">
                                    <CheckCircle2 size={14}/>
                                    常用邮箱 SMTP 配置
                                </div>
                                <div className="text-xs text-blue-800 space-y-2">
                                    <ul className="list-disc list-inside space-y-1 ml-2">
                                        <li>QQ 邮箱：smtp.qq.com，端口 587 或 465（SSL）</li>
                                        <li>163 邮箱：smtp.163.com，端口 465（SSL）</li>
                                        <li>Gmail：smtp.gmail.com，端口 587</li>
                                        <li>Outlook：smtp-mail.outlook.com，端口 587</li>
                                    </ul>
                                    <p className="mt-2 text-amber-700 bg-amber-50 border border-amber-200 rounded px-2 py-1">
                                        提示：QQ 邮箱和 163 邮箱等需要在邮箱设置中开启 SMTP 服务并使用授权码
                                    </p>
                                </div>
                            </div>
                        </CardContent>
                    )}
                </Card>
                {/* telegram通知 */}
                <Card
                    className={`border transition-all ${formValues.telegramlEnabled ? 'border-blue-200 bg-gradient-to-br from-white to-blue-50/20' : 'border-gray-200 opacity-95'}`}>
                    <CardHeader className="border-b border-gray-100 bg-white/50">
                        <div className="flex items-center justify-between">
                            <div className="flex items-center space-x-3 flex-1">
                                <div
                                    className={`w-12 h-12 rounded-lg flex items-center justify-center ${formValues.telegramlEnabled ? 'bg-blue-50 text-blue-600' : 'bg-gray-100 text-gray-400'}`}>
                                    <Bell size={24}/>
                                </div>
                                <div className="flex-1">
                                    <div className="flex items-center space-x-2">
                                        <CardTitle className="text-lg font-bold text-gray-800">telegram通知</CardTitle>
                                        <div
                                            className={`w-2 h-2 rounded-full ${formValues.telegramlEnabled ? 'bg-green-500' : 'bg-gray-300'}`}></div>
                                        <span
                                            className="text-xs text-gray-500">{formValues.telegramlEnabled ? '已启用' : '未启用'}</span>
                                        <div
                                            className={`w-2 h-2 rounded-full ${formValues.telegramProxyEnabled ? 'bg-green-500' : 'bg-gray-300'}`}></div>
                                        <span
                                            className="text-xs text-gray-500">{formValues.telegramProxyEnabled ? '代理开' : '代理关'}</span>
                                    </div>
                                    <CardDescription className="mt-1.5 text-xs">
                                        了解更多：
                                        <a
                                            href="https://core.telegram.org/bots/api"
                                            target="_blank"
                                            rel="noopener noreferrer"
                                            className="text-blue-600 hover:text-blue-700 hover:underline ml-1 transition-colors font-medium"
                                        >
                                            telegram自定义机器人接入文档
                                        </a>
                                    </CardDescription>
                                </div>
                            </div>
                            <div className="flex items-center space-x-3">
                                {formValues.telegramlEnabled && (
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        disabled={testMutation.isPending}
                                        onClick={() => testMutation.mutate('telegram')}
                                        className="text-xs bg-gray-100 hover:bg-gray-200 transition-colors border-none cursor-pointer"
                                    >
                                        <TestTube className="w-3.5 h-3.5 mr-1.5"/>
                                        {testMutation.isPending ? '测试中...' : '发送测试'}
                                    </Button>
                                )}
                                {formValues.telegramlEnabled && (
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        onClick={() =>
                                            updateField('telegramProxyEnabled', !formValues.telegramProxyEnabled)
                                        }
                                        className={`text-xs border-none transition-colors ${
                                            formValues.telegramProxyEnabled
                                                ? 'bg-blue-100 text-blue-700'
                                                : 'bg-gray-100 text-gray-600'
                                        }`}
                                    >
                                        🌐 HTTP代理
                                    </Button>
                                )}
                                <label className="relative inline-flex items-center cursor-pointer">
                                    <input
                                        type="checkbox"
                                        className="sr-only peer"
                                        checked={formValues.telegramlEnabled}
                                        onChange={(e) => updateField('telegramlEnabled', e.target.checked)}
                                    />
                                    <div
                                        className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-blue-300 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-blue-600"></div>
                                </label>
                            </div>
                        </div>
                    </CardHeader>
                    {formValues.telegramProxyEnabled && (
                        <div className="space-y-3 rounded-lg border border-blue-100 bg-blue-50/40 p-3 animate-in fade-in duration-200">
                            <div>
                                <label className="block text-xs font-semibold text-gray-600 mb-1 uppercase tracking-wide">
                                    HTTP代理地址
                                </label>
                                <Input
                                    value={formValues.telegramProxyUrl}
                                    onChange={(e) => updateField('telegramProxyUrl', e.target.value)}
                                    placeholder="http://127.0.0.1:7890"
                                    className="font-mono text-sm"
                                />
                            </div>

                            <div className="grid grid-cols-2 gap-3">
                                <div>
                                    <label className="block text-xs text-gray-500 mb-1">
                                        用户名（可选）
                                    </label>
                                    <Input
                                        value={formValues.telegramProxyUsername}
                                        onChange={(e) =>
                                            updateField('telegramProxyUsername', e.target.value)
                                        }
                                        className="font-mono text-sm"
                                    />
                                </div>

                                <div>
                                    <label className="block text-xs text-gray-500 mb-1">
                                        密码（可选）
                                    </label>
                                    <Input
                                        type="password"
                                        value={formValues.telegramProxyPassword}
                                        onChange={(e) =>
                                            updateField('telegramProxyPassword', e.target.value)
                                        }
                                        className="font-mono text-sm"
                                    />
                                </div>
                            </div>
                        </div>
                    )}
                    {formValues.telegramlEnabled && (
                        <CardContent className="space-y-4 animate-in slide-in-from-top-2 duration-200">
                            <div>
                                <label
                                    className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">
                                    apiToken <span className="text-red-500">*</span>
                                </label>
                                <Input
                                    value={formValues.telegramApiToken}
                                    onChange={(e) => updateField('telegramApiToken', e.target.value)}
                                    placeholder="apioken"
                                    className="bg-gray-50 border-gray-200 focus:bg-white focus:border-blue-500 focus:ring-1 focus:ring-blue-500 transition-all font-mono text-sm"
                                />
                            </div>
                            <p className="text-xs text-gray-400 mt-1.5">使用@botfather机器人获取</p>
                            <div>
                                <label
                                    className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">
                                    用户id
                                </label>
                                <div className="relative">
                                    <Input
                                        value={formValues.telegramUserid}
                                        onChange={(e) => updateField('telegramUserid', e.target.value)}
                                        placeholder="userid"
                                        className="bg-gray-50 border-gray-200 focus:bg-white focus:border-blue-500 focus:ring-1 focus:ring-blue-500 transition-all font-mono text-sm pr-10"
                                    />
                                    <Shield size={14}
                                            className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400"/>
                                </div>
                                <p className="text-xs text-gray-400 mt-1.5">使用@userinfobot机器人获取</p>
                            </div>
                        </CardContent>
                    )}
                </Card>

                {/* Bark 通知 */}
                <Card
                    className={`border transition-all ${formValues.barkEnabled ? 'border-purple-200 bg-gradient-to-br from-white to-purple-50/20' : 'border-gray-200 opacity-95'}`}>
                    <CardHeader className="border-b border-gray-100 bg-white/50">
                        <div className="flex items-center justify-between">
                            <div className="flex items-center space-x-3 flex-1">
                                <div
                                    className={`w-12 h-12 rounded-lg flex items-center justify-center ${formValues.barkEnabled ? 'bg-purple-50 text-purple-600' : 'bg-gray-100 text-gray-400'}`}>
                                    <Bell size={24}/>
                                </div>
                                <div className="flex-1">
                                    <div className="flex items-center space-x-2">
                                        <CardTitle className="text-lg font-bold text-gray-800">Bark 通知</CardTitle>
                                        <div
                                            className={`w-2 h-2 rounded-full ${formValues.barkEnabled ? 'bg-green-500' : 'bg-gray-300'}`}></div>
                                        <span
                                            className="text-xs text-gray-500">{formValues.barkEnabled ? '已启用' : '未启用'}</span>
                                    </div>
                                    <CardDescription className="mt-1.5 text-xs">
                                        了解更多：
                                        <a
                                            href="https://github.com/Finb/Bark"
                                            target="_blank"
                                            rel="noopener noreferrer"
                                            className="text-blue-600 hover:text-blue-700 hover:underline ml-1 transition-colors font-medium"
                                        >
                                            Bark 项目主页
                                        </a>
                                    </CardDescription>
                                </div>
                            </div>
                            <div className="flex items-center space-x-3">
                                {formValues.barkEnabled && (
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        disabled={testMutation.isPending}
                                        onClick={() => testMutation.mutate('bark')}
                                        className="text-xs bg-gray-100 hover:bg-gray-200 transition-colors border-none cursor-pointer"
                                    >
                                        <TestTube className="w-3.5 h-3.5 mr-1.5"/>
                                        {testMutation.isPending ? '测试中...' : '发送测试'}
                                    </Button>
                                )}
                                <label className="relative inline-flex items-center cursor-pointer">
                                    <input
                                        type="checkbox"
                                        className="sr-only peer"
                                        checked={formValues.barkEnabled}
                                        onChange={(e) => updateField('barkEnabled', e.target.checked)}
                                    />
                                    <div
                                        className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-purple-300 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-purple-600"></div>
                                </label>
                            </div>
                        </div>
                    </CardHeader>

                    {formValues.barkEnabled && (
                        <CardContent className="space-y-4 animate-in slide-in-from-top-2 duration-200">
                            <div>
                                <label
                                    className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">
                                    Bark 服务器地址 <span className="text-red-500">*</span>
                                </label>
                                <Input
                                    value={formValues.barkServerUrl}
                                    onChange={(e) => updateField('barkServerUrl', e.target.value)}
                                    placeholder="https://api.day.app"
                                    className="bg-gray-50 border-gray-200 focus:bg-white focus:border-purple-500 focus:ring-1 focus:ring-purple-500 transition-all font-mono text-sm"
                                />
                                <p className="text-xs text-gray-400 mt-1.5">默认使用官方服务器，也可自建服务器</p>
                            </div>
                            <div>
                                <label
                                    className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">
                                    设备密钥 <span className="text-red-500">*</span>
                                </label>
                                <Input
                                    value={formValues.barkDeviceKey}
                                    onChange={(e) => updateField('barkDeviceKey', e.target.value)}
                                    placeholder="填写你的设备密钥"
                                    className="bg-gray-50 border-gray-200 focus:bg-white focus:border-purple-500 focus:ring-1 focus:ring-purple-500 transition-all font-mono text-sm"
                                />
                                <p className="text-xs text-gray-400 mt-1.5">在 Bark App 中获取设备密钥</p>
                            </div>
                        </CardContent>
                    )}
                </Card>

                {/* Gotify 通知 */}
                <Card
                    className={`border transition-all ${formValues.gotifyEnabled ? 'border-indigo-200 bg-gradient-to-br from-white to-indigo-50/20' : 'border-gray-200 opacity-95'}`}>
                    <CardHeader className="border-b border-gray-100 bg-white/50">
                        <div className="flex items-center justify-between">
                            <div className="flex items-center space-x-3 flex-1">
                                <div
                                    className={`w-12 h-12 rounded-lg flex items-center justify-center ${formValues.gotifyEnabled ? 'bg-indigo-50 text-indigo-600' : 'bg-gray-100 text-gray-400'}`}>
                                    <Bell size={24}/>
                                </div>
                                <div className="flex-1">
                                    <div className="flex items-center space-x-2">
                                        <CardTitle className="text-lg font-bold text-gray-800">Gotify 通知</CardTitle>
                                        <div
                                            className={`w-2 h-2 rounded-full ${formValues.gotifyEnabled ? 'bg-green-500' : 'bg-gray-300'}`}></div>
                                        <span
                                            className="text-xs text-gray-500">{formValues.gotifyEnabled ? '已启用' : '未启用'}</span>
                                    </div>
                                    <CardDescription className="mt-1.5 text-xs">
                                        了解更多：
                                        <a
                                            href="https://gotify.net/"
                                            target="_blank"
                                            rel="noopener noreferrer"
                                            className="text-blue-600 hover:text-blue-700 hover:underline ml-1 transition-colors font-medium"
                                        >
                                            Gotify 项目主页
                                        </a>
                                    </CardDescription>
                                </div>
                            </div>
                            <div className="flex items-center space-x-3">
                                {formValues.gotifyEnabled && (
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        disabled={testMutation.isPending}
                                        onClick={() => testMutation.mutate('gotify')}
                                        className="text-xs bg-gray-100 hover:bg-gray-200 transition-colors border-none cursor-pointer"
                                    >
                                        <TestTube className="w-3.5 h-3.5 mr-1.5"/>
                                        {testMutation.isPending ? '测试中...' : '发送测试'}
                                    </Button>
                                )}
                                <label className="relative inline-flex items-center cursor-pointer">
                                    <input
                                        type="checkbox"
                                        className="sr-only peer"
                                        checked={formValues.gotifyEnabled}
                                        onChange={(e) => updateField('gotifyEnabled', e.target.checked)}
                                    />
                                    <div
                                        className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-indigo-300 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-indigo-600"></div>
                                </label>
                            </div>
                        </div>
                    </CardHeader>

                    {formValues.gotifyEnabled && (
                        <CardContent className="space-y-4 animate-in slide-in-from-top-2 duration-200">
                            <div>
                                <label
                                    className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">
                                    Gotify 服务器地址 <span className="text-red-500">*</span>
                                </label>
                                <Input
                                    value={formValues.gotifyServerUrl}
                                    onChange={(e) => updateField('gotifyServerUrl', e.target.value)}
                                    placeholder="https://your-gotify-server.com"
                                    className="bg-gray-50 border-gray-200 focus:bg-white focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-all font-mono text-sm"
                                />
                                <p className="text-xs text-gray-400 mt-1.5">填写你的 Gotify 服务器地址</p>
                            </div>
                            <div>
                                <label
                                    className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">
                                    应用令牌 <span className="text-red-500">*</span>
                                </label>
                                <Input
                                    value={formValues.gotifyToken}
                                    onChange={(e) => updateField('gotifyToken', e.target.value)}
                                    placeholder="填写你的应用令牌"
                                    className="bg-gray-50 border-gray-200 focus:bg-white focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500 transition-all font-mono text-sm"
                                />
                                <p className="text-xs text-gray-400 mt-1.5">在 Gotify 后台创建应用后获取令牌</p>
                            </div>
                        </CardContent>
                    )}
                </Card>

                {/* 保存按钮 */}
                <div className="flex pt-6 border-t border-gray-200">
                    <Button
                        onClick={handleSave}
                        disabled={saveMutation.isPending}
                        className="bg-gradient-to-r from-blue-600 to-blue-700 hover:from-blue-700 hover:to-blue-800 transition-all px-8 py-2.5 text-sm font-medium min-w-[140px]"
                    >
                        {saveMutation.isPending ? (
                            <>
                                <Loader2 className="w-4 h-4 mr-2 animate-spin"/>
                                保存中...
                            </>
                        ) : (
                            <>
                                <Save className="w-4 h-4 mr-2"/>
                                保存配置
                            </>
                        )}
                    </Button>
                </div>
            </div>
        </div>
    );
}
