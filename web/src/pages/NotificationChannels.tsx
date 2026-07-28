import {useMemo, useState, type ComponentProps, type ReactNode} from 'react';
import {
    Bell,
    Bot,
    Building2,
    ExternalLink,
    Link2,
    Loader2,
    Mail,
    MessageSquare,
    Save,
    TestTube,
    type LucideIcon,
} from 'lucide-react';
import {useMutation, useQuery, useQueryClient} from '@tanstack/react-query';
import {toast} from 'sonner';
import {Button} from '@/components/ui/button';
import {Input} from '@/components/ui/input';
import {Card, CardContent, CardHeader, CardTitle} from '@/components/ui/card';
import {Select, SelectContent, SelectItem, SelectTrigger, SelectValue} from '@/components/ui/select';
import {Switch} from '@/components/ui/switch';
import {Textarea} from '@/components/ui/textarea';
import {
    getNotificationChannels,
    type NotificationChannel,
    saveNotificationChannels,
    testNotificationChannel,
} from '@/api/property.ts';
import {PageHeader} from '@/components/PageHeader';
import {cn} from '@/lib/utils';

type ChannelType = NotificationChannel['type'];
type EnabledField =
    | 'dingtalkEnabled'
    | 'wecomEnabled'
    | 'feishuEnabled'
    | 'webhookEnabled'
    | 'emailEnabled'
    | 'telegramEnabled';

interface FormValues {
    dingtalkEnabled: boolean;
    dingtalkSecretKey: string;
    dingtalkSignSecret: string;
    wecomEnabled: boolean;
    wecomSecretKey: string;
    feishuEnabled: boolean;
    feishuSecretKey: string;
    feishuSignSecret: string;
    webhookEnabled: boolean;
    webhookUrl: string;
    webhookMethod: string;
    webhookContentType: string;
    webhookHeaders: string;
    webhookBody: string;
    emailEnabled: boolean;
    emailSmtpHost: string;
    emailSmtpPort: string;
    emailUsername: string;
    emailPassword: string;
    emailFrom: string;
    emailTo: string;
    emailSubject: string;
    telegramEnabled: boolean;
    telegramApiToken: string;
    telegramUserid: string;
    telegramProxyEnabled: boolean;
    telegramProxyUrl: string;
    telegramProxyUsername: string;
    telegramProxyPassword: string;
}

interface ChannelDefinition {
    type: ChannelType;
    name: string;
    description: string;
    enabledField: EnabledField;
    icon: LucideIcon;
    docsUrl?: string;
}

const DEFAULT_FORM_VALUES: FormValues = {
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
    telegramEnabled: false,
    telegramApiToken: '',
    telegramUserid: '',
    telegramProxyEnabled: false,
    telegramProxyUrl: '',
    telegramProxyUsername: '',
    telegramProxyPassword: '',
};

const CHANNELS: ChannelDefinition[] = [
    {
        type: 'dingtalk',
        name: '钉钉',
        description: '自定义机器人通知',
        enabledField: 'dingtalkEnabled',
        icon: Bell,
        docsUrl: 'https://open.dingtalk.com/document/robots/custom-robot-access',
    },
    {
        type: 'wecom',
        name: '企业微信',
        description: '群机器人通知',
        enabledField: 'wecomEnabled',
        icon: Building2,
        docsUrl: 'https://work.weixin.qq.com/api/doc/90000/90136/91770',
    },
    {
        type: 'feishu',
        name: '飞书',
        description: '自定义机器人通知',
        enabledField: 'feishuEnabled',
        icon: MessageSquare,
        docsUrl: 'https://www.feishu.cn/hc/zh-CN/articles/360024984973',
    },
    {
        type: 'webhook',
        name: 'Webhook',
        description: '自定义 HTTP 请求',
        enabledField: 'webhookEnabled',
        icon: Link2,
    },
    {
        type: 'email',
        name: '邮件',
        description: 'SMTP 邮件通知',
        enabledField: 'emailEnabled',
        icon: Mail,
    },
    {
        type: 'telegram',
        name: 'Telegram',
        description: 'Bot 消息通知',
        enabledField: 'telegramEnabled',
        icon: Bot,
        docsUrl: 'https://core.telegram.org/bots/api',
    },
];

const getStringConfig = (channel: NotificationChannel | undefined, key: string, fallback = '') => {
    const value = channel?.config?.[key];
    return typeof value === 'string' || typeof value === 'number' ? String(value) : fallback;
};

const getBooleanConfig = (channel: NotificationChannel | undefined, key: string) =>
    channel?.config?.[key] === true;

function channelsToFormValues(channels: NotificationChannel[]): FormValues {
    const values = {...DEFAULT_FORM_VALUES};
    const find = (type: ChannelType) => channels.find((channel) => channel.type === type);
    const dingtalk = find('dingtalk');
    const wecom = find('wecom');
    const feishu = find('feishu');
    const webhook = find('webhook');
    const email = find('email');
    const telegram = find('telegram');

    values.dingtalkEnabled = dingtalk?.enabled ?? false;
    values.dingtalkSecretKey = getStringConfig(dingtalk, 'secretKey');
    values.dingtalkSignSecret = getStringConfig(dingtalk, 'signSecret');
    values.wecomEnabled = wecom?.enabled ?? false;
    values.wecomSecretKey = getStringConfig(wecom, 'secretKey');
    values.feishuEnabled = feishu?.enabled ?? false;
    values.feishuSecretKey = getStringConfig(feishu, 'secretKey');
    values.feishuSignSecret = getStringConfig(feishu, 'signSecret');
    values.webhookEnabled = webhook?.enabled ?? false;
    values.webhookUrl = getStringConfig(webhook, 'url');
    values.webhookMethod = getStringConfig(webhook, 'method', 'POST');
    values.webhookContentType = getStringConfig(webhook, 'contentType', 'application/json; charset=utf-8');
    values.webhookBody = getStringConfig(webhook, 'body', DEFAULT_FORM_VALUES.webhookBody);
    values.webhookHeaders = webhook?.config?.headers
        ? JSON.stringify(webhook.config.headers, null, 2)
        : '';
    values.emailEnabled = email?.enabled ?? false;
    values.emailSmtpHost = getStringConfig(email, 'smtpHost');
    values.emailSmtpPort = getStringConfig(email, 'smtpPort', '587');
    values.emailUsername = getStringConfig(email, 'username');
    values.emailPassword = getStringConfig(email, 'password');
    values.emailFrom = getStringConfig(email, 'from');
    values.emailTo = getStringConfig(email, 'to');
    values.emailSubject = getStringConfig(email, 'subject', DEFAULT_FORM_VALUES.emailSubject);
    values.telegramEnabled = telegram?.enabled ?? false;
    values.telegramApiToken = getStringConfig(telegram, 'apiToken');
    values.telegramUserid = getStringConfig(telegram, 'userid');
    values.telegramProxyEnabled = getBooleanConfig(telegram, 'proxyEnabled');
    values.telegramProxyUrl = getStringConfig(telegram, 'proxyUrl');
    values.telegramProxyUsername = getStringConfig(telegram, 'proxyUsername');
    values.telegramProxyPassword = getStringConfig(telegram, 'proxyPassword');

    return values;
}

interface FieldProps {
    label: string;
    required?: boolean;
    hint?: string;
    children: ReactNode;
}

function Field({label, required = false, hint, children}: FieldProps) {
    return (
        <div className="space-y-1.5">
            <label className="block text-xs font-semibold text-slate-700">
                {label}{required && <span className="ml-1 text-rose-500">*</span>}
            </label>
            {children}
            {hint && <p className="text-xs leading-5 text-slate-400">{hint}</p>}
        </div>
    );
}

function ChannelInput(props: ComponentProps<typeof Input>) {
    return <Input {...props} autoComplete="off"/>;
}

export default function NotificationChannels() {
    const queryClient = useQueryClient();
    const [selectedType, setSelectedType] = useState<ChannelType>('dingtalk');
    const [draft, setDraft] = useState<FormValues | null>(null);

    const {data: channels = [], isLoading} = useQuery({
        queryKey: ['notificationChannels'],
        queryFn: getNotificationChannels,
    });

    const serverValues = useMemo(() => channelsToFormValues(channels), [channels]);
    const formValues = draft ?? serverValues;
    const selectedChannel = CHANNELS.find((channel) => channel.type === selectedType) ?? CHANNELS[0];
    const selectedEnabled = formValues[selectedChannel.enabledField];

    const updateField = <K extends keyof FormValues>(field: K, value: FormValues[K]) => {
        setDraft((current) => ({...(current ?? serverValues), [field]: value}));
    };

    const saveMutation = useMutation({
        mutationFn: saveNotificationChannels,
        onSuccess: async () => {
            toast.success('通知渠道配置已保存');
            await queryClient.invalidateQueries({queryKey: ['notificationChannels']});
            setDraft(null);
        },
        onError: (error: unknown) => {
            console.error('保存失败:', error);
            toast.error('保存失败');
        },
    });

    const testMutation = useMutation({
        mutationFn: testNotificationChannel,
        onSuccess: () => toast.success('测试通知已发送，请检查对应渠道'),
        onError: (error: unknown) => {
            console.error('测试失败:', error);
            toast.error('测试失败，请检查配置');
        },
    });

    const handleSave = () => {
        let webhookHeaders: Record<string, unknown> | undefined;
        if (formValues.webhookHeaders.trim()) {
            try {
                const parsed: unknown = JSON.parse(formValues.webhookHeaders);
                if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
                    toast.error('Webhook Headers 必须是 JSON 对象');
                    return;
                }
                webhookHeaders = parsed as Record<string, unknown>;
            } catch {
                toast.error('Webhook Headers JSON 格式错误');
                return;
            }
        }

        if (formValues.telegramProxyEnabled && !formValues.telegramProxyUrl.trim()) {
            toast.error('已启用 HTTP 代理，但未填写代理地址');
            setSelectedType('telegram');
            return;
        }

        const nextChannels: NotificationChannel[] = [];
        if (formValues.dingtalkEnabled || formValues.dingtalkSecretKey) {
            nextChannels.push({
                type: 'dingtalk',
                enabled: formValues.dingtalkEnabled,
                config: {secretKey: formValues.dingtalkSecretKey, signSecret: formValues.dingtalkSignSecret},
            });
        }
        if (formValues.wecomEnabled || formValues.wecomSecretKey) {
            nextChannels.push({
                type: 'wecom',
                enabled: formValues.wecomEnabled,
                config: {secretKey: formValues.wecomSecretKey},
            });
        }
        if (formValues.feishuEnabled || formValues.feishuSecretKey) {
            nextChannels.push({
                type: 'feishu',
                enabled: formValues.feishuEnabled,
                config: {secretKey: formValues.feishuSecretKey, signSecret: formValues.feishuSignSecret},
            });
        }
        if (formValues.webhookEnabled || formValues.webhookUrl) {
            nextChannels.push({
                type: 'webhook',
                enabled: formValues.webhookEnabled,
                config: {
                    url: formValues.webhookUrl,
                    method: formValues.webhookMethod,
                    contentType: formValues.webhookContentType,
                    headers: webhookHeaders,
                    body: formValues.webhookBody,
                },
            });
        }
        if (formValues.emailEnabled || formValues.emailSmtpHost) {
            nextChannels.push({
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
        if (formValues.telegramEnabled || formValues.telegramApiToken) {
            nextChannels.push({
                type: 'telegram',
                enabled: formValues.telegramEnabled,
                config: {
                    apiToken: formValues.telegramApiToken,
                    userid: formValues.telegramUserid,
                    proxyEnabled: formValues.telegramProxyEnabled,
                    proxyUrl: formValues.telegramProxyUrl,
                    proxyUsername: formValues.telegramProxyUsername,
                    proxyPassword: formValues.telegramProxyPassword,
                },
            });
        }

        saveMutation.mutate(nextChannels);
    };

    const inputClass = 'bg-slate-50 border-slate-200 focus:bg-white focus:border-blue-500 font-mono text-sm';
    const testingSelected = testMutation.isPending && testMutation.variables === selectedType;

    const renderConfig = () => {
        switch (selectedType) {
            case 'dingtalk':
                return (
                    <div className="grid gap-5">
                        <Field label="访问令牌（Access Token）" required>
                            <ChannelInput value={formValues.dingtalkSecretKey} onChange={(event) => updateField('dingtalkSecretKey', event.target.value)} placeholder="钉钉机器人 access_token" className={inputClass}/>
                        </Field>
                        <Field label="加签密钥" hint="如果机器人启用了加签，请填写 SEC 开头的密钥。">
                            <ChannelInput type="password" value={formValues.dingtalkSignSecret} onChange={(event) => updateField('dingtalkSignSecret', event.target.value)} placeholder="SEC..." className={inputClass}/>
                        </Field>
                    </div>
                );
            case 'wecom':
                return (
                    <Field label="Webhook Key" required hint="填写企业微信群机器人 Webhook 地址中 key 参数的值。">
                        <ChannelInput value={formValues.wecomSecretKey} onChange={(event) => updateField('wecomSecretKey', event.target.value)} placeholder="企业微信群机器人 key" className={inputClass}/>
                    </Field>
                );
            case 'feishu':
                return (
                    <div className="grid gap-5">
                        <Field label="Webhook Token" required>
                            <ChannelInput value={formValues.feishuSecretKey} onChange={(event) => updateField('feishuSecretKey', event.target.value)} placeholder="飞书机器人 Webhook Token" className={inputClass}/>
                        </Field>
                        <Field label="签名密钥" hint="仅在飞书机器人开启签名校验时填写。">
                            <ChannelInput type="password" value={formValues.feishuSignSecret} onChange={(event) => updateField('feishuSignSecret', event.target.value)} placeholder="签名校验密钥" className={inputClass}/>
                        </Field>
                    </div>
                );
            case 'webhook':
                return (
                    <div className="grid gap-5">
                        <Field label="请求地址" required>
                            <ChannelInput type="url" value={formValues.webhookUrl} onChange={(event) => updateField('webhookUrl', event.target.value)} placeholder="https://example.com/webhook" className={inputClass}/>
                        </Field>
                        <div className="grid gap-4 sm:grid-cols-2">
                            <Field label="请求方法">
                                <Select value={formValues.webhookMethod} onValueChange={(value) => updateField('webhookMethod', value)}>
                                    <SelectTrigger className="w-full bg-slate-50"><SelectValue/></SelectTrigger>
                                    <SelectContent>
                                        <SelectItem value="POST">POST</SelectItem>
                                        <SelectItem value="PUT">PUT</SelectItem>
                                    </SelectContent>
                                </Select>
                            </Field>
                            <Field label="Content-Type">
                                <ChannelInput value={formValues.webhookContentType} onChange={(event) => updateField('webhookContentType', event.target.value)} className={inputClass}/>
                            </Field>
                        </div>
                        <Field label="请求头（JSON）" hint="可选，必须是一个合法的 JSON 对象。">
                            <Textarea value={formValues.webhookHeaders} onChange={(event) => updateField('webhookHeaders', event.target.value)} placeholder={'{\n  "Authorization": "Bearer token"\n}'} className="min-h-28 resize-y bg-slate-50 font-mono text-xs"/>
                        </Field>
                        <Field label="请求体模板" required hint="可使用 {{from}}、{{content}} 和 {{timestamp}} 占位符。">
                            <Textarea value={formValues.webhookBody} onChange={(event) => updateField('webhookBody', event.target.value)} className="min-h-36 resize-y bg-slate-50 font-mono text-xs"/>
                        </Field>
                    </div>
                );
            case 'email':
                return (
                    <div className="grid gap-5">
                        <div className="grid gap-4 sm:grid-cols-[1fr_150px]">
                            <Field label="SMTP 服务器" required>
                                <ChannelInput value={formValues.emailSmtpHost} onChange={(event) => updateField('emailSmtpHost', event.target.value)} placeholder="smtp.example.com" className={inputClass}/>
                            </Field>
                            <Field label="端口" required>
                                <ChannelInput type="number" value={formValues.emailSmtpPort} onChange={(event) => updateField('emailSmtpPort', event.target.value)} className={inputClass}/>
                            </Field>
                        </div>
                        <div className="grid gap-4 sm:grid-cols-2">
                            <Field label="登录用户名" required>
                                <ChannelInput value={formValues.emailUsername} onChange={(event) => updateField('emailUsername', event.target.value)} className={inputClass}/>
                            </Field>
                            <Field label="登录密码" required>
                                <ChannelInput type="password" value={formValues.emailPassword} onChange={(event) => updateField('emailPassword', event.target.value)} className={inputClass}/>
                            </Field>
                        </div>
                        <div className="grid gap-4 sm:grid-cols-2">
                            <Field label="发件人" required>
                                <ChannelInput type="email" value={formValues.emailFrom} onChange={(event) => updateField('emailFrom', event.target.value)} placeholder="sender@example.com" className={inputClass}/>
                            </Field>
                            <Field label="收件人" required hint="多个地址请使用英文逗号分隔。">
                                <ChannelInput value={formValues.emailTo} onChange={(event) => updateField('emailTo', event.target.value)} placeholder="receiver@example.com" className={inputClass}/>
                            </Field>
                        </div>
                        <Field label="邮件主题">
                            <ChannelInput value={formValues.emailSubject} onChange={(event) => updateField('emailSubject', event.target.value)} className="bg-slate-50 border-slate-200 focus:bg-white focus:border-blue-500"/>
                        </Field>
                    </div>
                );
            case 'telegram':
                return (
                    <div className="grid gap-5">
                        <Field label="Bot API Token" required>
                            <ChannelInput type="password" value={formValues.telegramApiToken} onChange={(event) => updateField('telegramApiToken', event.target.value)} placeholder="123456:ABC..." className={inputClass}/>
                        </Field>
                        <Field label="接收用户或 Chat ID" required>
                            <ChannelInput value={formValues.telegramUserid} onChange={(event) => updateField('telegramUserid', event.target.value)} placeholder="用户 ID 或群组 Chat ID" className={inputClass}/>
                        </Field>
                        <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
                            <div className="flex items-center justify-between gap-4">
                                <div>
                                    <p className="text-sm font-semibold text-slate-800">使用 HTTP 代理</p>
                                    <p className="mt-1 text-xs text-slate-500">网络无法直接访问 Telegram 时启用。</p>
                                </div>
                                <Switch checked={formValues.telegramProxyEnabled} onCheckedChange={(checked) => updateField('telegramProxyEnabled', checked)} className="data-[state=checked]:bg-blue-600"/>
                            </div>
                            {formValues.telegramProxyEnabled && (
                                <div className="mt-4 grid gap-4 border-t border-slate-200 pt-4">
                                    <Field label="代理地址" required>
                                        <ChannelInput value={formValues.telegramProxyUrl} onChange={(event) => updateField('telegramProxyUrl', event.target.value)} placeholder="http://127.0.0.1:7890" className={inputClass}/>
                                    </Field>
                                    <div className="grid gap-4 sm:grid-cols-2">
                                        <Field label="代理用户名">
                                            <ChannelInput value={formValues.telegramProxyUsername} onChange={(event) => updateField('telegramProxyUsername', event.target.value)} className={inputClass}/>
                                        </Field>
                                        <Field label="代理密码">
                                            <ChannelInput type="password" value={formValues.telegramProxyPassword} onChange={(event) => updateField('telegramProxyPassword', event.target.value)} className={inputClass}/>
                                        </Field>
                                    </div>
                                </div>
                            )}
                        </div>
                    </div>
                );
        }
    };

    if (isLoading) {
        return (
            <div className="flex items-center justify-center py-20 text-sm text-slate-500">
                <Loader2 className="mr-2 size-5 animate-spin text-blue-600"/>
                正在读取通知渠道
            </div>
        );
    }

    const SelectedIcon = selectedChannel.icon;

    return (
        <div className="space-y-6 animate-in fade-in duration-300">
            <PageHeader
                title="通知渠道"
                description="配置短信和设备事件的第三方推送渠道。"
                action={<Button
                    onClick={handleSave}
                    disabled={!draft || saveMutation.isPending}
                    className="bg-blue-600 text-white hover:bg-blue-700"
                >
                    {saveMutation.isPending ? <Loader2 className="size-4 animate-spin"/> : <Save className="size-4"/>}
                    {saveMutation.isPending ? '保存中...' : draft ? '保存配置' : '已保存'}
                </Button>}
            />

            <div className="grid items-start gap-4 xl:grid-cols-[280px_minmax(0,1fr)]">
                <Card className="gap-0 py-0">
                    <CardHeader className="border-b border-slate-100 py-4">
                        <CardTitle className="text-sm">渠道列表</CardTitle>
                        <p className="mt-1 text-xs text-slate-500">选择渠道后在右侧编辑配置</p>
                    </CardHeader>
                    <CardContent className="p-3">
                        <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-1">
                            {CHANNELS.map((channel) => {
                                const Icon = channel.icon;
                                const enabled = formValues[channel.enabledField];
                                const selected = selectedType === channel.type;
                                return (
                                    <div key={channel.type} className={cn(
                                        'flex min-w-0 items-center gap-2 rounded-xl border p-2 transition-colors',
                                        selected ? 'border-blue-200 bg-blue-50' : 'border-transparent hover:bg-slate-50',
                                    )}>
                                        <button type="button" onClick={() => setSelectedType(channel.type)} className="flex min-w-0 flex-1 items-center gap-3 text-left">
                                            <span className={cn(
                                                'flex size-9 shrink-0 items-center justify-center rounded-lg',
                                                selected ? 'bg-white text-blue-700' : 'bg-slate-100 text-slate-500',
                                            )}>
                                                <Icon className="size-4"/>
                                            </span>
                                            <span className="min-w-0 flex-1">
                                                <span className="block truncate text-sm font-semibold text-slate-800">{channel.name}</span>
                                                <span className="mt-0.5 block truncate text-[10px] text-slate-400">{enabled ? '已启用' : '未启用'}</span>
                                            </span>
                                        </button>
                                        <Switch
                                            checked={enabled}
                                            onCheckedChange={(checked) => updateField(channel.enabledField, checked)}
                                            className="data-[state=checked]:bg-blue-600"
                                            aria-label={`${enabled ? '停用' : '启用'}${channel.name}`}
                                        />
                                    </div>
                                );
                            })}
                        </div>
                    </CardContent>
                </Card>

                <Card className="gap-0 overflow-hidden py-0">
                    <CardHeader className="border-b border-slate-100 py-5">
                        <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
                            <div className="flex items-center gap-3">
                                <span className="flex size-10 items-center justify-center rounded-xl bg-blue-50 text-blue-700">
                                    <SelectedIcon className="size-5"/>
                                </span>
                                <div>
                                    <div className="flex items-center gap-2">
                                        <CardTitle className="text-base">{selectedChannel.name}</CardTitle>
                                        <span className={cn(
                                            'rounded-full px-2 py-0.5 text-[10px] font-semibold',
                                            selectedEnabled ? 'bg-emerald-50 text-emerald-700' : 'bg-slate-100 text-slate-500',
                                        )}>
                                            {selectedEnabled ? '已启用' : '未启用'}
                                        </span>
                                    </div>
                                    <p className="mt-1 text-xs text-slate-500">{selectedChannel.description}</p>
                                </div>
                            </div>
                            <div className="flex items-center gap-2">
                                {selectedChannel.docsUrl && (
                                    <Button variant="outline" size="sm" asChild>
                                        <a href={selectedChannel.docsUrl} target="_blank" rel="noopener noreferrer">
                                            <ExternalLink className="size-3.5"/>
                                            接入文档
                                        </a>
                                    </Button>
                                )}
                                <Button
                                    variant="outline"
                                    size="sm"
                                    disabled={!selectedEnabled || Boolean(draft) || testMutation.isPending}
                                    onClick={() => testMutation.mutate(selectedType)}
                                    className="border-blue-200 text-blue-700 hover:bg-blue-50"
                                    title={draft ? '请先保存当前配置' : undefined}
                                >
                                    {testingSelected ? <Loader2 className="size-3.5 animate-spin"/> : <TestTube className="size-3.5"/>}
                                    {testingSelected ? '测试中...' : '发送测试'}
                                </Button>
                            </div>
                        </div>
                    </CardHeader>

                    <CardContent className="py-6">
                        {!selectedEnabled && (
                            <div className="mb-5 flex items-center gap-2 rounded-lg border border-slate-200 bg-slate-50 px-3.5 py-3 text-xs text-slate-500">
                                <span className="size-1.5 rounded-full bg-slate-300"/>
                                当前渠道未启用，启用后才能编辑配置。
                            </div>
                        )}
                        <fieldset
                            disabled={!selectedEnabled}
                            className="max-w-[900px] transition-opacity disabled:cursor-not-allowed disabled:opacity-55"
                        >
                            {renderConfig()}
                        </fieldset>
                        {draft && (
                            <div className="mt-6 flex items-center gap-2 border-t border-slate-100 pt-4 text-xs text-amber-700">
                                <span className="size-1.5 rounded-full bg-amber-500"/>
                                当前配置尚未保存，保存后才能发送测试通知。
                            </div>
                        )}
                    </CardContent>
                </Card>
            </div>
        </div>
    );
}
