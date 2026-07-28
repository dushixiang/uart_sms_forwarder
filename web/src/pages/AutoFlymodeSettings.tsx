import {useState} from 'react';
import {Activity, Clock3, Loader2, Save, ShieldAlert} from 'lucide-react';
import {useMutation, useQuery, useQueryClient} from '@tanstack/react-query';
import {toast} from 'sonner';
import {Card, CardContent, CardDescription, CardHeader, CardTitle} from '@/components/ui/card';
import {Button} from '@/components/ui/button';
import {Input} from '@/components/ui/input';
import {Switch} from '@/components/ui/switch';
import {
    getAutoFlymodeConfig,
    saveAutoFlymodeConfig,
    type AutoFlymodeConfig,
} from '@/api/property';
import {getStatus} from '@/api/serial';
import type {DeviceStatus} from '@/api/types';
import {PageHeader} from '@/components/PageHeader';

const MIN_IDLE_HOURS = 1;
const MAX_IDLE_HOURS = 30 * 24;

export default function AutoFlymodeSettings() {
    const queryClient = useQueryClient();
    const [draft, setDraft] = useState<{enabled: boolean; idleTimeoutHours: string} | null>(null);

    const configQuery = useQuery({
        queryKey: ['autoFlymodeConfig'],
        queryFn: getAutoFlymodeConfig,
    });

    const statusQuery = useQuery<DeviceStatus>({
        queryKey: ['deviceStatus'],
        queryFn: async () => getStatus() as Promise<DeviceStatus>,
        refetchInterval: 10000,
    });

    const saveMutation = useMutation({
        mutationFn: (config: AutoFlymodeConfig) => saveAutoFlymodeConfig(config),
        onSuccess: async () => {
            toast.success('自动飞行模式配置已保存');
            await queryClient.invalidateQueries({queryKey: ['autoFlymodeConfig']});
            setDraft(null);
        },
        onError: (error: unknown) => {
            console.error('保存自动飞行模式配置失败:', error);
            toast.error('保存失败，请检查配置');
        },
    });

    const savedValues = {
        enabled: configQuery.data?.enabled ?? false,
        idleTimeoutHours: String(configQuery.data?.idleTimeoutHours ?? 1),
    };
    const formValues = draft ?? savedValues;
    const isDirty = Boolean(draft) && (
        draft?.enabled !== savedValues.enabled ||
        draft?.idleTimeoutHours !== savedValues.idleTimeoutHours
    );

    const handleSave = () => {
        const hours = Number(formValues.idleTimeoutHours);
        if (!Number.isInteger(hours) || hours < MIN_IDLE_HOURS || hours > MAX_IDLE_HOURS) {
            toast.warning(`空闲时间必须是 ${MIN_IDLE_HOURS} 到 ${MAX_IDLE_HOURS} 之间的整数小时`);
            return;
        }

        saveMutation.mutate({
            enabled: formValues.enabled,
            idleTimeoutHours: hours,
        });
    };

    if (configQuery.isLoading) {
        return (
            <div className="flex items-center justify-center py-20">
                <Loader2 className="h-8 w-8 animate-spin text-blue-600"/>
            </div>
        );
    }

    const deviceStatus = statusQuery.data;

    return (
        <div className="space-y-6 animate-in fade-in duration-300">
            <PageHeader
                title="自动飞行模式"
                description="短信长时间无活动时自动关闭蜂窝网络，下次发送前恢复网络。"
            />

            <div className="grid grid-cols-1 gap-6 xl:grid-cols-3">
                <Card className="xl:col-span-2">
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2 text-base">
                            <Clock3 className="h-5 w-5 text-blue-600"/>
                            空闲策略
                        </CardTitle>
                        <CardDescription>
                            配置收到或发送短信后，持续空闲多长时间自动进入飞行模式。自动或手动切换成功后，
                            系统会通过所有已启用的通知渠道发送状态通知。
                        </CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-6">
                        <div className="grid max-w-[720px] grid-cols-[minmax(0,1fr)_auto] items-center gap-5 rounded-xl border border-slate-200 bg-slate-50 p-4">
                            <div>
                                <div className="font-medium text-gray-900">启用自动飞行模式</div>
                                <div className="mt-1 text-sm text-gray-500">
                                    启用后，从保存配置的时间开始计算第一个空闲周期。
                                </div>
                            </div>
                            <Switch
                                checked={formValues.enabled}
                                onCheckedChange={(checked) => setDraft({...formValues, enabled: checked})}
                                disabled={saveMutation.isPending}
                                aria-label="启用自动飞行模式"
                            />
                        </div>

                        <div className="space-y-2">
                            <label htmlFor="idle-timeout-hours" className="block text-sm font-medium text-gray-800">
                                短信空闲时间
                            </label>
                            <div className="flex max-w-[220px] items-center gap-3">
                                <Input
                                    id="idle-timeout-hours"
                                    type="number"
                                    min={MIN_IDLE_HOURS}
                                    max={MAX_IDLE_HOURS}
                                    step={1}
                                    value={formValues.idleTimeoutHours}
                                    onChange={(event) => setDraft({...formValues, idleTimeoutHours: event.target.value})}
                                    disabled={saveMutation.isPending}
                                />
                                <span className="shrink-0 text-sm font-medium text-gray-600">小时</span>
                            </div>
                            <p className="text-xs text-gray-500">
                                可设置 {MIN_IDLE_HOURS}～{MAX_IDLE_HOURS} 小时；收到短信或发起短信发送后会重新计时。
                            </p>
                        </div>

                        <div className="flex justify-end border-t border-gray-100 pt-5">
                            <Button onClick={handleSave} disabled={!isDirty || saveMutation.isPending}>
                                {saveMutation.isPending ? (
                                    <Loader2 className="mr-2 h-4 w-4 animate-spin"/>
                                ) : (
                                    <Save className="mr-2 h-4 w-4"/>
                                )}
                                {saveMutation.isPending ? '保存中...' : isDirty ? '保存配置' : '已保存'}
                            </Button>
                        </div>
                    </CardContent>
                </Card>

                <div className="space-y-6">
                    <Card>
                        <CardHeader>
                            <CardTitle className="flex items-center gap-2 text-base">
                                <Activity className="h-5 w-5 text-blue-600"/>
                                当前设备状态
                            </CardTitle>
                        </CardHeader>
                        <CardContent className="space-y-3">
                            <div className="flex items-center justify-between text-sm">
                                <span className="text-gray-500">串口连接</span>
                                <span className={statusQuery.isLoading
                                    ? 'font-medium text-slate-400'
                                    : statusQuery.isError
                                        ? 'font-medium text-rose-600'
                                        : deviceStatus?.connected ? 'font-medium text-green-600' : 'font-medium text-red-600'}>
                                    {statusQuery.isLoading ? '读取中' : statusQuery.isError ? '获取失败' : deviceStatus?.connected ? '在线' : '离线'}
                                </span>
                            </div>
                            <div className="flex items-center justify-between text-sm">
                                <span className="text-gray-500">飞行模式</span>
                                <span className={statusQuery.isLoading || statusQuery.isError
                                    ? 'font-medium text-slate-400'
                                    : deviceStatus?.flymode ? 'font-medium text-amber-600' : 'font-medium text-green-600'}>
                                    {statusQuery.isLoading || statusQuery.isError ? '—' : deviceStatus?.flymode ? '已开启' : '已关闭'}
                                </span>
                            </div>
                            {!statusQuery.isLoading && !statusQuery.isError && !deviceStatus?.connected && (
                                <p className="rounded-md bg-gray-50 p-3 text-xs leading-5 text-gray-500">
                                    配置仍可保存，设备重新连接后自动生效。
                                </p>
                            )}
                        </CardContent>
                    </Card>

                    <Card className="border-amber-200 bg-amber-50/60">
                        <CardContent className="flex gap-3 pt-5">
                            <ShieldAlert className="mt-0.5 h-5 w-5 shrink-0 text-amber-600"/>
                            <div className="text-sm leading-6 text-amber-900">
                                <div className="font-medium">使用提示</div>
                                <p className="mt-1 text-amber-800">
                                    飞行模式期间无法实时接收短信。运营商是否保留并在网络恢复后补发短信，取决于运营商策略。
                                </p>
                            </div>
                        </CardContent>
                    </Card>
                </div>
            </div>
        </div>
    );
}
