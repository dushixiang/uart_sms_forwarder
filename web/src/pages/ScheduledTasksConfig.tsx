import {useState} from 'react';
import {Calendar, CheckCircle2, Clock, Edit, Loader2, MessageSquare, Phone, Play, Plus, Trash2, XCircle} from 'lucide-react';
import {useMutation, useQuery, useQueryClient} from '@tanstack/react-query';
import {toast} from 'sonner';
import {Button} from '@/components/ui/button';
import {Input} from '@/components/ui/input';
import {Switch} from '@/components/ui/switch';
import {Textarea} from '@/components/ui/textarea';
import {
    Dialog,
    DialogContent,
    DialogDescription,
    DialogFooter,
    DialogHeader,
    DialogTitle,
} from '@/components/ui/dialog';
import {
    createScheduledTask,
    deleteScheduledTask,
    getScheduledTasks,
    type LastRunStatus,
    type ScheduledTask,
    triggerScheduledTask,
    updateScheduledTask,
} from '../api/scheduled_task';
import {PageHeader} from '@/components/PageHeader';
import {cn} from '@/lib/utils';

interface TaskFormData {
    name: string;
    enabled: boolean;
    intervalDays: number;
    phoneNumber: string;
    content: string;
}

interface ConfirmationState {
    type: 'trigger' | 'delete';
    task: ScheduledTask;
}

const EMPTY_FORM: TaskFormData = {
    name: '',
    enabled: false,
    intervalDays: 90,
    phoneNumber: '',
    content: '',
};

const getErrorMessage = (error: unknown, fallback: string) => {
    if (error && typeof error === 'object' && 'response' in error) {
        const response = (error as {response?: {data?: {error?: unknown}}}).response;
        if (typeof response?.data?.error === 'string') return response.data.error;
    }
    return fallback;
};

const lastRunDisplay = (status?: LastRunStatus) => {
    switch (status) {
        case 'success':
            return {text: '上次成功', icon: CheckCircle2, className: 'text-emerald-600'};
        case 'failed':
            return {text: '上次失败', icon: XCircle, className: 'text-rose-600'};
        case 'unknown':
            return {text: '结果未知', icon: Clock, className: 'text-slate-500'};
        default:
            return {text: '尚未执行', icon: Clock, className: 'text-slate-400'};
    }
};

export default function ScheduledTasksConfig() {
    const queryClient = useQueryClient();
    const [editorOpen, setEditorOpen] = useState(false);
    const [editingTask, setEditingTask] = useState<ScheduledTask | null>(null);
    const [confirmation, setConfirmation] = useState<ConfirmationState | null>(null);
    const [formData, setFormData] = useState<TaskFormData>({...EMPTY_FORM});

    const {data: tasks = [], isLoading} = useQuery({
        queryKey: ['scheduledTasks'],
        queryFn: getScheduledTasks,
    });

    const closeEditor = () => {
        setEditorOpen(false);
        setEditingTask(null);
        setFormData({...EMPTY_FORM});
    };

    const createMutation = useMutation({
        mutationFn: createScheduledTask,
        onSuccess: async () => {
            await queryClient.invalidateQueries({queryKey: ['scheduledTasks']});
            closeEditor();
            toast.success('任务创建成功');
        },
        onError: (error: unknown) => {
            console.error('创建任务失败:', error);
            toast.error(getErrorMessage(error, '创建任务失败'));
        },
    });

    const updateMutation = useMutation({
        mutationFn: ({id, task}: {id: string; task: TaskFormData}) => updateScheduledTask(id, task),
        onSuccess: async () => {
            await queryClient.invalidateQueries({queryKey: ['scheduledTasks']});
            closeEditor();
            toast.success('任务更新成功');
        },
        onError: (error: unknown) => {
            console.error('更新任务失败:', error);
            toast.error(getErrorMessage(error, '更新任务失败'));
        },
    });

    const deleteMutation = useMutation({
        mutationFn: deleteScheduledTask,
        onSuccess: async () => {
            await queryClient.invalidateQueries({queryKey: ['scheduledTasks']});
            setConfirmation(null);
            toast.success('任务删除成功');
        },
        onError: (error: unknown) => {
            console.error('删除任务失败:', error);
            toast.error(getErrorMessage(error, '删除任务失败'));
        },
    });

    const triggerMutation = useMutation({
        mutationFn: triggerScheduledTask,
        onSuccess: async () => {
            await queryClient.invalidateQueries({queryKey: ['scheduledTasks']});
            setConfirmation(null);
            toast.success('任务已触发执行');
        },
        onError: (error: unknown) => {
            console.error('触发任务失败:', error);
            toast.error(getErrorMessage(error, '触发任务失败'));
        },
    });

    const updateFormField = <K extends keyof TaskFormData>(field: K, value: TaskFormData[K]) => {
        setFormData((current) => ({...current, [field]: value}));
    };

    const openAddEditor = () => {
        setEditingTask(null);
        setFormData({...EMPTY_FORM});
        setEditorOpen(true);
    };

    const openEditEditor = (task: ScheduledTask) => {
        setEditingTask(task);
        setFormData({
            name: task.name,
            enabled: task.enabled,
            intervalDays: task.intervalDays,
            phoneNumber: task.phoneNumber,
            content: task.content,
        });
        setEditorOpen(true);
    };

    const handleSubmit = (event: React.FormEvent) => {
        event.preventDefault();
        if (!formData.name.trim()) {
            toast.warning('请输入任务名称');
            return;
        }
        if (!Number.isInteger(formData.intervalDays) || formData.intervalDays <= 0) {
            toast.warning('执行间隔必须是大于 0 的整数天数');
            return;
        }
        if (!formData.phoneNumber.trim()) {
            toast.warning('请输入目标手机号');
            return;
        }
        if (!formData.content.trim()) {
            toast.warning('请输入短信内容');
            return;
        }

        const normalized = {
            ...formData,
            name: formData.name.trim(),
            phoneNumber: formData.phoneNumber.trim(),
            content: formData.content.trim(),
        };
        if (editingTask) {
            updateMutation.mutate({id: editingTask.id, task: normalized});
        } else {
            createMutation.mutate(normalized);
        }
    };

    if (isLoading) {
        return (
            <div className="flex items-center justify-center py-20 text-sm text-slate-500">
                <Loader2 className="mr-2 size-5 animate-spin text-blue-600"/>
                正在读取计划任务
            </div>
        );
    }

    const editorPending = createMutation.isPending || updateMutation.isPending;
    const confirmationPending = deleteMutation.isPending || triggerMutation.isPending;

    return (
        <div className="space-y-6 animate-in fade-in duration-300">
            <PageHeader
                title="计划任务"
                description="配置周期短信任务，并分别查看启用状态和最近执行结果。"
                action={<Button onClick={openAddEditor} className="bg-blue-600 text-white hover:bg-blue-700">
                    <Plus className="size-4"/>
                    新建任务
                </Button>}
            />

            {tasks.length === 0 ? (
                <div className="rounded-2xl border border-slate-200 bg-white py-16 text-center">
                    <span className="mx-auto flex size-14 items-center justify-center rounded-full bg-blue-50 text-blue-600">
                        <Clock className="size-7"/>
                    </span>
                    <p className="mt-4 text-sm font-semibold text-slate-700">暂无计划任务</p>
                    <p className="mt-1 text-xs text-slate-400">创建任务后，系统会按照设定周期自动发送短信。</p>
                    <Button onClick={openAddEditor} variant="outline" size="sm" className="mt-5 border-blue-200 text-blue-700 hover:bg-blue-50">
                        <Plus className="size-4"/>
                        新建任务
                    </Button>
                </div>
            ) : (
                <div className="overflow-hidden rounded-2xl border border-slate-200 bg-white">
                    <div className="hidden grid-cols-[minmax(190px,1.3fr)_84px_132px_148px_120px_176px] gap-3 border-b border-slate-200 bg-slate-50 px-5 py-3 text-[11px] font-semibold text-slate-500 xl:grid">
                        <span>任务</span>
                        <span>周期</span>
                        <span>目标号码</span>
                        <span>上次执行</span>
                        <span>状态</span>
                        <span className="text-right">操作</span>
                    </div>
                    {tasks.map((task) => {
                        const runState = lastRunDisplay(task.lastRunStatus);
                        const RunIcon = runState.icon;
                        const triggering = triggerMutation.isPending && triggerMutation.variables === task.id;
                        const deleting = deleteMutation.isPending && deleteMutation.variables === task.id;
                        return (
                            <div key={task.id} className="grid gap-3 border-b border-slate-100 px-5 py-4 last:border-b-0 sm:grid-cols-2 xl:grid-cols-[minmax(190px,1.3fr)_84px_132px_148px_120px_176px] xl:items-center">
                                <div className="flex min-w-0 items-start gap-3 sm:col-span-2 xl:col-span-1">
                                    <span className={cn('flex size-9 shrink-0 items-center justify-center rounded-lg', task.enabled ? 'bg-blue-50 text-blue-600' : 'bg-slate-100 text-slate-400')}>
                                        <Calendar className="size-[17px]"/>
                                    </span>
                                    <div className="min-w-0">
                                        <p className="truncate text-sm font-bold text-slate-900">{task.name}</p>
                                        <p className="mt-1 line-clamp-1 text-xs text-slate-500">{task.content}</p>
                                    </div>
                                </div>
                                <div className="text-sm font-semibold text-slate-700">
                                    <span className="mr-2 text-xs font-normal text-slate-400 xl:hidden">周期</span>每 {task.intervalDays} 天
                                </div>
                                <div className="font-mono text-xs font-semibold text-slate-700">
                                    <span className="mr-2 font-sans font-normal text-slate-400 xl:hidden">目标</span>{task.phoneNumber}
                                </div>
                                <div className="text-xs text-slate-600">
                                    <span className="mr-2 text-slate-400 xl:hidden">上次执行</span>
                                    {(task.lastRunAt ?? 0) > 0
                                        ? new Date(task.lastRunAt as number).toLocaleString('zh-CN', {month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit'})
                                        : '从未执行'}
                                </div>
                                <div className="space-y-1.5">
                                    <span className={cn('flex items-center gap-1.5 text-xs font-semibold', task.enabled ? 'text-emerald-600' : 'text-slate-400')}>
                                        <span className={cn('size-1.5 rounded-full', task.enabled ? 'bg-emerald-500' : 'bg-slate-300')}/>
                                        {task.enabled ? '运行中' : '已暂停'}
                                    </span>
                                    <span className={cn('flex items-center gap-1 text-[10px] font-medium', triggering ? 'text-blue-600' : runState.className)}>
                                        {triggering ? <Loader2 className="size-3 animate-spin"/> : <RunIcon className="size-3"/>}
                                        {triggering ? '执行中' : runState.text}
                                    </span>
                                </div>
                                <div className="flex items-center gap-2 sm:col-span-2 xl:col-span-1 xl:justify-end">
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        onClick={() => setConfirmation({type: 'trigger', task})}
                                        disabled={triggering}
                                        className="text-xs font-medium hover:border-blue-200 hover:bg-blue-50 hover:text-blue-700"
                                    >
                                        {triggering ? <Loader2 className="size-3.5 animate-spin"/> : <Play className="size-3.5"/>}
                                        触发
                                    </Button>
                                    <Button variant="outline" size="sm" onClick={() => openEditEditor(task)} className="text-xs font-medium hover:bg-slate-50">
                                        <Edit className="size-3.5"/>
                                        编辑
                                    </Button>
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        onClick={() => setConfirmation({type: 'delete', task})}
                                        disabled={deleting}
                                        className="border-rose-200 px-2.5 text-rose-600 hover:bg-rose-50 hover:text-rose-700"
                                        aria-label={`删除任务 ${task.name}`}
                                    >
                                        {deleting ? <Loader2 className="size-3.5 animate-spin"/> : <Trash2 className="size-3.5"/>}
                                    </Button>
                                </div>
                            </div>
                        );
                    })}
                </div>
            )}

            <Dialog open={editorOpen} onOpenChange={(open) => open ? setEditorOpen(true) : closeEditor()}>
                <DialogContent className="sm:max-w-[540px]">
                    <form onSubmit={handleSubmit}>
                        <DialogHeader>
                            <DialogTitle>{editingTask ? '编辑任务' : '新建任务'}</DialogTitle>
                            <DialogDescription>
                                {editingTask ? '修改任务配置和启用状态。' : '创建周期短信任务，系统会按照设定间隔自动执行。'}
                            </DialogDescription>
                        </DialogHeader>

                        <div className="space-y-5 py-5">
                            <div className="space-y-1.5">
                                <label htmlFor="task-name" className="block text-sm font-medium text-slate-800">任务名称</label>
                                <Input id="task-name" value={formData.name} onChange={(event) => updateFormField('name', event.target.value)} placeholder="例如：90 天流量查询" className="bg-slate-50 focus:bg-white" autoFocus/>
                            </div>

                            <div className="flex items-center justify-between gap-4 rounded-xl border border-slate-200 bg-slate-50 p-4">
                                <div>
                                    <p className="text-sm font-medium text-slate-800">启用任务</p>
                                    <p className="mt-1 text-xs text-slate-500">启用后任务会按周期自动执行。</p>
                                </div>
                                <Switch checked={formData.enabled} onCheckedChange={(checked) => updateFormField('enabled', checked)} className="data-[state=checked]:bg-blue-600"/>
                            </div>

                            <div className="grid gap-4 sm:grid-cols-2">
                                <div className="space-y-1.5">
                                    <label htmlFor="task-interval" className="flex items-center gap-1.5 text-sm font-medium text-slate-800">
                                        <Clock className="size-3.5 text-slate-400"/>执行间隔
                                    </label>
                                    <div className="relative">
                                        <Input id="task-interval" type="number" min={1} step={1} value={formData.intervalDays} onChange={(event) => updateFormField('intervalDays', Number.parseInt(event.target.value, 10) || 0)} className="bg-slate-50 pr-10 focus:bg-white"/>
                                        <span className="absolute right-3 top-1/2 -translate-y-1/2 text-xs text-slate-400">天</span>
                                    </div>
                                </div>
                                <div className="space-y-1.5">
                                    <label htmlFor="task-phone" className="flex items-center gap-1.5 text-sm font-medium text-slate-800">
                                        <Phone className="size-3.5 text-slate-400"/>目标号码
                                    </label>
                                    <Input id="task-phone" value={formData.phoneNumber} onChange={(event) => updateFormField('phoneNumber', event.target.value)} placeholder="10086" className="bg-slate-50 font-mono focus:bg-white"/>
                                </div>
                            </div>

                            <div className="space-y-1.5">
                                <div className="flex items-center justify-between">
                                    <label htmlFor="task-content" className="flex items-center gap-1.5 text-sm font-medium text-slate-800">
                                        <MessageSquare className="size-3.5 text-slate-400"/>短信内容
                                    </label>
                                    <span className="text-xs text-slate-400">{formData.content.length} 字</span>
                                </div>
                                <Textarea id="task-content" value={formData.content} onChange={(event) => updateFormField('content', event.target.value)} placeholder="例如：查询流量、CXLL 等" className="min-h-28 resize-none bg-slate-50 focus:bg-white"/>
                                <p className="text-xs leading-5 text-slate-400">请避免使用设备或运营商不支持的特殊字符。</p>
                            </div>
                        </div>

                        <DialogFooter>
                            <Button type="button" variant="outline" onClick={closeEditor} disabled={editorPending}>取消</Button>
                            <Button type="submit" disabled={editorPending} className="bg-blue-600 text-white hover:bg-blue-700">
                                {editorPending && <Loader2 className="size-4 animate-spin"/>}
                                {editorPending ? '提交中...' : editingTask ? '保存修改' : '创建任务'}
                            </Button>
                        </DialogFooter>
                    </form>
                </DialogContent>
            </Dialog>

            <Dialog open={Boolean(confirmation)} onOpenChange={(open) => !open && !confirmationPending && setConfirmation(null)}>
                <DialogContent className="sm:max-w-md">
                    <DialogHeader>
                        <DialogTitle>{confirmation?.type === 'delete' ? '删除计划任务' : '立即执行任务'}</DialogTitle>
                        <DialogDescription>
                            {confirmation?.type === 'delete'
                                ? `确定删除“${confirmation.task.name}”吗？此操作无法撤销。`
                                : `确定立即执行“${confirmation?.task.name}”吗？系统会向 ${confirmation?.task.phoneNumber} 发送短信。`}
                        </DialogDescription>
                    </DialogHeader>
                    <DialogFooter>
                        <Button variant="outline" onClick={() => setConfirmation(null)} disabled={confirmationPending}>取消</Button>
                        <Button
                            onClick={() => {
                                if (!confirmation) return;
                                if (confirmation.type === 'delete') deleteMutation.mutate(confirmation.task.id);
                                else triggerMutation.mutate(confirmation.task.id);
                            }}
                            disabled={confirmationPending}
                            className={confirmation?.type === 'delete'
                                ? 'bg-rose-600 text-white hover:bg-rose-700'
                                : 'bg-blue-600 text-white hover:bg-blue-700'}
                        >
                            {confirmationPending && <Loader2 className="size-4 animate-spin"/>}
                            {confirmationPending
                                ? '处理中...'
                                : confirmation?.type === 'delete' ? '确认删除' : '立即执行'}
                        </Button>
                    </DialogFooter>
                </DialogContent>
            </Dialog>
        </div>
    );
}
