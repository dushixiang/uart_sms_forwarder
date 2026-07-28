import {useState} from 'react';
import {Calendar, Clock, Edit, MessageSquare, Phone, Plus, Play, Trash2, CheckCircle2, XCircle} from 'lucide-react';
import {useMutation, useQuery, useQueryClient} from '@tanstack/react-query';
import {toast} from 'sonner';
import {Button} from '@/components/ui/button';
import {Input} from '@/components/ui/input';
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
    type ScheduledTask,
    type LastRunStatus,
    triggerScheduledTask,
    updateScheduledTask,
} from '../api/scheduled_task';
import {PageHeader} from '@/components/PageHeader';

interface TaskFormData {
    name: string;
    enabled: boolean;
    intervalDays: number;
    phoneNumber: string;
    content: string;
}

export default function ScheduledTasksConfig() {
    const queryClient = useQueryClient();
    const [dialogOpen, setDialogOpen] = useState(false);
    const [editingTask, setEditingTask] = useState<ScheduledTask | null>(null);
    const [formData, setFormData] = useState<TaskFormData>({
        name: '',
        enabled: false,
        intervalDays: 90,
        phoneNumber: '',
        content: '',
    });

    // 获取状态显示信息
    const getStatusDisplay = (status?: LastRunStatus) => {
        switch (status) {
            case 'success':
                return {
                    icon: CheckCircle2,
                    text: '成功',
                    colorClass: 'text-green-600',
                    bgClass: 'bg-green-50',
                };
            case 'failed':
                return {
                    icon: XCircle,
                    text: '失败',
                    colorClass: 'text-red-600',
                    bgClass: 'bg-red-50',
                };
            case 'unknown':
            default:
                return {
                    icon: Clock,
                    text: '发送中',
                    colorClass: 'text-blue-600',
                    bgClass: 'bg-blue-50',
                };
        }
    };

    // 获取定时任务列表
    const {data: tasks = [], isLoading} = useQuery({
        queryKey: ['scheduledTasks'],
        queryFn: getScheduledTasks,
    });

    // 创建任务 mutation
    const createMutation = useMutation({
        mutationFn: createScheduledTask,
        onSuccess: () => {
            queryClient.invalidateQueries({queryKey: ['scheduledTasks']});
            setDialogOpen(false);
            resetForm();
            toast.success('任务创建成功');
        },
        onError: (error: any) => {
            console.error('创建任务失败:', error);
            toast.error(error.response?.data?.error || '创建任务失败');
        },
    });

    // 更新任务 mutation
    const updateMutation = useMutation({
        mutationFn: ({id, task}: { id: string; task: TaskFormData }) =>
            updateScheduledTask(id, task),
        onSuccess: () => {
            queryClient.invalidateQueries({queryKey: ['scheduledTasks']});
            setDialogOpen(false);
            setEditingTask(null);
            resetForm();
            toast.success('任务更新成功');
        },
        onError: (error: any) => {
            console.error('更新任务失败:', error);
            toast.error(error.response?.data?.error || '更新任务失败');
        },
    });

    // 删除任务 mutation
    const deleteMutation = useMutation({
        mutationFn: deleteScheduledTask,
        onSuccess: () => {
            queryClient.invalidateQueries({queryKey: ['scheduledTasks']});
            toast.success('任务删除成功');
        },
        onError: (error: any) => {
            console.error('删除任务失败:', error);
            toast.error(error.response?.data?.error || '删除任务失败');
        },
    });

    // 触发任务 mutation
    const triggerMutation = useMutation({
        mutationFn: triggerScheduledTask,
        onSuccess: () => {
            queryClient.invalidateQueries({queryKey: ['scheduledTasks']});
            toast.success('任务已触发执行');
        },
        onError: (error: any) => {
            console.error('触发任务失败:', error);
            toast.error(error.response?.data?.error || '触发任务失败');
        },
    });

    // 重置表单
    const resetForm = () => {
        setFormData({
            name: '',
            enabled: false,
            intervalDays: 90,
            phoneNumber: '',
            content: '',
        });
    };

    // 打开添加对话框
    const handleOpenAddDialog = () => {
        setEditingTask(null);
        resetForm();
        setDialogOpen(true);
    };

    // 打开编辑对话框
    const handleOpenEditDialog = (task: ScheduledTask) => {
        setEditingTask(task);
        setFormData({
            name: task.name,
            enabled: task.enabled,
            intervalDays: task.intervalDays,
            phoneNumber: task.phoneNumber,
            content: task.content,
        });
        setDialogOpen(true);
    };

    // 更新表单字段
    const updateFormField = (field: keyof TaskFormData, value: any) => {
        setFormData({
            ...formData,
            [field]: value,
        });
    };

    // 提交表单
    const handleSubmit = () => {
        // 验证必填字段
        if (!formData.name.trim()) {
            toast.warning('请输入任务名称');
            return;
        }
        if (!formData.intervalDays || formData.intervalDays <= 0) {
            toast.warning('请输入有效的执行间隔天数（必须大于0）');
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

        if (editingTask) {
            // 更新任务
            updateMutation.mutate({id: editingTask.id, task: formData});
        } else {
            // 创建任务
            createMutation.mutate(formData);
        }
    };

    // 删除任务
    const handleDeleteTask = (id: string) => {
        if (confirm('确定要删除这个任务吗？')) {
            deleteMutation.mutate(id);
        }
    };

    // 触发任务
    const handleTriggerTask = (id: string) => {
        if (confirm('确定要立即执行这个任务吗？')) {
            triggerMutation.mutate(id);
        }
    };

    if (isLoading) {
        return (
            <div className="flex justify-center items-center py-20">
                <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
            </div>
        );
    }

    return (
        <div className="space-y-6 animate-in fade-in duration-300">
            <PageHeader
                title="计划任务"
                description="配置周期短信任务，并查看每项任务最近一次执行状态。"
                action={<Button
                    onClick={handleOpenAddDialog}
                    className="bg-[#0b2a55] px-5 py-2.5 text-white transition-colors hover:bg-slate-800"
                >
                    <Plus className="w-4 h-4 mr-2"/>
                    新建任务
                </Button>}
            />

            {tasks.length === 0 ? (
                <div className="rounded-2xl border border-slate-200 bg-white py-16 text-center">
                    <div className="w-16 h-16 bg-blue-50 rounded-full flex items-center justify-center mx-auto mb-4">
                        <Clock className="w-8 h-8 text-blue-500"/>
                    </div>
                    <p className="text-gray-500 mb-2 font-medium">暂无任务</p>
                    <p className="text-gray-400 text-sm">点击"新建任务"开始配置定时短信发送</p>
                </div>
            ) : (
                <div className="overflow-hidden rounded-2xl border border-slate-200 bg-white">
                    <div className="hidden grid-cols-[minmax(190px,1.3fr)_84px_132px_148px_82px_160px] gap-3 border-b border-slate-200 bg-slate-50 px-5 py-3 text-[11px] font-semibold text-slate-500 xl:grid">
                        <span>任务</span>
                        <span>周期</span>
                        <span>目标号码</span>
                        <span>上次执行</span>
                        <span>状态</span>
                        <span className="text-right">操作</span>
                    </div>
                    {tasks.map((task) => (
                        <div key={task.id} className="grid gap-4 border-b border-slate-100 px-5 py-4 last:border-b-0 xl:grid-cols-[minmax(190px,1.3fr)_84px_132px_148px_82px_160px] xl:items-center xl:gap-3">
                            <div className="flex min-w-0 items-start gap-3">
                                <span className={`flex size-9 shrink-0 items-center justify-center rounded-lg ${task.enabled ? 'bg-blue-50 text-blue-600' : 'bg-slate-100 text-slate-400'}`}>
                                    <Calendar size={17}/>
                                </span>
                                <div className="min-w-0">
                                    <p className="truncate text-sm font-bold text-slate-900">{task.name}</p>
                                    <p className="mt-1 line-clamp-1 text-xs text-slate-500">{task.content}</p>
                                </div>
                            </div>
                            <div className="text-sm font-semibold text-slate-700">
                                <span className="mr-2 text-xs text-slate-400 xl:hidden">周期</span>每 {task.intervalDays} 天
                            </div>
                            <div className="font-mono text-xs font-semibold text-slate-700">
                                <span className="mr-2 font-sans text-xs font-normal text-slate-400 xl:hidden">目标</span>{task.phoneNumber}
                            </div>
                            <div className="text-xs text-slate-600">
                                <span className="mr-2 text-slate-400 xl:hidden">上次执行</span>
                                {task.lastRunAt > 0
                                    ? new Date(task.lastRunAt).toLocaleString('zh-CN', {month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit'})
                                    : '从未执行'}
                            </div>
                            <div>
                                {task.lastRunStatus ? (() => {
                                    const statusInfo = getStatusDisplay(task.lastRunStatus);
                                    const StatusIcon = statusInfo.icon;
                                    return (
                                        <span className={`inline-flex items-center gap-1 rounded-full px-2 py-1 text-xs font-medium ${statusInfo.bgClass} ${statusInfo.colorClass}`}>
                                            <StatusIcon className="size-3"/>{statusInfo.text}
                                        </span>
                                    );
                                })() : (
                                    <span className={`inline-flex items-center gap-1.5 text-xs font-medium ${task.enabled ? 'text-emerald-600' : 'text-slate-400'}`}>
                                        <span className={`size-1.5 rounded-full ${task.enabled ? 'bg-emerald-500' : 'bg-slate-300'}`}/>
                                        {task.enabled ? '运行中' : '已暂停'}
                                    </span>
                                )}
                            </div>
                            <div className="flex items-center gap-2 xl:justify-end">
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        onClick={() => handleTriggerTask(task.id)}
                                        disabled={triggerMutation.isPending}
                                        className="text-xs font-medium hover:border-blue-200 hover:bg-blue-50 hover:text-blue-700"
                                    >
                                        <Play className="w-3.5 h-3.5"/>
                                        触发
                                    </Button>
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        onClick={() => handleOpenEditDialog(task)}
                                        className="text-xs font-medium hover:bg-gray-50"
                                    >
                                        <Edit className="w-3.5 h-3.5"/>
                                        编辑
                                    </Button>
                                    <Button
                                        variant="outline"
                                        size="sm"
                                        onClick={() => handleDeleteTask(task.id)}
                                        disabled={deleteMutation.isPending}
                                        className="border-rose-200 px-2.5 text-rose-600 hover:bg-rose-50 hover:text-rose-700"
                                        aria-label="删除任务"
                                    >
                                        <Trash2 className="w-3.5 h-3.5"/>
                                    </Button>
                            </div>
                        </div>
                    ))}
                </div>
            )}

            {/* 添加/编辑任务对话框 */}
            <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
                <DialogContent className="sm:max-w-[500px]">
                    <DialogHeader>
                        <DialogTitle className="text-xl font-bold text-gray-800">
                            {editingTask ? '编辑任务' : '新建任务'}
                        </DialogTitle>
                        <DialogDescription className="text-sm text-gray-500">
                            {editingTask ? '修改定时任务的配置信息' : '创建新的定时短信任务，系统将按照设定的间隔自动发送'}
                        </DialogDescription>
                    </DialogHeader>

                    <div className="space-y-5 py-2">
                        {/* 任务名称 */}
                        <div>
                            <label className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide">
                                任务名称 <span className="text-red-500">*</span>
                            </label>
                            <Input
                                value={formData.name}
                                onChange={(e) => updateFormField('name', e.target.value)}
                                placeholder="例如：90天流量查询"
                                className="bg-gray-50 border-gray-200 focus:bg-white focus:border-blue-500 focus:ring-1 focus:ring-blue-500 transition-all"
                            />
                        </div>

                        {/* 启用状态 */}
                        <div className="bg-gray-50 border border-gray-200 rounded-lg p-3.5 flex items-center gap-3">
                            <input
                                type="checkbox"
                                id="enabled"
                                checked={formData.enabled}
                                onChange={(e) => updateFormField('enabled', e.target.checked)}
                                className="rounded border-gray-300 text-blue-600 focus:ring-blue-500 w-4 h-4 cursor-pointer"
                            />
                            <label htmlFor="enabled"
                                   className="text-sm font-medium text-gray-700 cursor-pointer flex-1">
                                启用此任务
                            </label>
                            <div
                                className={`w-2 h-2 rounded-full ${formData.enabled ? 'bg-green-500' : 'bg-gray-300'}`}></div>
                        </div>

                        <div className="grid grid-cols-2 gap-4">
                            {/* 执行间隔天数 */}
                            <div className="col-span-2 sm:col-span-1">
                                <label
                                    className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide flex items-center gap-1.5">
                                    <Clock size={12} className="text-gray-400"/>
                                    执行间隔 <span className="text-red-500">*</span>
                                </label>
                                <div className="relative">
                                    <Input
                                        type="number"
                                        min="1"
                                        value={formData.intervalDays}
                                        onChange={(e) => updateFormField('intervalDays', parseInt(e.target.value) || 0)}
                                        placeholder="90"
                                        className="bg-gray-50 border-gray-200 focus:bg-white focus:border-blue-500 focus:ring-1 focus:ring-blue-500 transition-all pr-12"
                                    />
                                    <span
                                        className="absolute right-3 top-1/2 -translate-y-1/2 text-xs text-gray-400 font-medium">天</span>
                                </div>
                                <p className="text-xs text-gray-400 mt-1.5">
                                    任务执行的时间间隔
                                </p>
                            </div>

                            {/* 目标手机号 */}
                            <div className="col-span-2 sm:col-span-1">
                                <label
                                    className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide flex items-center gap-1.5">
                                    <Phone size={12} className="text-gray-400"/>
                                    目标号码 <span className="text-red-500">*</span>
                                </label>
                                <Input
                                    value={formData.phoneNumber}
                                    onChange={(e) => updateFormField('phoneNumber', e.target.value)}
                                    placeholder="10086"
                                    className="bg-gray-50 border-gray-200 focus:bg-white focus:border-blue-500 focus:ring-1 focus:ring-blue-500 transition-all font-mono"
                                />
                                <p className="text-xs text-gray-400 mt-1.5">
                                    接收短信的手机号码
                                </p>
                            </div>
                        </div>

                        {/* 短信内容 */}
                        <div>
                            <label
                                className="block text-xs font-semibold text-gray-600 mb-2 uppercase tracking-wide flex items-center gap-1.5">
                                <MessageSquare size={12} className="text-gray-400"/>
                                短信内容 <span className="text-red-500">*</span>
                            </label>
                            <textarea
                                value={formData.content}
                                onChange={(e) => updateFormField('content', e.target.value)}
                                placeholder="例如：查询流量、CXLL 等"
                                rows={3}
                                className="w-full bg-gray-50 border border-gray-200 rounded-lg px-3 py-2.5 text-sm focus:bg-white focus:border-blue-500 focus:ring-1 focus:ring-blue-500 transition-all outline-none resize-none"
                            />
                            <p className="text-xs text-red-400 mt-1.5">
                                将要发送的短信内容，不支持 Emoji 等特殊字符。
                            </p>
                        </div>
                    </div>

                    <DialogFooter className="bg-gray-50 -mx-6 -mb-6 px-6 py-4 rounded-b-lg border-t border-gray-100">
                        <Button
                            variant="outline"
                            onClick={() => {
                                setDialogOpen(false);
                                setEditingTask(null);
                                resetForm();
                            }}
                            disabled={createMutation.isPending || updateMutation.isPending}
                            className="hover:bg-white transition-colors"
                        >
                            取消
                        </Button>
                        <Button
                            onClick={handleSubmit}
                            disabled={createMutation.isPending || updateMutation.isPending}
                            className="min-w-[100px] bg-[#0b2a55] text-white transition-colors hover:bg-slate-800"
                        >
                            {createMutation.isPending || updateMutation.isPending
                                ? '提交中...'
                                : editingTask
                                    ? '更新任务'
                                    : '创建任务'}
                        </Button>
                    </DialogFooter>
                </DialogContent>
            </Dialog>
        </div>
    );
}
