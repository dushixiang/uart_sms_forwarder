import {useEffect, useRef, useState} from 'react';
import {Loader2, MoreVertical, Plus, RefreshCw, Search, Send, Trash2, User} from 'lucide-react';
import {useSearchParams} from 'react-router-dom';
import {toast} from 'sonner';
import {clearMessages, getConversations, getConversationMessages, deleteConversation, deleteMessage} from '../api/messages';
import {getStatus, sendSMS} from '../api/serial';
import {Input} from '@/components/ui/input';
import {Button} from '@/components/ui/button';
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
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import {useMutation, useQuery, useQueryClient} from '@tanstack/react-query';
import type {Conversation, DeviceStatus, TextMessage} from '@/api/types';
import {PageHeader} from '@/components/PageHeader';

export default function Messages() {
    const queryClient = useQueryClient();
    const messagesEndRef = useRef<HTMLDivElement>(null);
    const [searchParams, setSearchParams] = useSearchParams();

    // 选中的联系人
    const [selectedPeer, setSelectedPeer] = useState<string | null>(null);
    // 输入框内容
    const [inputText, setInputText] = useState('');
    // 搜索关键词
    const [searchQuery, setSearchQuery] = useState('');
    const [composeOpen, setComposeOpen] = useState(searchParams.get('compose') === '1');
    const [newRecipient, setNewRecipient] = useState('');
    const [newContent, setNewContent] = useState('');

    // 根据手机号生成头像颜色
    const getAvatarColor = (phoneNumber: string) => {
        const colors = [
            'bg-blue-600',
            'bg-blue-700',
            'bg-blue-800',
            'bg-blue-700',
            'bg-blue-500',
            'bg-blue-700',
            'bg-slate-600',
            'bg-blue-500',
        ];
        // 使用手机号的数字总和来选择颜色
        const sum = phoneNumber.split('').reduce((acc, char) => acc + char.charCodeAt(0), 0);
        return colors[sum % colors.length];
    };

    // 使用新的会话列表 API
    const {data: conversations = [], isLoading, refetch} = useQuery<Conversation[]>({
        queryKey: ['conversations'],
        queryFn: getConversations,
        refetchInterval: 5000, // 每 5 秒自动刷新
    });

    const {data: deviceStatus} = useQuery<DeviceStatus>({
        queryKey: ['deviceStatus'],
        queryFn: async () => getStatus() as Promise<DeviceStatus>,
        refetchInterval: 10000,
    });

    // 获取指定会话的所有消息
    const {data: currentMessages = []} = useQuery<TextMessage[]>({
        queryKey: ['conversation-messages', selectedPeer],
        queryFn: () => {
            if (!selectedPeer) return Promise.resolve([]);
            return getConversationMessages(selectedPeer);
        },
        enabled: !!selectedPeer,
        refetchInterval: 5000,
    });

    // 发送短信 Mutation
    const sendSMSMutation = useMutation({
        mutationFn: (data: { to: string; content: string }) => sendSMS(data),
        onSuccess: (_, variables) => {
            setInputText('');
            setNewRecipient('');
            setNewContent('');
            setComposeOpen(false);
            setSelectedPeer(variables.to);
            const nextParams = new URLSearchParams(searchParams);
            nextParams.delete('compose');
            setSearchParams(nextParams, {replace: true});
            toast.success('短信已提交发送');
            // 刷新会话列表和当前会话消息
            queryClient.invalidateQueries({queryKey: ['conversations']});
            queryClient.invalidateQueries({queryKey: ['conversation-messages']});
        },
        onError: (error) => {
            console.error('发送失败:', error);
            toast.error('发送失败');
        },
    });

    // 清空所有短信
    const clearMutation = useMutation({
        mutationFn: clearMessages,
        onSuccess: () => {
            toast.success('清空成功');
            setSelectedPeer(null);
            queryClient.invalidateQueries({queryKey: ['conversations']});
            queryClient.invalidateQueries({queryKey: ['conversation-messages']});
        },
        onError: (error) => {
            console.error('清空失败:', error);
            toast.error('清空失败');
        },
    });

    // 删除整个会话
    const deleteConversationMutation = useMutation({
        mutationFn: (peer: string) => deleteConversation(peer),
        onSuccess: (_, peer) => {
            toast.success('会话已删除');
            // 如果删除的是当前选中的会话，清除选中状态
            if (selectedPeer === peer) {
                setSelectedPeer(null);
            }
            queryClient.invalidateQueries({queryKey: ['conversations']});
        },
        onError: (error) => {
            console.error('删除失败:', error);
            toast.error('删除会话失败');
        },
    });

    // 删除单条消息
    const deleteMessageMutation = useMutation({
        mutationFn: (messageId: string) => deleteMessage(messageId),
        onSuccess: () => {
            toast.success('消息已删除');
            queryClient.invalidateQueries({queryKey: ['conversations']});
            queryClient.invalidateQueries({queryKey: ['conversation-messages']});
        },
        onError: (error) => {
            console.error('删除失败:', error);
            toast.error('删除消息失败');
        },
    });

    // 自动滚动到底部
    useEffect(() => {
        messagesEndRef.current?.scrollIntoView({behavior: "smooth"});
    }, [selectedPeer, currentMessages]);

    // 获取当前选中的会话信息
    const activeConversation = conversations.find(c => c.peer === selectedPeer);

    // 过滤会话列表
    const filteredConversations = conversations.filter(conv =>
        conv.peer.toLowerCase().includes(searchQuery.toLowerCase()) ||
        conv.lastMessage.content.toLowerCase().includes(searchQuery.toLowerCase())
    );

    const handleSendSMS = (e: React.FormEvent) => {
        e.preventDefault();
        if (!selectedPeer || !inputText.trim()) {
            toast.warning('请输入短信内容');
            return;
        }
        sendSMSMutation.mutate({to: selectedPeer, content: inputText});
    };

    const handleSendNewSMS = (event: React.FormEvent) => {
        event.preventDefault();
        const recipient = newRecipient.trim();
        const message = newContent.trim();
        if (!recipient || !message) {
            toast.warning('请输入目标手机号和短信内容');
            return;
        }
        sendSMSMutation.mutate({to: recipient, content: message});
    };

    const handleComposeOpenChange = (open: boolean) => {
        setComposeOpen(open);
        if (!open) {
            const nextParams = new URLSearchParams(searchParams);
            nextParams.delete('compose');
            setSearchParams(nextParams, {replace: true});
        }
    };

    const handleClear = () => {
        if (!confirm('确定要清空所有短信吗？此操作不可恢复！')) return;
        clearMutation.mutate();
    };

    const handleDeleteConversation = () => {
        if (!selectedPeer) return;
        if (!confirm(`确定要删除与 ${selectedPeer} 的所有消息吗？此操作不可恢复！`)) return;
        deleteConversationMutation.mutate(selectedPeer);
    };

    const handleDeleteMessage = (messageId: string, e: React.MouseEvent) => {
        e.stopPropagation();
        if (!confirm('确定要删除这条消息吗？此操作不可恢复！')) return;
        deleteMessageMutation.mutate(messageId);
    };

    const formatTime = (timestamp: number) => {
        const date = new Date(timestamp);
        const now = new Date();
        const diff = now.getTime() - date.getTime();
        const oneDay = 24 * 60 * 60 * 1000;

        // 今天
        if (diff < oneDay && date.getDate() === now.getDate()) {
            return date.toLocaleTimeString('zh-CN', {hour: '2-digit', minute: '2-digit'});
        }
        // 昨天
        if (diff < 2 * oneDay && date.getDate() === now.getDate() - 1) {
            return '昨天 ' + date.toLocaleTimeString('zh-CN', {hour: '2-digit', minute: '2-digit'});
        }
        // 更早
        return date.toLocaleDateString('zh-CN', {month: '2-digit', day: '2-digit'}) + ' ' +
            date.toLocaleTimeString('zh-CN', {hour: '2-digit', minute: '2-digit'});
    };

    const getStatusBadge = (status: string) => {
        switch (status) {
            case 'sent':
                return <span className="text-[10px] text-green-600">✓ 已发送</span>;
            case 'failed':
                return <span className="text-[10px] text-red-600">✗ 失败</span>;
            case 'sending':
                return <span className="text-[10px] text-gray-400">发送中...</span>;
            default:
                return null;
        }
    };

    const connected = Boolean(deviceStatus?.connected);

    if (isLoading) {
        return (
            <div className="flex min-h-[560px] h-[calc(100dvh-108px)] items-center justify-center">
                <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
            </div>
        );
    }

    return (
        <div className="flex h-[calc(100dvh-108px)] min-h-[560px] flex-col">
            {/* 顶部操作栏 */}
            <PageHeader
                title="短信中心"
                description="查看短信会话、搜索历史记录，或向任意号码发送新短信。"
                action={<div className="flex gap-2">
                    <Button
                        onClick={() => setComposeOpen(true)}
                        size="sm"
                        className="bg-blue-600 text-white hover:bg-blue-700"
                    >
                        <Plus className="mr-2 size-4"/>
                        新建短信
                    </Button>
                    <Button
                        onClick={() => refetch()}
                        variant="outline"
                        size="sm"
                        className="hover:bg-gray-50"
                    >
                        <RefreshCw className="w-4 h-4 mr-2"/>
                        刷新
                    </Button>
                    <DropdownMenu>
                        <DropdownMenuTrigger asChild>
                            <Button variant="outline" size="sm" className="hover:bg-gray-50">
                                <MoreVertical className="w-4 h-4 mr-2"/>
                                更多
                            </Button>
                        </DropdownMenuTrigger>
                        <DropdownMenuContent align="end">
                            <DropdownMenuItem
                                onClick={handleClear}
                                className="cursor-pointer text-rose-600 focus:bg-rose-50 focus:text-rose-700"
                            >
                                <Trash2 className="mr-2 size-4"/>
                                清空所有短信
                            </DropdownMenuItem>
                        </DropdownMenuContent>
                    </DropdownMenu>
                </div>}
            />

            {/* 聊天界面 */}
            <div
                className="mt-6 flex min-h-0 flex-1 overflow-hidden rounded-2xl border border-slate-200 bg-white">
                {/* 左侧：会话列表 */}
                <div className={`${
                    selectedPeer ? 'hidden md:flex' : 'flex'
                } w-full flex-col border-r border-gray-200 bg-white md:w-[300px] xl:w-[330px]`}>
                    {/* 搜索框 */}
                    <div className="p-4 border-b border-gray-100">
                        <div className="relative">
                            <Search className="absolute left-3 top-2.5 w-4 h-4 text-gray-400"/>
                            <Input
                                type="text"
                                placeholder="搜索联系人或内容..."
                                value={searchQuery}
                                onChange={(e) => setSearchQuery(e.target.value)}
                                className="pl-9 pr-4 h-9 bg-gray-50 border-transparent focus:bg-white focus:border-blue-500"
                            />
                        </div>
                    </div>

                    {/* 会话列表 */}
                    <div className="flex-1 overflow-y-auto">
                        {filteredConversations.length === 0 ? (
                            <div className="flex flex-col items-center justify-center h-full text-gray-400">
                                <User className="w-12 h-12 mb-2 opacity-30"/>
                                <p className="text-sm">暂无会话</p>
                            </div>
                        ) : (
                            filteredConversations.map(conv => (
                                <div
                                    key={conv.peer}
                                    onClick={() => setSelectedPeer(conv.peer)}
                                    className={`p-4 cursor-pointer transition-all border-l-2 hover:bg-gray-50 ${
                                        selectedPeer === conv.peer
                                            ? 'bg-blue-50/50 border-blue-500'
                                            : 'border-transparent'
                                    }`}
                                >
                                    <div className="flex items-start justify-between mb-1">
                                        <div className="flex items-center space-x-2">
                                            <div
                                                className={`flex h-9 w-9 items-center justify-center rounded-full text-sm font-bold text-white ${getAvatarColor(conv.peer)}`}>
                                                {conv.peer.slice(-2)}
                                            </div>
                                            <span className={`text-sm font-semibold ${
                                                selectedPeer === conv.peer ? 'text-gray-900' : 'text-gray-700'
                                            }`}>
                                                {conv.peer}
                                            </span>
                                        </div>
                                        <span className="text-xs text-gray-400">
                                            {formatTime(conv.lastMessage.createdAt)}
                                        </span>
                                    </div>
                                    <p className="text-xs text-gray-500 line-clamp-2 ml-11">
                                        {conv.lastMessage.type === 'outgoing' && '我: '}
                                        {conv.lastMessage.content}
                                    </p>
                                </div>
                            ))
                        )}
                    </div>
                </div>

                {/* 右侧：聊天区域 */}
                <div className={`${
                    selectedPeer ? 'flex' : 'hidden md:flex'
                } flex-1 flex-col bg-gray-50/30`}>
                    {/* 聊天头部 */}
                    <div
                        className="flex h-15 shrink-0 items-center justify-between border-b border-gray-200 bg-white px-4 md:px-6">
                        {selectedPeer ? (
                            <>
                                <div className="flex items-center space-x-3">
                                    {/* 移动端返回按钮 */}
                                    <Button
                                        variant="ghost"
                                        size="sm"
                                        onClick={() => setSelectedPeer(null)}
                                        className="md:hidden -ml-2 text-gray-600"
                                    >
                                        <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                                                  d="M15 19l-7-7 7-7"/>
                                        </svg>
                                    </Button>
                                    <div
                                        className={`flex h-10 w-10 items-center justify-center rounded-full font-bold text-white ${getAvatarColor(selectedPeer)}`}>
                                        {selectedPeer.slice(-2)}
                                    </div>
                                    <div>
                                        <h3 className="text-sm font-bold text-gray-900">{selectedPeer}</h3>
                                        <span className="text-xs text-gray-500">
                                            共 {activeConversation?.messageCount || 0} 条消息
                                        </span>
                                    </div>
                                </div>
                                <DropdownMenu>
                                    <DropdownMenuTrigger asChild>
                                        <Button
                                            variant="ghost"
                                            size="sm"
                                            className="text-gray-400 hover:text-gray-600"
                                        >
                                            <MoreVertical className="w-4 h-4"/>
                                        </Button>
                                    </DropdownMenuTrigger>
                                    <DropdownMenuContent align="end">
                                        <DropdownMenuItem
                                            onClick={handleDeleteConversation}
                                            className="text-red-600 focus:text-red-700 focus:bg-red-50 cursor-pointer"
                                        >
                                            <Trash2 className="w-4 h-4 mr-2"/>
                                            删除会话
                                        </DropdownMenuItem>
                                    </DropdownMenuContent>
                                </DropdownMenu>
                            </>
                        ) : (
                            <div className="text-gray-400 text-sm">请选择会话</div>
                        )}
                    </div>

                    {/* 消息列表 */}
                    <div className="flex-1 overflow-y-auto p-4 md:p-6">
                        {selectedPeer && currentMessages.length > 0 ? (
                            <div className="mx-auto w-full max-w-[980px] space-y-4">
                                {currentMessages.map((msg) => (
                                    <div
                                        key={msg.id}
                                        className={`flex ${msg.type === 'outgoing' ? 'justify-end' : 'justify-start'} animate-in fade-in slide-in-from-bottom-2 duration-200 group`}
                                    >
                                        <div
                                            className={`relative flex max-w-[82%] flex-col sm:max-w-[75%] xl:max-w-[68%] ${msg.type === 'outgoing' ? 'items-end' : 'items-start'}`}>
                                            <div
                                                className={`rounded-2xl px-4 py-2.5 shadow-none text-sm leading-relaxed relative ${
                                                    msg.type === 'outgoing'
                                                        ? 'bg-[#0b2a55] text-white rounded-tr-sm'
                                                        : 'bg-white text-gray-800 border border-gray-100 rounded-tl-sm'
                                                }`}
                                            >
                                                <p className="break-words">{msg.content}</p>
                                                {/* 删除按钮 - 悬停时显示 */}
                                                <button
                                                    onClick={(e) => handleDeleteMessage(msg.id, e)}
                                                    className={`absolute -top-2 ${msg.type === 'outgoing' ? '-left-2' : '-right-2'} rounded-full border border-rose-200 bg-white p-1 text-rose-500 opacity-0 transition-opacity hover:bg-rose-50 group-hover:opacity-100`}
                                                    title="删除消息"
                                                >
                                                    <Trash2 className="w-3 h-3"/>
                                                </button>
                                            </div>
                                            <div className={`flex items-center space-x-2 mt-1 px-1 ${
                                                msg.type === 'outgoing' ? 'flex-row-reverse space-x-reverse' : ''
                                            }`}>
                                                <span
                                                    className={`text-[10px] ${msg.type === 'outgoing' ? 'text-blue-600' : 'text-gray-400'}`}>
                                                    {formatTime(msg.createdAt)}
                                                </span>
                                                {msg.type === 'outgoing' && getStatusBadge(msg.status)}
                                            </div>
                                        </div>
                                    </div>
                                ))}
                                <div ref={messagesEndRef}/>
                            </div>
                        ) : (
                            <div className="h-full flex flex-col items-center justify-center text-gray-400">
                                <Send className="w-12 h-12 mb-4 opacity-20"/>
                                <p className="text-sm">
                                    {selectedPeer ? '暂无消息，可以发送第一条短信' : '选择左侧联系人开始查看消息'}
                                </p>
                            </div>
                        )}
                    </div>

                    {/* 输入框 */}
                    <div className="p-4 bg-white border-t border-gray-200">
                        <form className="flex gap-3" onSubmit={handleSendSMS}>
                            <Input
                                type="text"
                                value={inputText}
                                onChange={(e) => setInputText(e.target.value)}
                                placeholder={!connected ? '设备未连接' : selectedPeer ? '输入消息内容...' : '请先选择联系人'}
                                disabled={!connected || !selectedPeer || sendSMSMutation.isPending}
                                className="flex-1 bg-gray-50 border-gray-200 focus:bg-white focus:border-blue-500 h-10"
                            />
                            <Button
                                type="submit"
                                disabled={!connected || !selectedPeer || !inputText.trim() || sendSMSMutation.isPending}
                                className="h-10 bg-[#0b2a55] px-6 text-white shadow-none hover:bg-slate-800"
                            >
                                {sendSMSMutation.isPending ? (
                                    <div
                                        className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"/>
                                ) : (
                                    <>
                                        <Send className="w-4 h-4 mr-2"/>
                                        发送
                                    </>
                                )}
                            </Button>
                        </form>
                    </div>
                </div>
            </div>

            <Dialog open={composeOpen} onOpenChange={handleComposeOpenChange}>
                <DialogContent className="sm:max-w-lg">
                    <form onSubmit={handleSendNewSMS}>
                        <DialogHeader>
                            <DialogTitle>新建短信</DialogTitle>
                            <DialogDescription>输入目标号码和短信内容，发送后将自动打开对应会话。</DialogDescription>
                        </DialogHeader>

                        <div className="space-y-5 py-5">
                            <div className="space-y-1.5">
                                <label htmlFor="new-sms-recipient" className="block text-sm font-medium text-slate-800">目标手机号</label>
                                <Input
                                    id="new-sms-recipient"
                                    type="tel"
                                    value={newRecipient}
                                    onChange={(event) => setNewRecipient(event.target.value)}
                                    placeholder="请输入手机号"
                                    autoComplete="tel"
                                    disabled={sendSMSMutation.isPending}
                                    autoFocus
                                />
                            </div>
                            <div className="space-y-1.5">
                                <div className="flex items-center justify-between">
                                    <label htmlFor="new-sms-content" className="block text-sm font-medium text-slate-800">短信内容</label>
                                    <span className="text-xs text-slate-400">{newContent.length} 字</span>
                                </div>
                                <Textarea
                                    id="new-sms-content"
                                    value={newContent}
                                    onChange={(event) => setNewContent(event.target.value)}
                                    placeholder="请输入短信内容"
                                    className="min-h-32 resize-none"
                                    disabled={sendSMSMutation.isPending}
                                />
                            </div>
                            {!connected && (
                                <p className="rounded-lg border border-rose-200 bg-rose-50 px-3.5 py-3 text-xs leading-5 text-rose-700">
                                    当前设备未连接，连接串口设备后才能发送短信。
                                </p>
                            )}
                        </div>

                        <DialogFooter>
                            <Button type="button" variant="outline" onClick={() => handleComposeOpenChange(false)} disabled={sendSMSMutation.isPending}>
                                取消
                            </Button>
                            <Button
                                type="submit"
                                disabled={!connected || !newRecipient.trim() || !newContent.trim() || sendSMSMutation.isPending}
                                className="bg-blue-600 text-white hover:bg-blue-700"
                            >
                                {sendSMSMutation.isPending ? <Loader2 className="size-4 animate-spin"/> : <Send className="size-4"/>}
                                {sendSMSMutation.isPending ? '发送中...' : '发送短信'}
                            </Button>
                        </DialogFooter>
                    </form>
                </DialogContent>
            </Dialog>
        </div>
    );
}
