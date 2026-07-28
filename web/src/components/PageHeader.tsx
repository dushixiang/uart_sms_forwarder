import type {ReactNode} from 'react';

interface PageHeaderProps {
    title: string;
    description: string;
    action?: ReactNode;
}

export function PageHeader({title, description, action}: PageHeaderProps) {
    return (
        <header className="flex min-h-[60px] flex-col justify-end gap-4 sm:flex-row sm:items-end sm:justify-between">
            <div>
                <h1 className="text-[28px] font-bold tracking-[-0.04em] text-slate-950">{title}</h1>
                <p className="mt-1 max-w-2xl text-sm text-slate-500">{description}</p>
            </div>
            {action && <div className="shrink-0">{action}</div>}
        </header>
    );
}
