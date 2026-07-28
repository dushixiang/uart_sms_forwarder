import React from 'react';
import {cn} from '@/lib/utils.ts';

export interface Props {
    label: string;
    value?: number | string;
    icon: React.FC<{size: number} & React.SVGProps<SVGSVGElement>>;
    unit?: string;
    subValue?: string;
    colorClass: string;
}

export const StatCard = ({label, value, icon: Icon, unit, subValue, colorClass}: Props) => (
    <div className="relative flex h-full flex-col overflow-hidden rounded-2xl border border-slate-200 bg-white p-4 transition-colors duration-200 hover:border-blue-300">
        <div className="flex items-start justify-between gap-4">
            <div className="min-w-0">
                <p className="text-xs font-semibold uppercase tracking-[0.11em] text-slate-400">{label}</p>
                <div className="mt-2 flex items-baseline gap-1.5">
                    <span className="truncate text-2xl font-bold tracking-[-0.04em] text-slate-900">{value ?? '—'}</span>
                    {unit && <span className="text-sm font-semibold text-slate-400">{unit}</span>}
                </div>
            </div>
            <div className={cn('flex size-10 shrink-0 items-center justify-center rounded-xl', colorClass)}>
                <Icon size={20}/>
            </div>
        </div>
        <div className="mt-auto min-h-5 border-t border-slate-100 pt-2.5">
            <p className="truncate text-xs text-slate-500">{subValue || '实时数据已同步'}</p>
        </div>
    </div>
);
