import { cn } from '@/lib/cn'

interface Props {
  title: string
  subtitle?: React.ReactNode
  /** 标题右侧、subtitle 之前的额外节点(如状态徽标) */
  titleExtra?: React.ReactNode
  right?: React.ReactNode
  className?: string
  /** 移动端 (<768px) titleExtra 纵向堆叠 — 计数等内容显示在标题下方; 桌面不变 */
  stackTitleOnMobile?: boolean
}

export function PageHeader({ title, subtitle, titleExtra, right, className, stackTitleOnMobile = false }: Props) {
  return (
    <header
      className={cn(
        'px-5 pt-3 pb-2 border-b border-border flex items-center justify-between gap-4',
        className,
      )}
    >
      <div
        className={cn(
          'flex items-center gap-2',
          stackTitleOnMobile && 'flex-col items-start gap-0.5 md:flex-row md:items-center md:gap-2',
        )}
      >
        <h1 className="text-lg font-semibold tracking-tight">{title}</h1>
        {titleExtra}
        {subtitle && <span className="text-xs text-muted">{subtitle}</span>}
      </div>
      {right}
    </header>
  )
}
