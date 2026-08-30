import { useQuery } from '@tanstack/react-query'
import { api } from '@/lib/api'
import { useChartTheme } from '@/lib/theme'

interface Props {
  symbol: string
  /** 轮询间隔(ms)。undefined = 不轮询 (仅拉一次)。 */
  refetchIntervalMs?: number
  /** 面板高度(px), 与相邻走势图对齐。 */
  height?: number
  className?: string
}

function fmtVol(v: number): string {
  if (v >= 10000) return `${(v / 10000).toFixed(1)}万`
  return v.toLocaleString()
}

/**
 * 五档盘口详情面板 — 卖五..卖一 / 买一..买五 (价格 + 手数)。
 *
 * 数据源: GET /api/intraday/depth5 (Cap.DEPTH5, Pro+)。
 * 无权限(403)时降级为提示卡; 其余错误短暂显示后保留重试。
 */
export function Depth5Panel({ symbol, refetchIntervalMs, height = 420, className }: Props) {
  const ct = useChartTheme()
  const depth = useQuery({
    queryKey: ['depth5', symbol],
    queryFn: () => api.depth5(symbol),
    enabled: !!symbol,
    retry: false,
    refetchInterval: refetchIntervalMs,
    refetchIntervalInBackground: true,
    staleTime: 3000,
  })

  const noCap = depth.error instanceof Error && depth.error.message.includes('403')
  const asks = [...(depth.data?.asks ?? [])].reverse() // 卖五在上, 卖一最靠近中间
  const bids = depth.data?.bids ?? []
  const ts = depth.data?.ts ? new Date(depth.data.ts).toLocaleTimeString('zh-CN', { hour12: false }) : null

  const row = (price: number, volume: number, side: 'ask' | 'bid') => {
    const color = side === 'ask' ? 'text-bear' : 'text-bull'
    const barColor = side === 'ask' ? 'rgba(240,68,56,0.28)' : 'rgba(18,183,106,0.28)'
    // 量占比条 (相对本侧最大量)
    const sideRows = side === 'ask' ? asks : bids
    const maxVol = Math.max(1, ...sideRows.map(r => r.volume))
    return (
      <div key={`${side}-${price}`} className="relative flex items-center justify-between px-2 py-[3px] font-mono text-[11px]">
        <div className="absolute inset-y-0 right-0" style={{ width: `${(volume / maxVol) * 70}%`, background: barColor }} />
        <span className="relative z-10 text-muted">{side === 'ask' ? '卖' : '买'}</span>
        <span className={`relative z-10 font-semibold ${color}`}>{price.toFixed(2)}</span>
        <span className="relative z-10 text-secondary">{fmtVol(volume)}</span>
      </div>
    )
  }

  return (
    <div
      className={`flex flex-col rounded-card border border-border bg-surface/60 overflow-hidden ${className ?? ''}`}
      style={{ height }}
      data-testid="depth5-panel"
    >
      <div className="flex shrink-0 items-center justify-between border-b border-border/60 px-2 py-1">
        <span className="text-[11px] font-semibold text-foreground">五档盘口</span>
        {ts && <span className="font-mono text-[10px] text-muted">{ts}</span>}
      </div>

      {noCap ? (
        <div className="flex flex-1 flex-col items-center justify-center gap-1 px-3 text-center">
          <span className="text-xs text-muted">五档数据需要 Pro+ 套餐</span>
          <a
            href="/settings?tab=data-sources"
            className="text-[10px] text-accent hover:underline"
          >
            查看数据源 →
          </a>
        </div>
      ) : depth.isLoading ? (
        <div className="flex flex-1 items-center justify-center text-xs text-muted">加载中…</div>
      ) : depth.data && (asks.length > 0 || bids.length > 0) ? (
        <>
          <div className="flex flex-col justify-start" style={{ minHeight: 0 }}>
            {asks.map(r => row(r.price, r.volume, 'ask'))}
          </div>
          <div
            className="my-0.5 flex items-center justify-between border-y border-border/40 bg-elevated/50 px-2 py-1 font-mono text-[11px]"
            style={{ color: ct.text }}
          >
            <span className="text-muted font-sans text-[10px]">最新</span>
          </div>
          <div className="flex flex-1 flex-col justify-start">
            {bids.map(r => row(r.price, r.volume, 'bid'))}
          </div>
        </>
      ) : (
        <div className="flex flex-1 items-center justify-center px-3 text-center text-xs text-muted">
          {depth.error ? '五档获取失败' : '暂无五档数据'}
        </div>
      )}
    </div>
  )
}
