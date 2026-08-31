import { useEffect, useRef, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api, type Depth5Level } from '@/lib/api'

interface Props {
  symbol: string
  /** 轮询间隔(ms)。undefined = 不轮询 (仅拉一次)。 */
  refetchIntervalMs?: number
  /** 面板高度(px), 与相邻走势图对齐。 */
  height?: number
  className?: string
}

type Level = { price: number; volume: number }
type LevelRow = Level | null

function fmtVol(v: number): string {
  if (v >= 10000) return `${(v / 10000).toFixed(1)}万`
  return v.toLocaleString()
}

/** 不足 5 档补 null 占位, 保证买卖两侧恒为 5 行。 */
function pad5(levels: Depth5Level[]): LevelRow[] {
  return [0, 1, 2, 3, 4].map(i => levels[i] ?? null)
}

/**
 * 五档盘口实时订单列表 — 卖5..卖1 / 买1..买5 (价格 + 手数)。
 *
 * 单一数据源契约: 卖侧与买侧由 **同一个** GET /api/intraday/depth5 响应原子渲染
 * (一次 TDX 快照同时产出两侧), 任意一次刷新都整体替换两侧, 不存在两侧错位。
 * 数据源: tdx_gateway (VM 通达信, 无套餐限制) → tickflow (Pro+) 兜底。
 * 档位价格/数量变动时该行高亮闪烁 (~0.7s), 使两侧同步更新肉眼可见。
 */
export function Depth5Panel({ symbol, refetchIntervalMs, height = 420, className }: Props) {
  const depth = useQuery({
    queryKey: ['depth5', symbol],
    queryFn: () => api.depth5(symbol),
    enabled: !!symbol,
    retry: false,
    refetchInterval: refetchIntervalMs,
    refetchIntervalInBackground: true,
    staleTime: 3000,
  })

  const snapshot = depth.data
  const asks = [...(snapshot?.asks ?? [])].reverse() // 卖5..卖1 (顶→底)
  const bids = snapshot?.bids ?? []                  // 买1..买5 (顶→底)
  const askRows = pad5(asks)
  const bidRows = pad5(bids)

  // ── 变动闪烁: 与上一快照逐档比对 (价格或量任一变化即高亮该行) ──
  const prevSig = useRef<Map<string, string>>(new Map())
  const [flashKeys, setFlashKeys] = useState<Set<string>>(new Set())
  useEffect(() => {
    if (!snapshot) return
    const nextSig = new Map<string, string>()
    const nextFlash = new Set<string>()
    const track = (key: string, row: LevelRow) => {
      if (!row) return
      const sig = `${row.price}|${row.volume}`
      const prev = prevSig.current.get(key)
      if (prev !== undefined && prev !== sig) nextFlash.add(key)
      nextSig.set(key, sig)
    }
    asks.forEach((row, i) => track(`ask${4 - i}`, row)) // i=0 是卖1 (反转后末位)
    bids.forEach((row, i) => track(`bid${i}`, row))
    prevSig.current = nextSig
    if (nextFlash.size > 0) {
      setFlashKeys(nextFlash)
      const t = setTimeout(() => setFlashKeys(new Set()), 700)
      return () => clearTimeout(t)
    }
    setFlashKeys(new Set())
  }, [snapshot])

  const noCap = !!depth.error
  const ts = snapshot?.ts ? new Date(snapshot.ts).toLocaleTimeString('zh-CN', { hour12: false }) : null
  const sourceLabel = snapshot?.source === 'tdx_gateway' ? 'TDX' : snapshot?.source === 'tickflow' ? 'TickFlow' : null

  // 价差 = 卖一 - 买一 (两侧同快照, 天然一致)
  const ask1 = bids.length >= 0 ? (asks[0] ?? null) : null
  const bid1 = bids[0] ?? null
  const spread = ask1 && bid1 ? +(ask1.price - bid1.price).toFixed(2) : null

  const renderRow = (key: string, label: string, row: LevelRow, side: 'ask' | 'bid') => {
    const color = side === 'ask' ? 'text-bear' : 'text-bull'
    const barColor = side === 'ask' ? 'rgba(240,68,56,0.25)' : 'rgba(18,183,106,0.25)'
    const flash = flashKeys.has(key)
    return (
      <div
        key={key}
        className={`relative flex items-center justify-between px-2 py-[3px] font-mono text-[11px] transition-colors duration-300 ${
          flash ? 'bg-accent/20' : ''
        }`}
      >
        <div
          className="absolute inset-y-0 right-0 transition-[width] duration-300"
          style={{
            width: row ? `${(row.volume / Math.max(1, ...askRows.concat(bidRows).filter(Boolean).map(r => r!.volume))) * 55}%` : 0,
            background: row && row.volume > 0 ? barColor : 'transparent',
          }}
        />
        <span className="relative z-10 text-muted">{label}</span>
        {row ? (
          <>
            <span className={`relative z-10 font-semibold ${row.volume === 0 ? 'text-muted' : color}`}>
              {row.price.toFixed(2)}
            </span>
            <span className={`relative z-10 text-secondary ${flash ? 'font-bold text-foreground' : ''}`}>
              {fmtVol(row.volume)}
            </span>
          </>
        ) : (
          <span className="relative z-10 text-muted/40">—</span>
        )}
      </div>
    )
  }

  return (
    <div
      className={`flex flex-col rounded-card border border-border bg-surface/60 overflow-hidden ${className ?? ''}`}
      style={{ height }}
      data-testid="depth5-panel"
    >
      {/* 标题行: 名称 + 数据源 + 快照时间 */}
      <div className="flex shrink-0 items-center justify-between border-b border-border/60 px-2 py-1">
        <span className="flex items-center gap-1.5 text-[11px] font-semibold text-foreground">
          {refetchIntervalMs != null && snapshot && (
            <span className="relative flex h-1.5 w-1.5">
              <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-bull opacity-60" />
              <span className="relative inline-flex h-1.5 w-1.5 rounded-full bg-bull" />
            </span>
          )}
          五档盘口
        </span>
        <span className="flex items-center gap-1.5">
          {sourceLabel && (
            <span className="rounded bg-elevated px-1 py-0.5 text-[9px] text-muted">{sourceLabel}</span>
          )}
          {ts && <span className="font-mono text-[10px] text-muted">{ts}</span>}
        </span>
      </div>

      {noCap ? (
        <div className="flex flex-1 flex-col items-center justify-center gap-1 px-3 text-center">
          <span className="text-[11px] leading-relaxed text-muted">
            {depth.error instanceof Error && depth.error.message
              ? depth.error.message
              : '五档数据不可用 (需配置 TDX 网关或 Pro+ 套餐)'}
          </span>
          <a href="/settings?tab=data-sources" className="text-[10px] text-accent hover:underline">
            前往数据源设置 →
          </a>
        </div>
      ) : depth.isLoading ? (
        <div className="flex flex-1 items-center justify-center text-xs text-muted">加载中…</div>
      ) : (
        <>
          {/* 卖五..卖一 */}
          <div className="flex flex-col">
            {askRows.map((row, i) => renderRow(`ask${4 - i}`, `卖${5 - i}`, row, 'ask'))}
          </div>
          {/* 中间条: 最新价差 (两侧同一快照, 天然一致) */}
          <div className="flex shrink-0 items-center justify-between border-y border-border/40 bg-elevated/50 px-2 py-1">
            <span className="text-[10px] text-muted">价差</span>
            <span className="font-mono text-[11px] text-foreground">
              {spread != null ? spread.toFixed(2) : '—'}
            </span>
          </div>
          {/* 买一..买五 */}
          <div className="flex flex-1 flex-col">
            {bidRows.map((row, i) => renderRow(`bid${i}`, `买${i + 1}`, row, 'bid'))}
          </div>
        </>
      )}
    </div>
  )
}
