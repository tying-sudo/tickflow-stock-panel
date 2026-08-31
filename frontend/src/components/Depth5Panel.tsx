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

/** 委托金额 (元): 价格×手数×100, 对齐通达信 L2 十档的金额口径, 实盘直观比较压单/托单强度。 */
function fmtAmt(price: number, volume: number): string {
  const yuan = price * volume * 100
  if (yuan >= 1e8) return `${(yuan / 1e8).toFixed(2)}亿`
  if (yuan >= 1e4) return `${(yuan / 1e4).toFixed(1)}万`
  return `${Math.round(yuan)}元`
}

/** 不足 count 档补 null 占位, 保证买卖两侧行数恒一致 (L1=5, L2=10)。 */
function padN(levels: Depth5Level[], count: number): LevelRow[] {
  return Array.from({ length: count }, (_, i) => levels[i] ?? null)
}

/**
 * 盘口实时订单列表 — 卖N..卖1 / 买1..买N (价格 + 手数 + 委托金额)。
 *
 * 档位自适应: 数据源返回 5 档 (TdxW Quant L1) 渲染 5 行, 返回 10 档 (L2)
 * 渲染 10 行; 源不可给 L2 时不会显示空占位的额外档位。
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
  const rawAsks = snapshot?.asks ?? []               // 卖1..卖N (升序, 卖1在前)
  const asks = [...rawAsks].reverse()                // 卖N..卖1 (顶→底, 用于渲染)
  const bids = snapshot?.bids ?? []                  // 买1..买N (顶→底)
  // 档位自适应: 数据源给 5 档 (TdxW Quant L1) 渲染 5 行, 给 10 档 (L2) 渲染 10 行;
  // L2 不可用时不会出现空占位的"多出来那 5 档"。
  const levelCount = Math.min(10, Math.max(rawAsks.length, bids.length, 5))
  const isL2 = levelCount > 5
  const askRows = padN(asks, levelCount)
  const bidRows = padN(bids, levelCount)

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

  // 涨跌停封板判定 (对齐通达信语义): 单侧档位全空 = 对侧封死。
  // 涨停: 卖档全空、买1 即封单; 跌停: 买档全空、卖1 即封单。
  const ask1 = rawAsks[0] ?? null // 卖1 = 升序数组首项 (反转前的 asks[0])
  const bid1 = bids[0] ?? null
  const sealedSide: 'up' | 'down' | null =
    !ask1 && bid1 ? 'up' : !bid1 && ask1 ? 'down' : null
  const allEmpty = !rawAsks.length && !bids.length
  const spread = ask1 && bid1 ? +(ask1.price - bid1.price).toFixed(2) : null

  const renderRow = (key: string, label: string, row: LevelRow, side: 'ask' | 'bid') => {
    const color = side === 'ask' ? 'text-bear' : 'text-bull'
    const barColor = side === 'ask' ? 'rgba(240,68,56,0.25)' : 'rgba(18,183,106,0.25)'
    const flash = flashKeys.has(key)
    return (
      <div
        key={key}
        className={`relative flex items-center px-2 font-mono text-[11px] transition-colors duration-300 ${
          isL2 ? 'py-[2px]' : 'py-[3px]'
        } ${flash ? 'bg-accent/20' : ''}`}
      >
        <div
          className="absolute inset-y-0 right-0 transition-[width] duration-300"
          style={{
            width: row ? `${(row.volume / Math.max(1, ...askRows.concat(bidRows).filter(Boolean).map(r => r!.volume))) * 55}%` : 0,
            background: row && row.volume > 0 ? barColor : 'transparent',
          }}
        />
        <span className="relative z-10 w-7 shrink-0 text-muted">{label}</span>
        {row ? (
          <>
            <span className={`relative z-10 flex-1 text-right font-semibold ${row.volume === 0 ? 'text-muted' : color}`}>
              {row.price.toFixed(2)}
            </span>
            <span
              className={`relative z-10 w-12 shrink-0 text-right text-secondary ${flash ? 'font-bold text-foreground' : ''}`}
              title={`${row.volume} 手 = ${(row.volume * 100).toLocaleString()} 股 (A股 1手=100股)`}
            >
              {fmtVol(row.volume)}
            </span>
            <span
              className="relative z-10 w-14 shrink-0 text-right text-[10px] text-muted"
              title="该档委托金额 (价格×手数×100)"
            >
              {fmtAmt(row.price, row.volume)}
            </span>
          </>
        ) : (
          <>
            <span className="relative z-10 flex-1 text-right text-muted/40">—</span>
            <span className="relative z-10 w-12 shrink-0 text-right text-muted/40">—</span>
            <span className="relative z-10 w-14 shrink-0 text-right text-muted/40">—</span>
          </>
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
          {isL2 ? '十档盘口' : '五档盘口'}
        </span>
        <span className="flex items-center gap-1.5">
          <span className={`rounded px-1 py-0.5 text-[9px] font-semibold ${isL2 ? 'bg-accent/15 text-accent' : 'bg-elevated text-muted'}`}>
            {isL2 ? 'L2 十档' : 'L1 五档'}
          </span>
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
          {allEmpty && (
            <div className="border-b border-border/40 px-2 py-1.5 text-center text-[10px] leading-relaxed text-muted">
              暂无有效盘口档位 — 数据源延迟, 已拦截与最新价矛盾的陈旧快照
            </div>
          )}
          {/* 卖N..卖1 */}
          <div className="flex flex-col">
            {askRows.map((row, i) => renderRow(`ask${levelCount - 1 - i}`, `卖${levelCount - i}`, row, 'ask'))}
          </div>
          {/* 中间条: 封板徽标 / 价差 (两侧同一快照, 天然一致) */}
          <div className="flex shrink-0 items-center justify-between border-y border-border/40 bg-elevated/50 px-2 py-1">
            {sealedSide === 'up' && bid1 ? (
              <>
                <span className="rounded bg-bull/15 px-1.5 py-0.5 text-[10px] font-semibold text-bull">涨停封板</span>
                <span className="font-mono text-[11px] font-semibold text-bull">
                  封单 {fmtVol(bid1.volume)}手 · {fmtAmt(bid1.price, bid1.volume)}
                </span>
              </>
            ) : sealedSide === 'down' && ask1 ? (
              <>
                <span className="rounded bg-bear/15 px-1.5 py-0.5 text-[10px] font-semibold text-bear">跌停封板</span>
                <span className="font-mono text-[11px] font-semibold text-bear">
                  封单 {fmtVol(ask1.volume)}手 · {fmtAmt(ask1.price, ask1.volume)}
                </span>
              </>
            ) : (
              <>
                <span className="text-[10px] text-muted">价差</span>
                <span className="font-mono text-[11px] text-foreground">
                  {spread != null ? spread.toFixed(2) : '—'}
                </span>
              </>
            )}
          </div>
          {/* 买1..买N */}
          <div className="flex flex-1 flex-col">
            {bidRows.map((row, i) => renderRow(`bid${i}`, `买${i + 1}`, row, 'bid'))}
          </div>
        </>
      )}
    </div>
  )
}
