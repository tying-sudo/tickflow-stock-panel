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

/** 委托金额 (元): 价格×手数×100 (封板徽标 / hover 提示用)。 */
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
 * 盘口实时订单列表 — 通达信 PC 横排双栏: 买1..买N (左) | 卖1..卖N (右), 三列 档位/价格/量。
 *
 * 档位自适应: 数据源返回 5 档 (TdxW Quant L1) 渲染 5 行, 返回 10 档 (L2)
 * 渲染 10 行; 源不可给 L2 时不会显示空占位的额外档位。
 *
 * 单一数据源契约: 卖侧与买侧由 **同一个** GET /api/intraday/depth5 响应原子渲染
 * (一次 TDX 快照同时产出两侧), 任意一次刷新都整体替换两侧, 不存在两侧错位。
 * 数据源: tdx_gateway (VM 通达信, 无套餐限制) → tickflow (Pro+) 兜底。
 * 档位价格/数量变动时该行高亮闪烁 (~0.7s); 封板时标题下显示封单徽标条。
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
  const rawAsks = snapshot?.asks ?? []               // 卖1..卖N (升序, 卖1在前, 顶→底渲染)
  const bids = snapshot?.bids ?? []                  // 买1..买N (降序, 买1在前, 顶→底渲染)
  // 档位自适应: 数据源给 5 档 (TdxW Quant L1) 渲染 5 行, 给 10 档 (L2) 渲染 10 行;
  // L2 不可用时不会出现空占位的"多出来那 5 档"。
  const levelCount = Math.min(10, Math.max(rawAsks.length, bids.length, 5))
  const isL2 = levelCount > 5
  const askRows = padN(rawAsks, levelCount)
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
    rawAsks.forEach((row, i) => track(`ask${i}`, row))
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
  const ask1 = rawAsks[0] ?? null
  const bid1 = bids[0] ?? null
  const sealedSide: 'up' | 'down' | null =
    !ask1 && bid1 ? 'up' : !bid1 && ask1 ? 'down' : null
  const allEmpty = !rawAsks.length && !bids.length

  const renderRow = (key: string, label: string, row: LevelRow, side: 'ask' | 'bid') => {
    const color = side === 'ask' ? 'text-bear' : 'text-bull'
    const flash = flashKeys.has(key)
    return (
      <div
        key={key}
        className={`relative flex items-center px-1.5 font-mono text-[11px] transition-colors duration-300 ${
          isL2 ? 'py-[1.5px]' : 'py-[3px]'
        } ${flash ? 'bg-accent/20' : ''}`}
      >
        <span className="w-6 shrink-0 text-muted">{label}</span>
        {row ? (
          <>
            <span className={`flex-1 text-right font-semibold ${row.volume === 0 ? 'text-muted' : color}`}>
              {row.price.toFixed(2)}
            </span>
            <span
              className={`w-10 shrink-0 text-right text-secondary ${flash ? 'font-bold text-foreground' : ''}`}
              title={`${row.volume} 手 = ${(row.volume * 100).toLocaleString()} 股 · 委托金额 ${fmtAmt(row.price, row.volume)}`}
            >
              {fmtVol(row.volume)}
            </span>
          </>
        ) : (
          <>
            <span className="flex-1 text-right text-muted/40">—</span>
            <span className="w-10 shrink-0 text-right text-muted/40">—</span>
          </>
        )}
      </div>
    )
  }

  const renderBlock = (rows: LevelRow[], side: 'ask' | 'bid') => (
    <div className="min-w-0 flex-1">
      <div className="flex items-center px-1.5 pb-0.5 pt-1 text-[9px] text-muted/60">
        <span className="w-6 shrink-0">档位</span>
        <span className="flex-1 text-right">价格</span>
        <span className="w-10 shrink-0 text-right">量</span>
      </div>
      {rows.map((row, i) =>
        renderRow(`${side}${i}`, `${side === 'ask' ? '卖' : '买'}${i + 1}`, row, side),
      )}
    </div>
  )

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

      {/* 封板徽标条 (仅封板态显示): 对齐通达信封单语义 */}
      {sealedSide === 'up' && bid1 && (
        <div className="flex shrink-0 items-center justify-between border-b border-border/40 bg-bull/[0.06] px-2 py-0.5">
          <span className="text-[10px] font-semibold text-bull">涨停封板</span>
          <span className="font-mono text-[10px] font-semibold text-bull">
            封单 {fmtVol(bid1.volume)}手 · {fmtAmt(bid1.price, bid1.volume)}
          </span>
        </div>
      )}
      {sealedSide === 'down' && ask1 && (
        <div className="flex shrink-0 items-center justify-between border-b border-border/40 bg-bear/[0.06] px-2 py-0.5">
          <span className="text-[10px] font-semibold text-bear">跌停封板</span>
          <span className="font-mono text-[10px] font-semibold text-bear">
            封单 {fmtVol(ask1.volume)}手 · {fmtAmt(ask1.price, ask1.volume)}
          </span>
        </div>
      )}

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
          {/* 通达信横排: 买1..买N (左, 红) | 卖1..卖N (右, 绿); L2 十档时行数多可滚动 */}
          <div className="flex min-h-0 flex-1 divide-x divide-border/40 overflow-y-auto">
            {renderBlock(bidRows, 'bid')}
            {renderBlock(askRows, 'ask')}
          </div>
        </>
      )}
    </div>
  )
}
