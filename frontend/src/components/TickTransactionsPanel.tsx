import { memo, useCallback, useEffect, useRef, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api, type TickTrade } from '@/lib/api'

interface Props {
  symbol: string
  /** 面板高度(px)。 */
  height?: number
  /** 轮询间隔(ms)。undefined = 不轮询 (仅拉一次)。 */
  refetchIntervalMs?: number
  className?: string
}

const DIRECTION_META: Record<string, { label: string; cls: string }> = {
  buy: { label: '买盘', cls: 'text-bull' },
  sell: { label: '卖盘', cls: 'text-bear' },
  neutral: { label: '中性盘', cls: 'text-secondary' },
  auction: { label: '竞价', cls: 'text-accent' },
  after_hours: { label: '盘后', cls: 'text-muted' },
  other: { label: '—', cls: 'text-muted/50' },
}

function fmtVol(v: number): string {
  if (v >= 10000) return `${(v / 10000).toFixed(1)}万`
  return v.toLocaleString()
}

/**
 * 单笔行 (memo 化): 值不变的行直接跳过重渲染。
 * 行 key 用内容指纹 (时间+价+量+方向+序号) — 追加式更新下旧行 key 恒定,
 * 不会因整列表重挂导致滚动跳动。
 */
const TickRow = memo(
  function TickRow({ time, price, volume, num, direction }: TickTrade) {
    const meta = DIRECTION_META[direction] ?? DIRECTION_META.other
    return (
      <div className="flex items-center px-2 py-[2.5px] font-mono text-[11px] hover:bg-elevated/60">
        <span className="w-9 shrink-0 text-muted">{time}</span>
        <span className={`flex-1 text-right font-semibold ${meta.cls}`}>
          {price.toFixed(2)}
        </span>
        <span
          className="w-9 shrink-0 text-right text-secondary"
          title={`${volume} 手 = ${(volume * 100).toLocaleString()} 股 (A股 1手=100股)`}
        >
          {fmtVol(volume)}
        </span>
        <span className="w-7 shrink-0 text-right text-secondary">{num > 0 ? num : '—'}</span>
        <span className={`w-10 shrink-0 text-right ${meta.cls}`}>{meta.label}</span>
      </div>
    )
  },
  (a, b) =>
    a.time === b.time && a.price === b.price && a.volume === b.volume &&
    a.num === b.num && a.direction === b.direction,
)

/**
 * 分时成交 (分笔明细) — 通达信口径: 时间 / 价格 / 成交量(手) / 笔数 / 方向。
 *
 * 流式增量刷新 (2026-09-05 消抖改造):
 * - 首拉全量 (含 minute_volumes/一致性核对), 之后 0.5s 轮询走 /transactions?tail=N
 *   尾部增量 — 无新成交时响应近空且**不动 React 状态** → 列表不重渲染、
 *   滚动位置不重置 (旧实现每 500ms 整包替换 data + 强制 scrollTop 吸底 = 抖动)。
 * - 只有真的追加了新笔才 setState; 服务端 full=true (跨日/重建) 时整表重置。
 * - 列表时间升序、自动钉在最新一笔 (用户上滚查看历史时不打断, 滚回底部恢复跟随)。
 */
export function TickTransactionsPanel({ symbol, height = 420, refetchIntervalMs, className }: Props) {
  // 首拉 (symbol 变化时重置)
  const tx = useQuery({
    queryKey: ['transactions', symbol],
    queryFn: () => api.transactions(symbol),
    enabled: !!symbol,
    retry: false,
    staleTime: Infinity, // 后续刷新全靠 tail 轮询, 不再整包 refetch
  })

  const [ticks, setTicks] = useState<TickTrade[]>([])
  const [day, setDay] = useState<string | null>(null)
  const [source, setSource] = useState<string | null>(null)
  const [updatedAt, setUpdatedAt] = useState<number | null>(null)

  // 首拉/换 symbol → 整表替换
  useEffect(() => {
    const data = tx.data
    if (!data) return
    setTicks(data.ticks ?? [])
    setDay(data.date ?? null)
    setSource(data.source ?? null)
    setUpdatedAt(Date.now())
  }, [tx.data, symbol])

  // 尾部增量轮询: refs 拿最新值, interval 只建一次 (symbol/开关变化时重建)
  const ticksRef = useRef(ticks)
  ticksRef.current = ticks
  const dayRef = useRef(day)
  dayRef.current = day

  const applyTail = useCallback((r: Awaited<ReturnType<typeof api.transactionsTail>>) => {
    if (r.full) {
      setTicks(r.appended)
      setDay(r.date)
      setSource(r.source)
    } else if (r.appended.length > 0) {
      setTicks(prev => (prev.length === r.tick_count - r.appended.length ? [...prev, ...r.appended] : prev))
      setDay(r.date)
      setSource(r.source)
    }
    setUpdatedAt(Date.now())
  }, [])

  useEffect(() => {
    if (!refetchIntervalMs || !symbol || !tx.isSuccess) return
    let stopped = false
    const id = window.setInterval(async () => {
      if (stopped) return
      try {
        const r = await api.transactionsTail(symbol, dayRef.current, ticksRef.current.length)
        if (!stopped) applyTail(r)
      } catch {
        /* 静默: 下一轮重试 */
      }
    }, refetchIntervalMs)
    return () => { stopped = true; window.clearInterval(id) }
  }, [refetchIntervalMs, symbol, tx.isSuccess, applyTail])

  // 自动滚动跟随最新: 仅当用户停在底部附近时才吸附 (且只在追加后触发)
  const scrollRef = useRef<HTMLDivElement>(null)
  const pinnedRef = useRef(true)
  const prevLenRef = useRef(0)
  useEffect(() => {
    if (ticks.length === prevLenRef.current) return // 无新增不动 DOM
    prevLenRef.current = ticks.length
    const el = scrollRef.current
    if (el && pinnedRef.current) el.scrollTop = el.scrollHeight
  }, [ticks])

  return (
    <div
      className={`flex flex-col rounded-card border border-border bg-surface/60 overflow-hidden ${className ?? ''}`}
      style={{ height }}
      data-testid="tick-transactions-panel"
    >
      {/* 标题行: 名称 + 数据日期/来源 */}
      <div className="flex shrink-0 items-center justify-between border-b border-border/60 px-2 py-1">
        <span className="flex items-center gap-1.5 text-[11px] font-semibold text-foreground">
          {refetchIntervalMs != null && ticks.length > 0 && (
            <span className="relative flex h-1.5 w-1.5">
              <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-bull opacity-60" />
              <span className="relative inline-flex h-1.5 w-1.5 rounded-full bg-bull" />
            </span>
          )}
          分时成交
        </span>
        <span className="flex items-center gap-1.5">
          {source === 'local_archive' && (
            <span className="rounded bg-elevated px-1 py-0.5 text-[9px] text-muted">归档</span>
          )}
          {day && (
            <span className="font-mono text-[9px] text-muted">{day}</span>
          )}
        </span>
      </div>

      {tx.isError ? (
        <div className="flex flex-1 items-center justify-center px-3 text-center">
          <span className="text-[11px] leading-relaxed text-muted">
            {tx.error instanceof Error && tx.error.message
              ? tx.error.message
              : '分笔数据不可用'}
          </span>
        </div>
      ) : tx.isLoading ? (
        <div className="flex flex-1 items-center justify-center text-xs text-muted">加载中…</div>
      ) : (
        <>
          {/* 列头 */}
          <div className="flex shrink-0 items-center border-b border-border/40 px-2 py-0.5 text-[9px] text-muted/60">
            <span className="w-9 shrink-0">时间</span>
            <span className="flex-1 text-right">价格</span>
            <span className="w-9 shrink-0 text-right">成交量</span>
            <span className="w-7 shrink-0 text-right">笔数</span>
            <span className="w-10 shrink-0 text-right">方向</span>
          </div>
          {/* 明细列表: 时间升序, 跟随最新 */}
          <div
            ref={scrollRef}
            onScroll={(e) => {
              const el = e.currentTarget
              pinnedRef.current = el.scrollHeight - el.scrollTop - el.clientHeight < 48
            }}
            className="min-h-0 flex-1 overflow-y-auto"
          >
            {ticks.length === 0 && (
              <div className="px-2 py-4 text-center text-[10px] text-muted">当日暂无分笔记录</div>
            )}
            {ticks.map((t, i) => (
              <TickRow key={`${t.time}|${t.price}|${t.volume}|${t.direction}|${i}`} {...t} />
            ))}
          </div>
          {/* 底栏: 更新时间 + 总笔数 */}
          <div className="flex shrink-0 items-center justify-between border-t border-border/60 px-2 py-1">
            <span className="text-[10px] text-muted">更新 {updatedAt ? new Date(updatedAt).toLocaleTimeString('zh-CN', { hour12: false }) : '—'}</span>
            <span className="font-mono text-[10px] text-muted">{ticks.length}笔</span>
          </div>
        </>
      )}
    </div>
  )
}
