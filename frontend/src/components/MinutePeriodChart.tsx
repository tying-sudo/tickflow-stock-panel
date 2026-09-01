import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '@/lib/api'

/**
 * 分钟周期 K 线 (个股预览弹窗中列分时区的周期切换档):
 * 数据 = 指定日期 (或最新交易日) 的 1 分钟线 (api.klineMinute, live);
 * N 分钟在前端聚合 (按 A 股交易分钟窗对齐: 09:30 起 N 根 1 分钟合一根,
 * OHLC 取首高低末), 保证切换周期后走势图每一根 bar 都是窗口真实计算。
 * 与 StockIntradayChart 同区渲染 (右上角标签切换), 无独立卡片外壳。
 */
export type MinutePeriod = 1 | 5 | 15 | 30 | 60

const BULL = '#C74040' // 涨-红 (A 股惯例)
const BEAR = '#2D9B65' // 跌-绿

interface AggBar {
  time: string // 窗口结束时间 HH:mm
  open: number
  high: number
  low: number
  close: number
}

function minuteOfDay(dt: string): number {
  // datetime 'YYYY-MM-DD HH:mm(:ss)' → 当日分钟数 (00:00 起)
  const m = dt.match(/(\d{2}):(\d{2}):?(\d{2})?/)
  return m ? Number(m[1]) * 60 + Number(m[2]) : 0
}

/** 1 分钟序列 → N 分钟 bar (按交易分钟窗对齐, A 股 09:30-11:30/13:00-15:00) */
export function aggregateMinutes(rows: { datetime: string; open: number | null; high: number; low: number; close: number }[], n: number): AggBar[] {
  if (n <= 1) {
    return rows.map(r => ({
      time: r.datetime.slice(11, 16),
      open: r.open ?? r.close,
      high: r.high,
      low: r.low,
      close: r.close,
    }))
  }
  // 交易分钟序号: 将 11:30-13:00 的午休跳过 → 连续索引, 保证每窗恰好 N 根
  const toSeq = (dt: string): number => {
    const m = minuteOfDay(dt)
    const morning = Math.max(0, Math.min(m, 690) - 570) // 09:30-11:30 → 0..120
    const afternoon = m >= 780 ? Math.min(m, 900) - 780 + 120 : m < 570 ? 0 : 120 // 13:00-15:00 → 121..240
    return m < 780 ? morning : afternoon
  }
  const groups = new Map<number, { datetime: string; open: number | null; high: number; low: number; close: number }[]>()
  for (const r of rows) {
    const seq = toSeq(r.datetime)
    const win = Math.floor(Math.max(0, seq - 1) / n) // 第 1 根(09:31)归窗口 0
    if (!groups.has(win)) groups.set(win, [])
    groups.get(win)!.push(r)
  }
  const bars: AggBar[] = []
  for (const [, rs] of [...groups.entries()].sort((a, b) => a[0] - b[0])) {
    if (!rs.length) continue
    const opens = rs.map(r => r.open).filter((v): v is number => v != null)
    bars.push({
      time: rs[rs.length - 1].datetime.slice(11, 16),
      open: opens[0] ?? rs[0].close,
      high: Math.max(...rs.map(r => r.high)),
      low: Math.min(...rs.map(r => r.low)),
      close: rs[rs.length - 1].close,
    })
  }
  return bars
}

export function MinutePeriodChart({
  symbol,
  date = null,
  period,
  height,
  refetchIntervalMs,
}: {
  symbol: string | null
  /** 交易日 (null = 最新交易日); 跟随中列分时图选中日 */
  date?: string | null
  period: MinutePeriod
  height: number
  refetchIntervalMs?: number
}) {
  const { data } = useQuery({
    queryKey: ['minute-period', symbol, date, period],
    queryFn: () => api.klineMinute(symbol!, date ?? undefined, true),
    enabled: !!symbol,
    staleTime: 15_000,
    refetchInterval: refetchIntervalMs ?? false,
  })

  const bars = useMemo(() => {
    const rows = data?.rows ?? []
    return aggregateMinutes(rows, period)
  }, [data, period])

  const W = 420
  const H = height
  const padR = 40 // 右侧最新价签
  const padB = 16 // 底部时间刻度
  const padT = 6
  const innerW = W - padR - 2
  const innerH = H - padT - padB

  const content = (() => {
    if (!bars.length) {
      return <div className="grid h-full place-items-center text-[10px] text-muted">该日无分钟数据</div>
    }
    let hi = -Infinity
    let lo = Infinity
    for (const b of bars) {
      if (b.high > hi) hi = b.high
      if (b.low < lo) lo = b.low
    }
    const range = hi - lo || 1
    const pad = range * 0.06
    const top = hi + pad
    const bot = lo - pad
    const y = (v: number) => padT + (1 - (v - bot) / (top - bot)) * innerH
    const n = bars.length
    const barW = innerW / n
    const bodyW = Math.max(barW * 0.6, 1.5)

    const last = bars[n - 1]
    const up = last.close >= bars[0].open
    const gridLines = 4
    const ticks: React.ReactNode[] = []
    for (let i = 0; i <= gridLines; i++) {
      const v = bot + ((top - bot) * i) / gridLines
      const yy = y(v)
      ticks.push(
        <g key={i}>
          <line x1={0} x2={innerW} y1={yy} y2={yy} stroke="currentColor" className="text-border" strokeWidth={0.5} opacity={0.5} />
          <text x={innerW + 4} y={yy + 3} fontSize={8.5} fill="currentColor" className="text-muted">{v.toFixed(2)}</text>
        </g>,
      )
    }
    // 时间刻度: 取 4 个均匀点
    const timeTicks = [0, Math.floor(n / 3), Math.floor((2 * n) / 3), n - 1].filter((v, i, a) => a.indexOf(v) === i)

    return (
      <svg width="100%" viewBox={`0 0 ${W} ${H}`} className="block" preserveAspectRatio="none" role="img" aria-label={`${period}分钟K线`}>
        {ticks}
        {bars.map((b, i) => {
          const x = i * barW + barW / 2
          const color = b.close >= b.open ? BULL : BEAR
          const yO = y(b.open)
          const yC = y(b.close)
          const rectY = Math.min(yO, yC)
          const rectH = Math.max(Math.abs(yC - yO), 0.8)
          return (
            <g key={i}>
              <line x1={x} x2={x} y1={y(b.high)} y2={y(b.low)} stroke={color} strokeWidth={0.8} />
              <rect x={x - bodyW / 2} y={rectY} width={bodyW} height={rectH} fill={b.close >= b.open ? 'none' : color} stroke={color} strokeWidth={0.8} />
            </g>
          )
        })}
        {/* 最新价虚线 + 价签 */}
        <line x1={0} x2={innerW} y1={y(last.close)} y2={y(last.close)} stroke={up ? BULL : BEAR} strokeWidth={0.6} strokeDasharray="3 2" opacity={0.7} />
        <rect x={innerW + 1} y={Math.max(2, Math.min(y(last.close) - 6.5, innerH - 13))} width={padR - 2} height={13} rx={2} fill={up ? BULL : BEAR} />
        <text x={innerW + 3} y={Math.max(11.5, Math.min(y(last.close) + 3, innerH - 0.5))} fontSize={8.5} fill="#fff">{last.close.toFixed(2)}</text>
        {timeTicks.map((i, k) => (
          <text key={k} x={Math.min(Math.max(i * barW + barW / 2, 14), innerW - 14)} y={H - 4} fontSize={8.5} fill="currentColor" className="text-muted" textAnchor="middle">
            {bars[i].time}
          </text>
        ))}
      </svg>
    )
  })()

  return (
    <div className="h-full" style={{ height: H }}>
      {content}
    </div>
  )
}
