import { useEffect, useState, useCallback, useRef, useMemo } from 'react'
import { X } from 'lucide-react'
import { type KlineRow, type FinancialMetricRecord } from '@/lib/api'
import { StockInfoBar } from '@/components/StockInfoBar'
import { StockDailyKChart, getDefaultRange, type StockDailyKChartResult } from '@/components/StockDailyKChart'
import { StockIntradayChart } from '@/components/StockIntradayChart'
import { MinutePeriodChart, type MinutePeriod } from '@/components/MinutePeriodChart'
import { useFinancialMetrics } from '@/lib/useFinancials'
import { useCapabilities } from '@/lib/useSharedQueries'
import type { ChartMarker, ChartPriceLine, ChartRange } from '@/components/EChartsCandlestick'
import {
  loadInfoFields,
  saveInfoFields,
  buildInfoExtColumnsParam,
  type ColumnConfig,
} from '@/lib/stock-info-fields'

interface Props {
  symbol: string
  height?: number
  showIntraday?: boolean
  className?: string
  /** 当用户点击蜡烛选中日期时回调（用于外部自动开启分时图）。 */
  onSelectDate?: (date: string) => void
  /** 外部传入的日期范围 */
  dateRange?: { start: string; end: string }
  markers?: ChartMarker[]
  ranges?: ChartRange[]
  priceLines?: ChartPriceLine[]
  showLimitMarkers?: boolean
  showMarkerToggle?: boolean
  /** 加监控回调 (传入后信息条显示 RadioTower 图标) */
  onMonitor?: () => void
  onPriceDoubleClick?: (price: number, currentPrice: number) => void
  /** 自选操作（传入后信息条显示 Star 图标） */
  inWatchlist?: boolean
  onAddToWatchlist?: (groupId: string | null) => void
  onRemoveFromWatchlist?: () => void
  watchlistPending?: boolean
  /** 分时图自动刷新间隔(ms)。undefined = 不轮询。个股对话框盘中实时刷新时传入。 */
  refetchIntervalMs?: number
  /** 只渲染信息条, 隐藏图表 (用于分时 tab 共享信息条) */
  infoBarOnly?: boolean
  /** 第三列面板 (如分时成交), 渲染在分时图右侧; 传入后布局变为 [日K | 分时 | 面板] */
  rightPanel?: React.ReactNode
  /** 分时图下方插槽 (如五档盘口横排); height 为该插槽固定像素高度, 分时图自动让位 */
  intradayBottom?: { node: React.ReactNode; height: number }
  /** 分时区右上角周期标签 [分时|1分|5分|15分|30分|60分] (用户指定: 在分时走势图内切换周期, 不新增独立图) */
  periodTabs?: boolean
}

const MINUTE_PERIOD_TABS: { key: 'intraday' | MinutePeriod; label: string }[] = [
  { key: 'intraday', label: '分时' },
  { key: 1, label: '1分' },
  { key: 5, label: '5分' },
  { key: 15, label: '15分' },
  { key: 30, label: '30分' },
  { key: 60, label: '60分' },
]

export { getDefaultRange }

export function StockPanel({
  symbol,
  height = 520,
  showIntraday = true,
  className,
  onSelectDate,
  dateRange: externalDateRange,
  markers,
  ranges,
  priceLines,
  showLimitMarkers = true,
  showMarkerToggle = true,
  onMonitor,
  onPriceDoubleClick,
  inWatchlist,
  onAddToWatchlist,
  onRemoveFromWatchlist,
  watchlistPending,
  refetchIntervalMs,
  infoBarOnly = false,
  rightPanel,
  intradayBottom,
  periodTabs = false,
}: Props) {
  const [linkedPrice, setLinkedPrice] = useState<number | null>(null)
  const [selectedDate, setSelectedDate] = useState<string | null>(null)
  const [intradayDismissed, setIntradayDismissed] = useState(false)
  // 中列分时区周期: 'intraday'=分时线, N=该日 N 分钟K (同一区域内切换, 不新增独立图)
  const [minutePeriod, setMinutePeriod] = useState<'intraday' | MinutePeriod>('intraday')
  const [dailyResult, setDailyResult] = useState<StockDailyKChartResult | null>(null)
  // 信息条指标配置提升到此层：同时供 StockInfoBar 渲染与 StockDailyKChart 请求 ext 数据
  const [fields, setFields] = useState<ColumnConfig[]>(loadInfoFields)
  const extColumns = useMemo(() => buildInfoExtColumnsParam(fields), [fields])

  const handleFieldsChange = useCallback((next: ColumnConfig[]) => {
    setFields(next)
    saveInfoFields(next)
  }, [])

  // 财务指标：仅当信息条配置含可见的财务字段且用户具备财务数据能力 (financial) 时才请求
  // 无能力时跳过请求, 避免后端抛 CapabilityDenied (403) 导致 free/starter 档弹错误提示
  const { data: caps } = useCapabilities()
  const hasFinancialCap = !!caps?.capabilities?.['financial']
  const hasFinanceField = useMemo(
    () => fields.some(f => f.visible && f.source.type === 'builtin'
      && ['eps', 'bps', 'roe', 'pe_ttm', 'pb', 'gross_margin', 'net_margin', 'debt_ratio', 'revenue_yoy', 'net_income_yoy'].includes(f.source.key)),
    [fields],
  )
  const financials = useFinancialMetrics(hasFinanceField && hasFinancialCap ? symbol : undefined)

  const dateRange = externalDateRange ?? getDefaultRange()

  const handleDateClick = useCallback((date: string) => {
    setSelectedDate(date)
    setIntradayDismissed(false)
    onSelectDate?.(date)
  }, [onSelectDate])

  const rows = dailyResult?.rows ?? []
  const stockInfo = dailyResult?.stockInfo
  const rawRows: KlineRow[] = dailyResult?.rawRows ?? []

  // symbol 变化时重置分时相关状态，避免切股后残留旧日期。
  // 注意：必须跳过首次挂载——重开弹窗时 kline 命中 react-query 缓存，
  // 子组件 onDataChange effect（先于父 effect 执行）会把 dailyResult 置为有效数据，
  // 若此处再无条件清空，会把刚加载的数据抹掉，导致信息条整行消失。
  const prevSymbol = useRef<string | null>(symbol)
  useEffect(() => {
    if (prevSymbol.current === symbol) return
    prevSymbol.current = symbol
    setSelectedDate(null)
    setLinkedPrice(null)
    setDailyResult(null)
  }, [symbol])

  // 当分时开启、无选中日期时，自动选中最新日期
  useEffect(() => {
    if (showIntraday && !selectedDate && rows.length > 0) {
      setSelectedDate(rows[rows.length - 1].date)
    }
  }, [showIntraday, selectedDate, rows])

  const selectedIdx = selectedDate ? rows.findIndex(r => r.date === selectedDate) : -1
  const prevClose = selectedIdx > 0
    ? rows[selectedIdx - 1].close
    : rows.length >= 2
      ? rows[rows.length - 2].close
      : undefined
  if (!symbol) return null

  // 财务指标最新一期（metrics 按 period_end 排序，取首项）
  const financialMetrics: FinancialMetricRecord | undefined = financials.data?.data?.[0]

  return (
    <div className={className}>
      <StockInfoBar
        symbol={symbol}
        name={dailyResult?.name}
        stockInfo={stockInfo}
        rows={rawRows}
        fields={fields}
        onFieldsChange={handleFieldsChange}
        financialMetrics={financialMetrics}
        onMonitor={onMonitor}
        inWatchlist={inWatchlist}
        onAddToWatchlist={onAddToWatchlist}
        onRemoveFromWatchlist={onRemoveFromWatchlist}
        watchlistPending={watchlistPending}
      />

      {infoBarOnly ? null : (
      /* 移动端单列堆叠 (max-sm:grid-cols-1); 桌面列结构与原版一致 */
      <div className={`grid gap-3 items-start max-sm:grid-cols-1 ${rightPanel ? 'sm:grid-cols-[1fr_1fr_16rem]' : 'sm:grid-cols-2'}`}>
        <StockDailyKChart
          symbol={symbol}
          height={height}
          className="min-w-0"
          dateRange={dateRange}
          markers={markers}
          ranges={ranges}
          priceLines={priceLines}
          showLimitMarkers={showLimitMarkers}
          showMarkerToggle={showMarkerToggle}
          linkedPrice={linkedPrice}
          onDateClick={handleDateClick}
          onPriceDoubleClick={onPriceDoubleClick}
          onDataChange={setDailyResult}
          visibleBars={showIntraday ? 40 : 60}
          extColumns={extColumns}
          refetchIntervalMs={refetchIntervalMs}
        />

        {showIntraday && selectedDate && !intradayDismissed ? (
          <div className="relative flex min-w-0 flex-col border-l border-border pl-3 max-sm:border-l-0 max-sm:pl-0">
            <button
              onClick={() => setIntradayDismissed(true)}
              className="absolute -left-1.5 -top-1.5 z-10 flex h-5 w-5 items-center justify-center rounded-full border border-border bg-surface text-muted shadow-sm transition-colors hover:text-foreground hover:bg-elevated"
              title="收起分时图"
              aria-label="收起分时图"
            >
              <X className="h-3 w-3" />
            </button>
            {/* 周期标签栏 (独立占位行, 不悬浮 — 悬浮会压住分时图 OHLC 信息条): 分时线 / 该日 N 分钟K */}
            {periodTabs && (
              <div className="mb-1 flex shrink-0 items-center justify-end gap-0.5">
                {MINUTE_PERIOD_TABS.map(p => (
                  <button
                    key={String(p.key)}
                    type="button"
                    onClick={() => setMinutePeriod(p.key)}
                    className={`rounded px-1.5 py-0.5 text-[10px] transition-colors ${
                      minutePeriod === p.key
                        ? 'bg-accent/15 font-semibold text-accent'
                        : 'text-muted hover:bg-elevated hover:text-secondary'
                    }`}
                    title={p.key === 'intraday' ? '当日分时走势' : `${p.label}钟K线 (1分钟聚合, 同一选中日)`}
                  >
                    {p.label}
                  </button>
                ))}
              </div>
            )}
            {minutePeriod === 'intraday' ? (
              <StockIntradayChart
                symbol={symbol}
                date={selectedDate}
                height={intradayBottom
                  ? Math.max(200, height - intradayBottom.height - 12 - (periodTabs ? 24 : 0))
                  : Math.max(200, height - (periodTabs ? 24 : 0))}
                prevClose={prevClose}
                onPriceHover={setLinkedPrice}
                onPriceDoubleClick={onPriceDoubleClick}
                currentPrice={rows[rows.length - 1]?.close}
                priceLines={priceLines}
                refetchIntervalMs={refetchIntervalMs}
              />
            ) : (
              <MinutePeriodChart
                symbol={symbol}
                date={selectedDate}
                period={minutePeriod as MinutePeriod}
                height={intradayBottom
                  ? Math.max(200, height - intradayBottom.height - 12 - (periodTabs ? 24 : 0))
                  : Math.max(200, height - (periodTabs ? 24 : 0))}
                refetchIntervalMs={refetchIntervalMs}
              />
            )}
            {intradayBottom && (
              <div className="mt-3 shrink-0" style={{ height: intradayBottom.height }}>
                {intradayBottom.node}
              </div>
            )}
          </div>
        ) : (
          <div className="min-w-0" />
        )}
        {rightPanel && <div className="min-w-0">{rightPanel}</div>}
      </div>
      )}
    </div>
  )
}
