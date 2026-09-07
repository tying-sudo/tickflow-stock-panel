import { useMemo, useState } from 'react'
import {
  keepPreviousData,
  useInfiniteQuery,
  useMutation,
  useQuery,
  useQueryClient,
} from '@tanstack/react-query'
import {
  ChevronDown,
  ChevronUp,
  ExternalLink,
  Inbox,
  Newspaper,
  RefreshCw,
  Search,
  Settings2,
} from 'lucide-react'
import type { FrontendExtension } from '@/extensions/types'
import { PageHeader } from '@/components/PageHeader'
import { EmptyState } from '@/components/EmptyState'
import { Modal } from '@/components/Modal'
import { DatePicker } from '@/components/DatePicker'
import { api } from '@/lib/api'
import { cn } from '@/lib/cn'

/**
 * 最新资讯二开扩展 — 多源财经快讯聚合页。
 * 后端契约见 backend/app/custom/news_feed.py (/api/custom/news/*)。
 * 按 docs/secondary-development.md 通过路由 + 导航注册接入, 不改核心文件。
 */

// ---------------------------------------------------------------------------
// 类型与请求
// ---------------------------------------------------------------------------

interface NewsStock {
  code: string
  name: string
}

interface NewsItem {
  id: string
  source: string
  source_label: string
  title: string
  content: string
  url: string | null
  published_at: string
  sentiment: 'positive' | 'negative' | 'neutral' | 'none'
  important: boolean
  tags: string[]
  stocks: NewsStock[]
  analyzed: boolean
}

interface NewsItemsResponse {
  total: number
  positive: number
  negative: number
  page: number
  page_size: number
  items: NewsItem[]
}

interface NewsStatus {
  last_fetch_at: string | null
  next_run_at: string | null
  interval_minutes: number
  sources_enabled: Record<string, boolean>
  sources_status: Record<string, { ok: boolean; count: number; error: string | null; at: string }>
  focus_concepts: string[]
  push: { enabled: boolean; channels: string[]; configured: boolean }
  ai_configured: boolean
}

interface NewsSettings {
  interval_minutes: number
  sources: Record<string, boolean>
  push_enabled: boolean
  push_channels: string[]
  focus_concepts: string[]
  retention_days: number
}

interface NewsCycleResult {
  fetched_at: string
  new_items: number
  analyzed: number
  push_sent: number
}

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    ...init,
    headers: init?.body ? { 'Content-Type': 'application/json' } : undefined,
  })
  let data: unknown = null
  try {
    data = await res.json()
  } catch {
    // 非 JSON 响应按空处理, 走状态码分支
  }
  if (!res.ok) {
    const payload = data as { detail?: unknown; message?: unknown } | null
    const detail = payload?.detail ?? payload?.message
    throw new Error(
      typeof detail === 'string' ? detail : `${res.status} ${res.statusText}`,
    )
  }
  return data as T
}

const newsApi = {
  items: (params: Record<string, string | number | undefined>) => {
    const qs = new URLSearchParams()
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined && value !== '') qs.set(key, String(value))
    }
    return req<NewsItemsResponse>(`/api/custom/news/items?${qs.toString()}`)
  },
  fetchNow: () => req<NewsCycleResult>('/api/custom/news/fetch', { method: 'POST' }),
  settings: () => req<NewsSettings>('/api/custom/news/settings'),
  saveSettings: (settings: NewsSettings) =>
    req<NewsSettings>('/api/custom/news/settings', {
      method: 'PUT',
      body: JSON.stringify(settings),
    }),
  status: () => req<NewsStatus>('/api/custom/news/status'),
}

const extNewsKeys = {
  items: (params: Record<string, string>) => ['ext-news', 'items', params] as const,
  status: ['ext-news', 'status'] as const,
  settings: ['ext-news', 'settings'] as const,
  watchlist: ['ext-news', 'watchlist-symbols'] as const,
}

// ---------------------------------------------------------------------------
// 展示常量
// ---------------------------------------------------------------------------

const SOURCE_LABELS: Record<string, string> = {
  cls: '财联社',
  eastmoney: '东方财富',
  em_news: '东财要闻',
  exchange: '交易所公告',
  sina: '新浪财经',
  jin10: '金十数据',
  ths: '同花顺',
}

const SOURCE_BADGE: Record<string, string> = {
  cls: 'border-sky-400/30 bg-sky-400/10 text-sky-300',
  eastmoney: 'border-orange-400/30 bg-orange-400/10 text-orange-300',
  em_news: 'border-amber-400/30 bg-amber-400/10 text-amber-300',
  exchange: 'border-violet-400/30 bg-violet-400/10 text-violet-300',
  sina: 'border-rose-400/30 bg-rose-400/10 text-rose-300',
  jin10: 'border-yellow-400/30 bg-yellow-400/10 text-yellow-300',
  ths: 'border-cyan-400/30 bg-cyan-400/10 text-cyan-300',
}

const PUSH_CHANNEL_LABELS: Record<string, string> = {
  feishu: '飞书',
  wecom: '企业微信',
}

type RangeKey = 'today' | '3d' | '1w' | '1m' | 'custom'
type TypeKey = 'all' | 'positive' | 'negative' | 'important' | 'watchlist'

// ---------------------------------------------------------------------------
// 通用小组件
// ---------------------------------------------------------------------------

function ChipGroup<T extends string>({
  label,
  value,
  options,
  onChange,
}: {
  label: string
  value: T
  options: { key: T; label: string }[]
  onChange: (key: T) => void
}) {
  return (
    <div className="flex items-start gap-3">
      <span className="w-8 shrink-0 pt-1.5 text-xs text-muted">{label}</span>
      <div className="flex flex-wrap gap-1.5">
        {options.map(option => (
          <button
            key={option.key}
            type="button"
            onClick={() => onChange(option.key)}
            className={cn(
              'h-7 rounded-btn border px-3 text-xs transition-colors duration-150',
              value === option.key
                ? 'border-accent/60 bg-accent/10 text-accent'
                : 'border-border bg-elevated text-secondary hover:text-foreground',
            )}
          >
            {option.label}
          </button>
        ))}
      </div>
    </div>
  )
}

function SearchInput({
  value,
  onChange,
  placeholder,
}: {
  value: string
  onChange: (v: string) => void
  placeholder: string
}) {
  return (
    <input
      value={value}
      onChange={event => onChange(event.target.value)}
      onKeyDown={event => {
        if (event.key === 'Enter') (event.target as HTMLInputElement).form?.requestSubmit()
      }}
      placeholder={placeholder}
      className="h-8 min-w-0 flex-1 rounded-btn border border-border bg-elevated px-3 text-xs text-foreground placeholder:text-muted focus:border-accent/50 focus:outline-none"
    />
  )
}

function Switch({
  checked,
  onChange,
  label,
}: {
  checked: boolean
  onChange: (v: boolean) => void
  label: string
}) {
  return (
    <button
      type="button"
      role="switch"
      aria-checked={checked}
      aria-label={label}
      onClick={() => onChange(!checked)}
      className={cn(
        'relative inline-flex h-5 w-9 items-center rounded-full border transition-all duration-200',
        checked ? 'border-accent/50 bg-accent' : 'border-border bg-elevated',
      )}
    >
      <span
        className={cn(
          'inline-block h-3.5 w-3.5 rounded-full bg-white shadow transition-transform duration-200',
          checked ? 'translate-x-[18px]' : 'translate-x-[3px]',
        )}
      />
    </button>
  )
}

// ---------------------------------------------------------------------------
// 资讯卡片
// ---------------------------------------------------------------------------

function NewsCard({ item }: { item: NewsItem }) {
  const [expanded, setExpanded] = useState(false)
  const longContent = item.content.length > 120
  return (
    <article className="rounded-card border border-border bg-surface p-4">
      <div className="flex items-center gap-2">
        <span
          className={cn(
            'rounded border px-1.5 py-0.5 text-[11px] font-medium',
            SOURCE_BADGE[item.source] ?? 'border-border bg-elevated text-secondary',
          )}
        >
          {item.source_label}
        </span>
        {item.important && (
          <span className="rounded border border-amber-400/30 bg-amber-400/10 px-1.5 py-0.5 text-[11px] font-medium text-amber-300">
            重要
          </span>
        )}
        {item.sentiment === 'positive' && (
          <span className="rounded border border-bull/30 bg-bull/10 px-1.5 py-0.5 text-[11px] font-medium text-bull">
            利好
          </span>
        )}
        {item.sentiment === 'negative' && (
          <span className="rounded border border-bear/30 bg-bear/10 px-1.5 py-0.5 text-[11px] font-medium text-bear">
            利空
          </span>
        )}
        <span className="ml-auto flex shrink-0 items-center gap-1.5 text-xs text-muted">
          {item.published_at}
          {item.url && (
            <a
              href={item.url}
              target="_blank"
              rel="noreferrer"
              aria-label="打开原文"
              className="text-muted transition-colors hover:text-accent"
            >
              <ExternalLink size={12} />
            </a>
          )}
        </span>
      </div>
      <h3 className="mt-2 text-sm font-medium leading-snug text-foreground">{item.title}</h3>
      {item.content !== item.title && (
        <p
          className={cn(
            'mt-1.5 whitespace-pre-wrap text-[13px] leading-relaxed text-secondary',
            !expanded && 'line-clamp-3',
          )}
        >
          {item.content}
        </p>
      )}
      {longContent && (
        <button
          type="button"
          onClick={() => setExpanded(value => !value)}
          className="mt-1 inline-flex items-center gap-0.5 text-xs text-accent/90 hover:text-accent"
        >
          {expanded ? (
            <>
              收起 <ChevronUp size={12} />
            </>
          ) : (
            <>
              展开全文 <ChevronDown size={12} />
            </>
          )}
        </button>
      )}
      {(item.tags.length > 0 || item.stocks.length > 0) && (
        <div className="mt-2.5 flex flex-wrap gap-1.5">
          {item.tags.map(tag => (
            <span
              key={`tag-${tag}`}
              className="rounded bg-accent/10 px-2 py-0.5 text-[11px] text-accent/80"
            >
              #{tag}
            </span>
          ))}
          {item.stocks.map(stock => (
            <span
              key={`stock-${stock.code}`}
              className="rounded bg-elevated px-2 py-0.5 text-[11px] text-secondary"
              title={stock.code}
            >
              {stock.name || stock.code} {stock.code}
            </span>
          ))}
        </div>
      )}
    </article>
  )
}

// ---------------------------------------------------------------------------
// 资讯设置对话框
// ---------------------------------------------------------------------------

function SettingsDialog({
  settings,
  status,
  onClose,
}: {
  settings: NewsSettings
  status: NewsStatus | undefined
  onClose: () => void
}) {
  const queryClient = useQueryClient()
  const [draft, setDraft] = useState<NewsSettings>(settings)
  const save = useMutation({
    mutationFn: newsApi.saveSettings,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['ext-news'] })
      onClose()
    },
  })

  return (
    <Modal onClose={onClose} labelledBy="news-settings-title" panelClassName="w-[92vw] max-w-md bg-surface border border-border rounded-card shadow-xl">
      <div className="flex items-center justify-between border-b border-border px-4 py-3">
        <h2 id="news-settings-title" className="text-sm font-semibold">
          资讯设置
        </h2>
        <button type="button" onClick={onClose} className="text-xs text-muted hover:text-foreground">
          关闭
        </button>
      </div>
      <div className="max-h-[70vh] space-y-4 overflow-y-auto px-4 py-3">
        <div className="flex items-center justify-between gap-3">
          <span className="text-xs text-secondary">抓取间隔(分钟, 10-360)</span>
          <input
            type="number"
            min={10}
            max={360}
            value={draft.interval_minutes}
            onChange={event =>
              setDraft(prev => ({ ...prev, interval_minutes: Number(event.target.value) || 30 }))
            }
            className="h-8 w-24 rounded-btn border border-border bg-elevated px-2 text-xs text-foreground focus:border-accent/50 focus:outline-none"
          />
        </div>
        <div className="space-y-1.5">
          <div className="text-xs text-secondary">资讯来源</div>
          <div className="grid grid-cols-2 gap-1.5">
            {Object.entries(draft.sources).map(([name, enabled]) => (
              <label key={name} className="flex cursor-pointer items-center gap-2 text-xs text-foreground">
                <input
                  type="checkbox"
                  checked={enabled}
                  onChange={event =>
                    setDraft(prev => ({
                      ...prev,
                      sources: { ...prev.sources, [name]: event.target.checked },
                    }))
                  }
                  className="accent-[hsl(var(--accent))]"
                />
                {SOURCE_LABELS[name] ?? name}
              </label>
            ))}
          </div>
        </div>
        <div className="flex items-center justify-between gap-3">
          <span className="text-xs text-secondary">消息推送(重要资讯)</span>
          <Switch
            checked={draft.push_enabled}
            onChange={value => setDraft(prev => ({ ...prev, push_enabled: value }))}
            label="消息推送"
          />
        </div>
        {draft.push_enabled && (
          <>
            <div className="space-y-1.5">
              <div className="text-xs text-secondary">推送渠道</div>
              <div className="flex gap-3">
                {Object.entries(PUSH_CHANNEL_LABELS).map(([key, label]) => (
                  <label key={key} className="flex cursor-pointer items-center gap-2 text-xs text-foreground">
                    <input
                      type="checkbox"
                      checked={draft.push_channels.includes(key)}
                      onChange={event =>
                        setDraft(prev => ({
                          ...prev,
                          push_channels: event.target.checked
                            ? [...prev.push_channels, key]
                            : prev.push_channels.filter(channel => channel !== key),
                        }))
                      }
                      className="accent-[hsl(var(--accent))]"
                    />
                    {label}
                  </label>
                ))}
              </div>
              {status && !status.push.configured && (
                <div className="text-[11px] text-warning/90">渠道未配置: 请先在监控中心填写对应 Webhook 地址</div>
              )}
            </div>
            <div className="text-[11px] text-muted">
              推送与利好/利空标注依赖 AI: {status?.ai_configured ? '已配置' : '未配置(设置页 AI 中填写)'}
            </div>
          </>
        )}
        <div className="flex items-center justify-between gap-3">
          <span className="text-xs text-secondary">关注概念(逗号分隔)</span>
          <input
            value={draft.focus_concepts.join(',')}
            onChange={event =>
              setDraft(prev => ({
                ...prev,
                focus_concepts: event.target.value.split(/[,，]/).map(s => s.trim()).filter(Boolean),
              }))
            }
            placeholder="如: 科技,算力"
            className="h-8 w-40 rounded-btn border border-border bg-elevated px-2 text-xs text-foreground placeholder:text-muted focus:border-accent/50 focus:outline-none"
          />
        </div>
        <div className="flex items-center justify-between gap-3">
          <span className="text-xs text-secondary">保留天数(7-365)</span>
          <input
            type="number"
            min={7}
            max={365}
            value={draft.retention_days}
            onChange={event =>
              setDraft(prev => ({ ...prev, retention_days: Number(event.target.value) || 30 }))
            }
            className="h-8 w-24 rounded-btn border border-border bg-elevated px-2 text-xs text-foreground focus:border-accent/50 focus:outline-none"
          />
        </div>
        {save.isError && (
          <div className="text-xs text-bull">保存失败: {(save.error as Error).message}</div>
        )}
      </div>
      <div className="flex justify-end gap-2 border-t border-border px-4 py-3">
        <button
          type="button"
          onClick={onClose}
          className="h-8 rounded-btn border border-border bg-elevated px-3 text-xs text-secondary hover:text-foreground"
        >
          取消
        </button>
        <button
          type="button"
          disabled={save.isPending}
          onClick={() => save.mutate(draft)}
          className="h-8 rounded-btn border border-accent/40 bg-accent/15 px-3 text-xs text-accent hover:bg-accent/25 disabled:opacity-60"
        >
          {save.isPending ? '保存中…' : '保存'}
        </button>
      </div>
    </Modal>
  )
}

// ---------------------------------------------------------------------------
// 主页面
// ---------------------------------------------------------------------------

function NewsPage() {
  const queryClient = useQueryClient()
  const [range, setRange] = useState<RangeKey>('today')
  const [startDate, setStartDate] = useState('')
  const [endDate, setEndDate] = useState('')
  const [itemType, setItemType] = useState<TypeKey>('all')
  const [source, setSource] = useState('all')
  const [keywordDraft, setKeywordDraft] = useState('')
  const [conceptDraft, setConceptDraft] = useState('')
  const [symbolDraft, setSymbolDraft] = useState('')
  const [keyword, setKeyword] = useState('')
  const [concept, setConcept] = useState('')
  const [symbol, setSymbol] = useState('')
  const [settingsOpen, setSettingsOpen] = useState(false)

  const statusQuery = useQuery({
    queryKey: extNewsKeys.status,
    queryFn: newsApi.status,
    refetchInterval: 60_000,
  })
  const settingsQuery = useQuery({
    queryKey: extNewsKeys.settings,
    queryFn: newsApi.settings,
    staleTime: 60_000,
  })

  // 自选股筛选需要标的列表; 仅在该类型下拉取(复用核心自选 API)
  const watchlistQuery = useQuery({
    queryKey: extNewsKeys.watchlist,
    queryFn: () => api.watchlistList(),
    enabled: itemType === 'watchlist',
    staleTime: 60_000,
  })
  const watchlistSymbols = watchlistQuery.data?.symbols.map(entry => entry.symbol) ?? []

  const queryParams = useMemo(() => {
    const params: Record<string, string> = {
      range,
      type: itemType,
      source,
      keyword,
      concept,
      symbol,
    }
    if (range === 'custom' && startDate && endDate) {
      params.start_date = startDate
      params.end_date = endDate
    }
    if (itemType === 'watchlist' && watchlistSymbols.length > 0) {
      params.symbols = watchlistSymbols.join(',')
    }
    return params
  }, [range, itemType, source, keyword, concept, symbol, startDate, endDate, watchlistSymbols.join(',')])

  const itemsQuery = useInfiniteQuery({
    queryKey: extNewsKeys.items(queryParams),
    queryFn: ({ pageParam }) => newsApi.items({ ...queryParams, page: pageParam }),
    initialPageParam: 1,
    getNextPageParam: last => (last.items.length < last.page_size ? undefined : last.page + 1),
    placeholderData: keepPreviousData,
    refetchInterval: 60_000,
  })

  const fetchNow = useMutation({
    mutationFn: newsApi.fetchNow,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['ext-news'] })
    },
  })

  const items = itemsQuery.data?.pages.flatMap(page => page.items) ?? []
  const summary = itemsQuery.data?.pages[0]
  const status = statusQuery.data

  const applySearch = () => {
    setKeyword(keywordDraft.trim())
    setConcept(conceptDraft.trim())
    setSymbol(symbolDraft.trim())
  }

  return (
    <div className="flex h-full flex-col">
      <PageHeader
        title="最新资讯"
        subtitle="财联社 · 东财要闻 · 东方财富 · 同花顺 · 新浪 · 金十 · 交易所公告"
        right={
          <div className="flex items-center gap-2">
            <button
              type="button"
              disabled={fetchNow.isPending}
              onClick={() => fetchNow.mutate()}
              title={status?.last_fetch_at ? `上次抓取 ${status.last_fetch_at}` : '抓取全部启用源'}
              className="inline-flex h-8 items-center gap-1.5 rounded-btn border border-accent/40 bg-accent/15 px-3 text-xs text-accent transition-colors hover:bg-accent/25 disabled:opacity-60"
            >
              <RefreshCw size={13} className={fetchNow.isPending ? 'animate-spin' : ''} />
              {fetchNow.isPending ? '抓取中…' : '立即抓取'}
            </button>
            <button
              type="button"
              onClick={() => setSettingsOpen(true)}
              className="inline-flex h-8 items-center gap-1.5 rounded-btn border border-border bg-elevated px-3 text-xs text-secondary transition-colors hover:text-foreground"
            >
              <Settings2 size={13} />
              资讯设置
            </button>
          </div>
        }
      />

      <div className="flex-1 space-y-3 overflow-y-auto px-5 py-3">
        {/* 状态行 */}
        <div className="flex flex-wrap items-center gap-2">
          {(settingsQuery.data?.focus_concepts ?? []).map(conceptItem => (
            <button
              key={conceptItem}
              type="button"
              title={`按概念「${conceptItem}」筛选`}
              onClick={() => {
                setConceptDraft(conceptItem)
                setConcept(conceptItem)
              }}
              className="inline-flex h-7 items-center gap-1 rounded-full border border-border bg-surface px-2.5 text-xs text-secondary transition-colors hover:text-accent"
            >
              <Newspaper size={12} />
              {conceptItem}聚焦
            </button>
          ))}
          <span className="inline-flex h-7 items-center rounded-full border border-border bg-surface px-2.5 text-xs text-muted">
            定时抓取: 每隔 {status?.interval_minutes ?? '—'} 分钟
            {status?.next_run_at ? ` · 下次 ${status.next_run_at.slice(11)}` : ''}
          </span>
          <span className="inline-flex h-7 items-center rounded-full border border-border bg-surface px-2.5 text-xs text-muted">
            消息推送:{' '}
            {status?.push.enabled
              ? status.push.configured
                ? `开启(${status.push.channels.map(channel => PUSH_CHANNEL_LABELS[channel] ?? channel).join('/')})`
                : '开启(渠道未配置)'
              : '关闭'}
          </span>
          {status && !status.ai_configured && (
            <span className="inline-flex h-7 items-center rounded-full border border-amber-400/30 bg-amber-400/10 px-2.5 text-xs text-amber-300">
              AI 未配置, 暂不标注利好/利空
            </span>
          )}
        </div>

        {/* 筛选区 */}
        <div className="space-y-2.5 rounded-card border border-border bg-surface px-4 py-3">
          <ChipGroup<RangeKey>
            label="时间"
            value={range}
            onChange={setRange}
            options={[
              { key: 'today', label: '今天' },
              { key: '3d', label: '近三天' },
              { key: '1w', label: '近一周' },
              { key: '1m', label: '近一月' },
              { key: 'custom', label: '自定义' },
            ]}
          />
          {range === 'custom' && (
            <div className="flex items-center gap-2 pl-11">
              <DatePicker value={startDate} onChange={setStartDate} placeholder="开始日期" align="left" />
              <span className="text-xs text-muted">至</span>
              <DatePicker value={endDate} onChange={setEndDate} placeholder="结束日期" align="left" />
            </div>
          )}
          <ChipGroup<TypeKey>
            label="类型"
            value={itemType}
            onChange={setItemType}
            options={[
              { key: 'all', label: '全部' },
              { key: 'positive', label: '利好' },
              { key: 'negative', label: '利空' },
              { key: 'important', label: '重要' },
              { key: 'watchlist', label: '自选股' },
            ]}
          />
          <ChipGroup
            label="来源"
            value={source}
            onChange={setSource}
            options={[
              { key: 'all', label: '全部' },
              ...Object.entries(SOURCE_LABELS).map(([key, label]) => ({ key, label })),
            ]}
          />
          <form
            className="flex items-center gap-2"
            onSubmit={event => {
              event.preventDefault()
              applySearch()
            }}
          >
            <span className="w-8 shrink-0 text-xs text-muted">检索</span>
            <SearchInput value={keywordDraft} onChange={setKeywordDraft} placeholder="标题 / 正文关键词" />
            <SearchInput value={conceptDraft} onChange={setConceptDraft} placeholder="影响概念(如 算力)" />
            <SearchInput value={symbolDraft} onChange={setSymbolDraft} placeholder="影响个股代码(如 300726)" />
            <button
              type="submit"
              className="inline-flex h-8 shrink-0 items-center gap-1.5 rounded-btn border border-accent/40 bg-accent/15 px-3 text-xs text-accent transition-colors hover:bg-accent/25"
            >
              <Search size={13} />
              查询
            </button>
          </form>
        </div>

        {/* 统计行 */}
        {summary && (
          <div className="flex items-center gap-3 text-xs text-muted">
            <span>
              共 <span className="font-semibold text-foreground">{summary.total}</span> 条
            </span>
            <span className="inline-flex items-center gap-1">
              <span className="h-1.5 w-1.5 rounded-full bg-bull" /> 利好 {summary.positive}
            </span>
            <span className="inline-flex items-center gap-1">
              <span className="h-1.5 w-1.5 rounded-full bg-bear" /> 利空 {summary.negative}
            </span>
          </div>
        )}

        {/* 错误提示 */}
        {itemsQuery.isError && (
          <div className="rounded-card border border-bull/30 bg-bull/10 px-4 py-3 text-xs text-bull">
            加载失败: {(itemsQuery.error as Error).message}
            <button
              type="button"
              onClick={() => itemsQuery.refetch()}
              className="ml-2 underline underline-offset-2"
            >
              重试
            </button>
          </div>
        )}
        {fetchNow.isError && (
          <div className="rounded-card border border-bull/30 bg-bull/10 px-4 py-3 text-xs text-bull">
            抓取失败: {(fetchNow.error as Error).message}
          </div>
        )}

        {/* 列表 */}
        {itemsQuery.isLoading ? (
          <div className="py-16 text-center text-xs text-muted">加载中…</div>
        ) : items.length === 0 ? (
          <EmptyState
            icon={Inbox}
            title="暂无资讯"
            hint="点击右上角「立即抓取」, 或等待定时抓取; 也可放宽筛选条件"
          />
        ) : (
          <div className="space-y-2.5 pb-2">
            {items.map(item => (
              <NewsCard key={item.id} item={item} />
            ))}
            {itemsQuery.hasNextPage && (
              <button
                type="button"
                disabled={itemsQuery.isFetchingNextPage}
                onClick={() => itemsQuery.fetchNextPage()}
                className="mx-auto block h-8 rounded-btn border border-border bg-elevated px-4 text-xs text-secondary transition-colors hover:text-foreground disabled:opacity-60"
              >
                {itemsQuery.isFetchingNextPage ? '加载中…' : '加载更多'}
              </button>
            )}
          </div>
        )}
      </div>

      {settingsOpen && settingsQuery.data && (
        <SettingsDialog settings={settingsQuery.data} status={status} onClose={() => setSettingsOpen(false)} />
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// 扩展注册
// ---------------------------------------------------------------------------

const extension: FrontendExtension = {
  id: 'news.feed',
  apiVersion: 1,
  routes: [{ id: 'news-feed', path: '/news', component: NewsPage }],
  navigation: [{ id: 'news-feed', routeId: 'news-feed', label: '最新资讯', icon: Newspaper, order: 90 }],
}

export default extension
