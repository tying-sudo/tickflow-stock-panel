import { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  ChevronDown,
  ChevronRight,
  Coins,
  FolderPlus,
  LoaderCircle,
  RefreshCw,
  Search,
  Trash2,
} from 'lucide-react'
import { api, type EtfGroupAddResult, type FundSearchResult, type WatchlistEntry } from '@/lib/api'
import { QK } from '@/lib/queryKeys'
import { toast } from '@/components/Toast'

// ===== 基金自选页 =====
// 面向基金/ETF 视角: 添加 ETF/基金 → 后端 add-etf-group 以其名称自动建分组,
// 成分股入组 (指数官方成分优先, 披露兜底; 不含 ETF 本体)。
// 本页维护"基金卡片"注册表 (localStorage): 记录 group_id/code/报告期等元数据,
// 成员列表实时取自 /api/watchlist 按 group_ids 过滤, 与自选页分组始终一致。

const REGISTRY_KEY = 'fund-watchlist-registry-v1'

interface FundCard {
  groupId: string
  fundName: string
  /** 建组输入: 场内 ETF 带后缀 (588710.SH) 或 6 位基金代码 (001470) */
  code: string
  reportDate: string
  holdingsCount: number
  /** 官方跟踪指数 (有值 = 严格跟踪口径, 同步会移出旧成员) */
  index?: string | null
  addedAt: string
  syncedAt: string
}

function loadRegistry(): FundCard[] {
  try {
    const raw = localStorage.getItem(REGISTRY_KEY)
    const arr = raw ? JSON.parse(raw) : []
    return Array.isArray(arr) ? arr : []
  } catch {
    return []
  }
}

function saveRegistry(cards: FundCard[]) {
  localStorage.setItem(REGISTRY_KEY, JSON.stringify(cards))
}

function toResult(card: FundCard, r: EtfGroupAddResult): FundCard {
  return {
    ...card,
    groupId: r.group.id,
    fundName: r.fund_name || card.fundName,
    reportDate: r.report_date,
    holdingsCount: r.holdings_count,
    index: r.index ?? null,
    syncedAt: new Date().toISOString(),
  }
}

function upsertCard(cards: FundCard[], next: FundCard): FundCard[] {
  const idx = cards.findIndex(c => c.groupId === next.groupId || c.code === next.code)
  if (idx >= 0) {
    const copy = [...cards]
    copy[idx] = { ...copy[idx], ...next }
    return copy
  }
  return [...cards, next]
}

function fmtTime(iso: string) {
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return '--'
  return `${d.getMonth() + 1}/${d.getDate()} ${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`
}

export function FundWatchlist() {
  const qc = useQueryClient()
  const [input, setInput] = useState('')
  const [expanded, setExpanded] = useState<Record<string, boolean>>({})
  const [cards, setCards] = useState<FundCard[]>(() => loadRegistry())

  useEffect(() => {
    const sync = () => setCards(loadRegistry())
    window.addEventListener('storage', sync)
    return () => window.removeEventListener('storage', sync)
  }, [])

  const groupsQuery = useQuery({
    queryKey: QK.watchlistGroups,
    queryFn: api.watchlistGroups,
    staleTime: 30_000,
  })
  const groups = groupsQuery.data?.groups ?? []
  const groupById = useMemo(() => new Map(groups.map(g => [g.id, g])), [groups])

  const listQuery = useQuery({
    queryKey: QK.watchlist,
    queryFn: api.watchlistList,
    staleTime: 30_000,
  })
  const entries: WatchlistEntry[] = listQuery.data?.symbols ?? []

  // 6 位纯数字 → 场外基金联想 (instruments 维表里没有, 需要 fund-search)
  const bareCode = input.trim()
  const fundLookup = useQuery({
    queryKey: ['fund-search', bareCode],
    queryFn: () => api.watchlistFundSearch(bareCode),
    enabled: /^\d{6}$/.test(bareCode),
    staleTime: 60_000,
    retry: false,
  })
  const fundHit: FundSearchResult | null = fundLookup.data ?? null

  const persist = (next: FundCard[]) => {
    setCards(next)
    saveRegistry(next)
  }

  const addMutation = useMutation({
    mutationFn: (symbol: string) => api.watchlistAddEtfGroup(symbol),
    onSuccess: (r, symbol) => {
      const now = new Date().toISOString()
      const card: FundCard = {
        groupId: r.group.id,
        fundName: r.fund_name,
        code: symbol,
        reportDate: r.report_date,
        holdingsCount: r.holdings_count,
        index: r.index ?? null,
        addedAt: now,
        syncedAt: now,
      }
      persist(upsertCard(cards, card))
      setInput('')
      qc.invalidateQueries({ queryKey: QK.watchlist })
      qc.invalidateQueries({ queryKey: QK.watchlistGroups })
      const parts = [`已建组「${r.fund_name}」`, `${r.added}/${r.holdings_count} 只成分股入组`]
      if (r.removed > 0) parts.push(`移出过期 ${r.removed} 只`)
      toast(parts.join(' · '), 'success')
    },
    onError: (e: Error) => toast(`建组失败: ${e.message}`, 'error'),
  })

  const syncMutation = useMutation({
    mutationFn: (card: FundCard) => api.watchlistAddEtfGroup(card.code),
    onSuccess: (r, card) => {
      persist(upsertCard(cards, toResult(card, r)))
      qc.invalidateQueries({ queryKey: QK.watchlist })
      toast(`「${r.fund_name}」已同步至 ${r.report_date} 口径 (${r.holdings_count} 只)`, 'success')
    },
    onError: (e: Error) => toast(`同步失败: ${e.message}`, 'error'),
  })

  const syncAllMutation = useMutation({
    mutationFn: () => api.watchlistSyncEtfGroups(),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: QK.watchlist })
      qc.invalidateQueries({ queryKey: QK.watchlistGroups })
      toast('全部 ETF 分组已同步到最新口径', 'success')
    },
    onError: (e: Error) => toast(`全量同步失败: ${e.message}`, 'error'),
  })

  const submit = () => {
    const sym = input.trim()
    if (!sym || addMutation.isPending) return
    addMutation.mutate(sym)
  }

  const removeCard = (card: FundCard) => {
    persist(cards.filter(c => c.groupId !== card.groupId))
    toast(`已从基金自选移除「${card.fundName}」卡片 (分组与自选标的保留, 可在自选页分组管理删除)`, 'success')
  }

  const membersOf = (groupId: string) =>
    entries.filter(e => (e.group_ids ?? []).includes(groupId))

  const pending = addMutation.isPending || syncMutation.isPending

  return (
    <div className="mx-auto max-w-5xl px-4 py-5">
      {/* 页头 */}
      <div className="mb-4 flex items-center gap-2.5">
        <Coins className="h-5 w-5 text-accent" />
        <h1 className="text-base font-semibold text-foreground">基金自选</h1>
        <span className="text-xs text-muted">添加 ETF/基金自动建成分股分组</span>
        <div className="flex-1" />
        {cards.length > 0 && (
          <button
            type="button"
            onClick={() => syncAllMutation.mutate()}
            disabled={syncAllMutation.isPending}
            className="inline-flex h-8 items-center gap-1.5 rounded-btn bg-elevated px-2.5 text-xs text-secondary transition-colors hover:bg-accent/10 hover:text-accent disabled:opacity-40"
            title="全部分组按最新口径同步 (指数官方成分优先)"
          >
            {syncAllMutation.isPending
              ? <LoaderCircle className="h-3.5 w-3.5 animate-spin" />
              : <RefreshCw className="h-3.5 w-3.5" />}
            全量同步
          </button>
        )}
      </div>

      {/* 添加区 */}
      <div className="mb-5 rounded-card border border-border bg-surface p-3">
        <div className="flex items-center gap-2">
          <div className="relative flex flex-1 items-center">
            <Search className="pointer-events-none absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted" />
            <input
              type="text"
              value={input}
              onChange={e => setInput(e.target.value)}
              onKeyDown={e => { if (e.key === 'Enter') submit() }}
              placeholder="ETF 代码 (588710.SH) 或 6 位基金代码 (001470)…"
              className="h-9 w-full rounded-btn bg-elevated border border-border pl-8 pr-2.5 text-xs text-foreground placeholder:text-muted focus:border-accent/50 focus:outline-none"
            />
          </div>
          <button
            type="button"
            onClick={submit}
            disabled={!input.trim() || pending}
            className="inline-flex h-9 shrink-0 items-center gap-1.5 rounded-btn bg-accent px-3 text-xs font-medium text-white transition-opacity hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-40"
          >
            {addMutation.isPending
              ? <LoaderCircle className="h-3.5 w-3.5 animate-spin" />
              : <FolderPlus className="h-3.5 w-3.5" />}
            建成分股组
          </button>
        </div>
        {/* 场外基金联想: 6 位纯数字时显示命中结果 */}
        {fundHit && (
          <div className="mt-2 flex items-center gap-2 rounded-btn border border-accent/30 bg-accent/5 px-2.5 py-1.5 text-xs">
            <span className="font-mono text-muted">{fundHit.code}</span>
            <span className="text-foreground">{fundHit.name}</span>
            {fundHit.fund_type && <span className="text-muted">{fundHit.fund_type}</span>}
            <span className="flex-1" />
            <button
              type="button"
              onClick={() => addMutation.mutate(fundHit.code)}
              disabled={pending}
              className="rounded px-1.5 py-0.5 text-accent hover:bg-accent/10 disabled:opacity-40"
            >
              用此基金建组
            </button>
          </div>
        )}
        {!fundHit && /^\d{6}$/.test(bareCode) && !fundLookup.isPending && (
          <div className="mt-2 text-xs text-muted">未在天天基金搜到 {bareCode}，可直接尝试建组 (按 ETF 场内代码处理)</div>
        )}
        <p className="mt-2 text-[11px] leading-relaxed text-muted">
          场内 ETF 走跟踪指数官方成分 (严格跟踪, 同步时自动移出过期成员)；场外基金取披露的重仓股 (只增不删)。
          成分股会同时进入自选列表并归入同名分组，分组可在自选页统一管理。
        </p>
      </div>

      {/* 卡片列表 */}
      {cards.length === 0 ? (
        <div className="grid place-items-center rounded-card border border-dashed border-border py-14 text-muted">
          <div className="flex flex-col items-center gap-2 text-xs">
            <Coins className="h-6 w-6 opacity-50" />
            <span>还没有基金卡片 — 输入 ETF/基金代码创建第一个成分股分组</span>
          </div>
        </div>
      ) : (
        <div className="flex flex-col gap-2.5">
          {cards.map(card => {
            const alive = groupById.has(card.groupId)
            const members = membersOf(card.groupId)
            const open = !!expanded[card.groupId]
            return (
              <div key={card.groupId} className="rounded-card border border-border bg-surface">
                <div className="flex items-center gap-2.5 px-3 py-2.5">
                  <button
                    type="button"
                    onClick={() => setExpanded(m => ({ ...m, [card.groupId]: !open }))}
                    disabled={!alive || members.length === 0}
                    className="rounded p-0.5 text-muted hover:bg-elevated hover:text-foreground disabled:opacity-30"
                    title={open ? '收起成员' : `展开 ${members.length} 只成分股`}
                  >
                    {open ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
                  </button>
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-1.5">
                      <span className="truncate text-sm font-medium text-foreground">{card.fundName}</span>
                      {alive
                        ? <span className="shrink-0 rounded bg-accent/10 px-1 py-0.5 text-[10px] text-accent">{members.length} 只</span>
                        : <span className="shrink-0 rounded bg-danger/10 px-1 py-0.5 text-[10px] text-danger">分组已删除</span>}
                    </div>
                    <div className="mt-0.5 flex flex-wrap items-center gap-x-2.5 gap-y-0.5 text-[11px] text-muted">
                      <span className="font-mono">{card.code}</span>
                      <span>报告期 {card.reportDate || '--'}</span>
                      {card.index && <span>跟踪 {card.index}</span>}
                      <span>同步于 {fmtTime(card.syncedAt)}</span>
                    </div>
                  </div>
                  <button
                    type="button"
                    onClick={() => syncMutation.mutate(card)}
                    disabled={pending || !alive}
                    className="inline-flex shrink-0 items-center gap-1 rounded-btn px-2 py-1 text-xs text-secondary hover:bg-accent/10 hover:text-accent disabled:cursor-not-allowed disabled:opacity-40"
                    title="重新拉取成分股并同步分组 (同名组复用, 严格口径会移出过期成员)"
                  >
                    {syncMutation.isPending && syncMutation.variables?.groupId === card.groupId
                      ? <LoaderCircle className="h-3.5 w-3.5 animate-spin" />
                      : <RefreshCw className="h-3.5 w-3.5" />}
                    同步
                  </button>
                  <button
                    type="button"
                    onClick={() => removeCard(card)}
                    className="shrink-0 rounded p-1 text-muted hover:bg-danger/10 hover:text-danger"
                    title="移除卡片 (不删除分组与自选标的)"
                  >
                    <Trash2 className="h-3.5 w-3.5" />
                  </button>
                </div>
                {open && alive && (
                  <div className="border-t border-border/70 px-3 py-2">
                    {members.length === 0 ? (
                      <div className="py-2 text-center text-xs text-muted">分组暂无成员</div>
                    ) : (
                      <div className="grid grid-cols-2 gap-x-4 gap-y-1 sm:grid-cols-3 lg:grid-cols-4">
                        {members.map(m => (
                          <div key={m.symbol} className="flex min-w-0 items-center gap-1.5 text-xs">
                            <span className="w-[76px] shrink-0 font-mono text-muted">{m.symbol}</span>
                            <span className="truncate text-secondary">{m.name || '--'}</span>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )}
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}
