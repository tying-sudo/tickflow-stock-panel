import type { MinuteKlineRow } from '@/lib/api'

/** 从 datetime 串取 HH:MM。契约: 分钟K datetime 已在后端入口统一为北京墙钟, 前端不做时区换算。 */
export function formatMinuteTime(datetime: string): string {
  const match = datetime.match(/(\d{2}):(\d{2})/)
  if (!match) return datetime.slice(11, 16)
  return `${match[1]}:${match[2]}`
}

export function computeIntradayAverage(data: MinuteKlineRow[]): number[] {
  const result: number[] = []
  let amount = 0
  let volume = 0
  for (const row of data) {
    amount += row.amount
    // 分钟K volume 新契约 (2026-09-04 easy_tdx 切换) 已是股, 不再 ×100
    volume += row.volume
    result.push(volume > 0 ? amount / volume : row.close)
  }
  return result
}

function generateFullDayTimes(): string[] {
  const times: string[] = []
  // 09:25-09:29 竞价槽: 数据源分钟K窗口从 9:25 起 (fetch_minute_single), 9:25 集合竞价
  // 撮合 bar 需要槽位落位, 否则被网格丢弃; 9:26-9:29 通常是空槽 (connectNulls 跨过)。
  for (let minute = 25; minute <= 29; minute++) {
    times.push(`09:${String(minute).padStart(2, '0')}`)
  }
  for (let hour = 9; hour <= 11; hour++) {
    const startMinute = hour === 9 ? 30 : 0
    const endMinute = hour === 11 ? 30 : 59
    for (let minute = startMinute; minute <= endMinute; minute++) {
      times.push(`${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}`)
    }
  }
  for (let hour = 13; hour <= 15; hour++) {
    const endMinute = hour === 15 ? 0 : 59
    for (let minute = 0; minute <= endMinute; minute++) {
      times.push(`${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}`)
    }
  }
  return times
}

export const FULL_DAY_TIMES = generateFullDayTimes()
