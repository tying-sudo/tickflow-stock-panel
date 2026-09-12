import { useEffect, useState } from 'react'

/**
 * 移动端判定 — 视口宽度 < 768px。
 *
 * 界定标准 (全项目统一):
 * - 视口驱动而非 UA 嗅探: 桌面窗口缩窄同样获得移动布局, 标准响应式语义;
 * - 768 边界与 Tailwind `md` 断点及 Layout 历史 767px matchMedia 逻辑对齐;
 * - JS 结构性分支 (如侧栏→抽屉) 用本 hook, 纯样式差异用 CSS `md:` 前缀。
 *
 * SSR 安全: 初始按 window 实测, 无 window 时 false; 挂载后随 matchMedia 变化更新。
 */
export const MOBILE_QUERY = '(max-width: 767px)'

export function useIsMobile(): boolean {
  const getMatch = () =>
    typeof window !== 'undefined' && window.matchMedia(MOBILE_QUERY).matches
  const [isMobile, setIsMobile] = useState(getMatch)

  useEffect(() => {
    const mq = window.matchMedia(MOBILE_QUERY)
    const onChange = (e: MediaQueryListEvent) => setIsMobile(e.matches)
    // 挂载即校正 (useState 初始化到 effect 之间视口可能已变化)
    setIsMobile(mq.matches)
    mq.addEventListener('change', onChange)
    return () => mq.removeEventListener('change', onChange)
  }, [])

  return isMobile
}
