import type { ReactNode } from 'react'

export function WorkspaceSidebar({ title, count, actions, className, children }: {
  title: string
  count?: number
  actions?: ReactNode
  className: string
  children: ReactNode
}) {
  return <aside className={`workspace-sidebar ${className}`} aria-label={title}>
    <header className="sidebar-heading">
      <h2>{title}</h2>
      {count !== undefined && <span className="sidebar-count">{count}</span>}
      {actions && <div className="sidebar-actions">{actions}</div>}
    </header>
    {children}
  </aside>
}
