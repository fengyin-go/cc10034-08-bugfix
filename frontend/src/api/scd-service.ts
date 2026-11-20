/** 缓慢变化维版本链接口：历史清单、当前版本、时点重算与回放核对。 */
import { request } from './client'

export type DimensionVersion = {
  dim_table: string
  natural_key: string
  version: number
  surrogate_key: string
  effective_from: string
  effective_to: string
  is_current: boolean
  event_id: string
  changed_fields: { field: string; old: unknown; new: unknown }[]
  late_correction: boolean
  restated: boolean
  digest: string
  [field: string]: unknown
}

export type ReplayEntry = {
  replay_id: string
  dim_table: string
  natural_key: string
  version: number | null
  event_id: string
  disposition: string
  detail: string
  late_correction: boolean
  restated: boolean
  effective_from: string | null
  effective_to: string | null
  digest: string
  [field: string]: unknown
}

export type ReconcileResult = {
  dim_table: string
  consistent: boolean
  manifest_version_count: number
  history_version_count: number
  mismatches: { type: string; [field: string]: unknown }[]
}

async function getJson<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) throw new Error(`版本链接口读取失败（${response.status}）`)
  return (await response.json()) as T
}

export function listDimensionTables(moduleKey = 'scd_handler'): Promise<{
  items: { 维度表?: string }[]
}> {
  return getJson(`/api/${moduleKey}?size=200`)
}

export function fetchHistory(dimTable: string, naturalKey?: string) {
  const query = new URLSearchParams({ dim_table: dimTable })
  if (naturalKey) query.set('natural_key', naturalKey)
  return getJson<{ items: DimensionVersion[]; total: number }>(`/api/scd/history?${query}`)
}

export function fetchCurrent(dimTable: string, naturalKey?: string) {
  const query = new URLSearchParams({ dim_table: dimTable })
  if (naturalKey) query.set('natural_key', naturalKey)
  return getJson<{ item?: DimensionVersion; items?: Record<string, DimensionVersion> }>(
    `/api/scd/current?${query}`,
  )
}

export function fetchAsOf(dimTable: string, naturalKey: string, moment: string) {
  const query = new URLSearchParams({ dim_table: dimTable, natural_key: naturalKey, moment })
  return getJson<{ item: DimensionVersion | null }>(`/api/scd/as-of?${query}`)
}

export function fetchManifest(dimTable?: string, naturalKey?: string) {
  const query = new URLSearchParams()
  if (dimTable) query.set('dim_table', dimTable)
  if (naturalKey) query.set('natural_key', naturalKey)
  return getJson<{ items: ReplayEntry[]; total: number }>(`/api/scd/manifest?${query}`)
}

export function fetchReconcile(dimTable?: string) {
  const query = new URLSearchParams()
  if (dimTable) query.set('dim_table', dimTable)
  return getJson<ReconcileResult>(`/api/scd/reconcile?${query}`)
}
