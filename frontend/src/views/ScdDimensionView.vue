<template>
  <div>
    <WorkbenchView module-key="scd_handler" />

    <section class="page" data-testid="scd-version-panel">
      <header class="page-head">
        <div>
          <h2>Type2 历史版本与回放清单</h2>
          <p class="page-desc">
            维度变更按维度表分别落库为独立版本链；迟到更正不重述已生效区间，重复提交复用首次处置。
          </p>
        </div>
        <div class="page-actions">
          <button class="btn" data-testid="refresh-versions" type="button" @click="refreshAll">
            刷新版本链
          </button>
        </div>
      </header>

      <form class="filter-bar" @submit.prevent="refreshAll">
        <label class="filter-item">
          <span>维度表</span>
          <input v-model="dimTable" data-testid="dimension-input" placeholder="如 dim_customer" />
        </label>
        <label class="filter-item">
          <span>自然键</span>
          <input v-model="naturalKey" data-testid="natural-key-input" placeholder="留空看全部" />
        </label>
        <label class="filter-item">
          <span>历史时点</span>
          <input v-model="asOfMoment" data-testid="as-of-input" placeholder="2026-11-20T12:00:00" />
        </label>
        <button class="btn primary" data-testid="load-history" type="submit">刷新历史查询</button>
      </form>

      <p v-if="errorMessage" class="error-text" data-testid="error">{{ errorMessage }}</p>

      <div class="stat-row">
        <article class="stat-card">
          <span class="stat-label">历史版本数</span>
          <strong class="stat-value" data-testid="history-count">{{ history.length }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">当前版本</span>
          <strong class="stat-value" data-testid="current-version">{{ currentVersion ?? '—' }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">时点重算版本</span>
          <strong class="stat-value" data-testid="as-of-version">{{ asOfVersion ?? '—' }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">清单核对</span>
          <strong class="stat-value" data-testid="reconcile-state">
            {{ reconcile === null ? '—' : reconcile.consistent ? '一致' : '不一致' }}
          </strong>
        </article>
      </div>

      <h3 class="section-title">维度历史清单</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>版本</th>
            <th>自然键</th>
            <th>代理键</th>
            <th>生效时间</th>
            <th>失效时间</th>
            <th>当前</th>
            <th>来源事件</th>
            <th>字段差异</th>
            <th>迟到更正</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="row in history"
            :key="`${row.dim_table}-${row.natural_key}-${row.version}`"
            :data-testid="`version-${row.version}`"
          >
            <td>{{ row.version }}</td>
            <td>{{ row.natural_key }}</td>
            <td>{{ row.surrogate_key }}</td>
            <td>{{ row.effective_from }}</td>
            <td>{{ row.effective_to }}</td>
            <td>{{ row.is_current ? '是' : '否' }}</td>
            <td>{{ row.event_id || '—' }}</td>
            <td>{{ describeDelta(row.changed_fields) }}</td>
            <td>{{ row.late_correction ? '是（不重述）' : '否' }}</td>
          </tr>
          <tr v-if="!history.length">
            <td colspan="9" class="empty-state">暂无历史版本，先从变更事件入口提交维度变更</td>
          </tr>
        </tbody>
      </table>

      <h3 class="section-title">事件回放清单</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>回放号</th>
            <th>维度表</th>
            <th>自然键</th>
            <th>版本</th>
            <th>事件ID</th>
            <th>处置</th>
            <th>说明</th>
            <th>摘要</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="entry in manifest" :key="entry.replay_id" :data-testid="`replay-${entry.replay_id}`">
            <td>{{ entry.replay_id }}</td>
            <td data-testid="replay-dim">{{ entry.dim_table }}</td>
            <td>{{ entry.natural_key }}</td>
            <td>{{ entry.version ?? '—' }}</td>
            <td>{{ entry.event_id || '—' }}</td>
            <td>{{ entry.disposition }}</td>
            <td>{{ entry.detail }}</td>
            <td>{{ entry.digest }}</td>
          </tr>
          <tr v-if="!manifest.length">
            <td colspan="8" class="empty-state">回放清单为空</td>
          </tr>
        </tbody>
      </table>
    </section>
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref, watch } from 'vue'

import {
  fetchAsOf,
  fetchCurrent,
  fetchHistory,
  fetchManifest,
  fetchReconcile,
  type DimensionVersion,
  type ReconcileResult,
  type ReplayEntry,
} from '@/api/scd-service'
import WorkbenchView from '@/views/WorkbenchView.vue'

const dimTable = ref('dim_customer')
const naturalKey = ref('')
const asOfMoment = ref('2026-11-20T12:00:00')
const history = ref<DimensionVersion[]>([])
const manifest = ref<ReplayEntry[]>([])
const currentVersion = ref<number | null>(null)
const asOfVersion = ref<number | null>(null)
const reconcile = ref<ReconcileResult | null>(null)
const errorMessage = ref('')

function describeDelta(fields: DimensionVersion['changed_fields'] | undefined) {
  if (!fields || !fields.length) return '—'
  return fields.map((item) => `${item.field}: ${String(item.old)} → ${String(item.new)}`).join('；')
}

function latestVersion(items: DimensionVersion[]): number | null {
  if (!items.length) return null
  return items[items.length - 1].version
}

async function refreshAll() {
  errorMessage.value = ''
  if (!dimTable.value.trim()) {
    errorMessage.value = '请先填写维度表'
    return
  }
  const table = dimTable.value.trim()
  const key = naturalKey.value.trim()
  try {
    const [historyPayload, manifestPayload, currentPayload, reconcilePayload] = await Promise.all([
      fetchHistory(table, key || undefined),
      fetchManifest(table, key || undefined),
      fetchCurrent(table, key || undefined),
      fetchReconcile(table),
    ])
    history.value = historyPayload.items
    manifest.value = manifestPayload.items
    currentVersion.value = currentPayload.item?.version ?? latestVersion(historyPayload.items)
    reconcile.value = reconcilePayload
    asOfVersion.value = null
    if (key && asOfMoment.value.trim()) {
      const asOfPayload = await fetchAsOf(table, key, asOfMoment.value.trim())
      asOfVersion.value = asOfPayload.item?.version ?? null
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '版本链读取失败'
  }
}

watch(naturalKey, () => {
  if (naturalKey.value.trim()) void refreshAll()
})

onMounted(refreshAll)
</script>
