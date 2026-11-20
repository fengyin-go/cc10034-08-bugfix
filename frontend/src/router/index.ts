import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import ScdDimensionView from '@/views/ScdDimensionView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    // 缓慢变化维除了配置清单，还要能看 Type2 历史版本和回放清单。
    { path: '/scd_handler', name: 'scd-handler', component: ScdDimensionView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
