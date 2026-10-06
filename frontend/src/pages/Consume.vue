<template>
  <div>
    <h1>按临期消费</h1>
    <select v-model.number="item_id" :disabled="busy">
      <option v-for="i in items" :key="i.id" :value="i.id">{{ i.name }}</option>
    </select>
    <input type="number" v-model.number="qty" :disabled="busy" />
    <!-- 确认期间按钮锁死：同一次手势不可能发出第二笔；
         即便网络层重试，请求携带同一 token，后端只拍板一次 -->
    <button :disabled="busy" @click="go">{{ busy ? '确认中…' : 'FEFO 扣减' }}</button>

    <div v-if="outcome" class="outcome" :class="outcome.kind">
      <template v-if="outcome.kind === 'ok'">
        <h3>✅ 已拍板成功（唯一成功态）</h3>
        <p v-for="d in outcome.deductions" :key="d.lot_id" class="lot">
          批次 {{ d.lot_id }}（{{ d.expiry }}）扣 {{ d.take }} → 余 {{ lotRemain(d.lot_id) }}
        </p>
        <p>本品在架余量：<b>{{ outcome.world.item_remain_after }}</b></p>
        <p class="muted">全层余量：{{ JSON.stringify(outcome.world.layer_remain_after) }}</p>
        <p v-if="outcome.replayed" class="muted">（本响应为同一确认手势的幂等重放，未重复扣减）</p>
      </template>
      <template v-else>
        <h3>⛔ 未生效：{{ outcome.reason }}</h3>
        <p v-if="outcome.short">在架正余量还差 {{ outcome.short }}</p>
        <p>本笔已整体失败回滚，未改动任何批次余量。当前本品在架余量：
          <b>{{ outcome.world ? outcome.world.item_remain : '—' }}</b></p>
      </template>
    </div>
  </div>
</template>
<script setup>
import { ref, onMounted } from 'vue'
import { api } from '../api'
const items = ref([])
const item_id = ref(1)
const qty = ref(1)
const busy = ref(false)
const outcome = ref(null)
onMounted(async () => { items.value = await api('/items'); if (items.value[0]) item_id.value = items.value[0].id })

function lotRemain(lotId) {
  const w = outcome.value.world
  return w.lot_remain_after[String(lotId)] ?? 0
}

function newToken() {
  // http + 非 localhost 不是安全上下文，crypto.randomUUID 可能不存在
  if (globalThis.crypto?.randomUUID) return crypto.randomUUID()
  return 'g-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 10)
}

async function go() {
  if (busy.value) return
  outcome.value = null
  busy.value = true
  // 每次确认手势一个新 token：相邻两次确认是两笔独立请求，
  // 由后端锁决定谁成功；只有同手势的重发才共享 token
  const token = newToken()
  try {
    const r = await api('/consume', {
      method: 'POST',
      body: JSON.stringify({ item_id: item_id.value, qty: qty.value, token }),
    })
    outcome.value = { kind: 'ok', ...r }
  } catch (e) {
    // api.js 把 409 的 detail（reason/world）序列化进 message：展示失败世界，
    // 成功态卡片不再渲染，页面上只剩"未生效"这一种结果
    let info = null
    try { info = JSON.parse(e.message) } catch {}
    outcome.value = {
      kind: 'fail',
      reason: info?.reason || e.message,
      short: info?.short,
      world: info?.world
        ? { item_remain: info.world.item_remain_after, layer: info.world.layer_remain_after }
        : null,
    }
  } finally {
    busy.value = false
  }
}
</script>
