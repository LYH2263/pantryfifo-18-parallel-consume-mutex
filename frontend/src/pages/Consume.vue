<template>
  <div>
    <h1>按临期消费</h1>
    <select v-model.number="item_id"><option v-for="i in items" :value="i.id">{{ i.name }}</option></select>
    <input type="number" v-model.number="qty" min="0" step="any" />
    <button :disabled="pending" @click="go">{{ pending ? '扣减中…' : 'FEFO 扣减' }}</button>
    <p v-if="state === 'ok'" class="ok">已按 FEFO 扣减，余量已刷新</p>
    <p v-else-if="state === 'conflict'" class="bad">冲突：在架余量不足或批次刚变化，本笔未扣减</p>
    <p v-else-if="state === 'error'" class="bad">{{ detail }}</p>
    <pre v-if="state === 'ok' && detail">{{ detail }}</pre>
    <h3>当前在架</h3>
    <span v-for="x in shelf" :key="x.id" class="lot">{{ x.name }} ×{{ x.qty_remain }} · {{ x.expiry }}</span>
    <p v-if="!shelf.length" class="muted">该品项无在架批次</p>
  </div>
</template>
<script setup>
import { ref, watch, onMounted } from 'vue'
import { api } from '../api'
const items = ref([])
const shelf = ref([])
const item_id = ref(1)
const qty = ref(1)
const pending = ref(false)
// 拍板结果态：ok / conflict / error，覆盖式单态——
// 连续确认两次，界面上只会留下后端拍板允许的那一种结果。
const state = ref('')
const detail = ref('')
async function loadShelf() {
  if (!item_id.value) { shelf.value = []; return }
  const all = await api('/fridge')
  shelf.value = all.filter(x => x.item_id === item_id.value && x.qty_remain > 0)
}
async function go() {
  if (pending.value) return
  pending.value = true
  try {
    const r = await api('/consume', { method: 'POST', body: JSON.stringify({ item_id: item_id.value, qty: qty.value }) })
    state.value = 'ok'
    detail.value = JSON.stringify(r.deductions, null, 2)
  } catch (e) {
    state.value = e.status === 409 ? 'conflict' : 'error'
    detail.value = e.message
  } finally {
    pending.value = false
    await loadShelf() // 成功/冲突都刷新，下一次确认看到的是真实余量
  }
}
watch(item_id, loadShelf)
onMounted(async () => {
  items.value = await api('/items')
  if (items.value[0]) item_id.value = items.value[0].id
  await loadShelf()
})
</script>
