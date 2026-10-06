<template>
  <div>
    <h1>{{ props.layer }} 层</h1>
    <span v-for="x in rows" :key="x.id" class="lot">{{ x.name }} ×{{ x.qty_remain }} · {{ x.expiry }}</span>
    <p class="muted">本层在架合计：{{ total }}</p>
  </div>
</template>
<script setup>
import { ref, computed, watch } from 'vue'
import { api } from '../api'
const props = defineProps({ layer: String })
const rows = ref([])
const total = computed(() =>
  rows.value.reduce((s, r) => s + Number(r.qty_remain || 0), 0))
async function load() { rows.value = await api('/fridge?layer=' + props.layer) }
// 每次进入层页都按拍板后的唯一世界重新拉取
watch(() => props.layer, load, { immediate: true })
</script>
