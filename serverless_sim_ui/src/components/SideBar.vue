<script lang="ts">
import { request } from "@/request";
export default {
  data() {
    return {
      records: ["当前仿真监控", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
      _select_bar: (idx: number, name: string) => {},
    };
  },

  mounted() {
    request
      .history_list()
      .request()
      .then((history_list) => {
        console.log("history list", history_list);
        this.records = ["当前仿真监控"].concat(history_list.data.list);
      });
  },

  unmounted() {},

  methods: {
    init(_select_bar: (idx: number) => void) {
      this._select_bar = _select_bar;
    },
    item_click(key) {
      this._select_bar(key, this.records[key]);
    },
  },

  props: {
    selected_keys: {
      type: Object,
      default: {},
    },
  },
};
</script>

<template>
  <div class="col_container sidebar">
    <div v-for="(value, key) in records" :key="key" @click="item_click(key)">
      {{ "_" + key in selected_keys ? ">" : "" }}{{ value }}
    </div>
  </div>
</template>

<style scoped>
.row {
  display: flex;
  flex-direction: row;
}
.col_container {
  display: flex;
  flex-direction: column;
}
.sidebar {
  overflow: scroll;
}
</style>
