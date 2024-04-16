<script lang="ts">
import CurrentState from "./components/CurrentState.vue";
import SideBar from "./components/SideBar.vue";
import HistoryMetric from "./components/HistoryMetric.vue";
import HorizontalComparation from "./components/HorizontalComparation.vue";
import NetworkTopology from "./components/NetworkTopology.vue";

import { request } from "./request";
import { local_cache } from "./local_cache";
import { page } from "@/page";

class FetchCanceller {
  cancel = false;
}

export default {
  components: {
    SideBar,
    CurrentState,
    HistoryMetric,
    HorizontalComparation,
    NetworkTopology,
  },
  data() {
    return {
      current_page: page.TopoPage,
      // current_state_or_history: true,
      selected_keys: {},
      page: page,
    };
  },

  // `mounted` 是生命周期钩子，之后我们会讲到
  mounted() {
    this.$refs.sidebar.init((select: number, select_name: string) => {
      // console.log("selected bar", select);
      if (select == 0) {
        this.selected_keys = {};
        this.current_page = page.StatePage;
      } else {
        this.current_page = page.RecordPage;
        if ("_" + select in this.selected_keys) {
          this.selected_keys["_" + select].cancel = true;
          this.$refs.history_metric.clean_frame_draw(select_name);
          delete this.selected_keys["_" + select];
          // } else if (this.load_from_local(select_name)) {
        } else {
          this.selected_keys["_" + select] = new FetchCanceller();
          // if (this.$refs.history_metric) {
          this.start_fetch_history(
            this.selected_keys["_" + select],
            select_name
          );
          // }
        }
      }
    });
  },
  methods: {
    load_from_local(name: string) {
      let data = local_cache.try_get(name);
      if (data) {
        let load = JSON.parse(data);
        console.log(load);
        this.$refs.history_metric.update_frames(name, JSON.parse(data));
        return true;
      } else {
        return false;
      }
    },
    async start_fetch_history(canceller: FetchCanceller, name: string) {
      let all_frames = [];
      let last_end = -1;
      let handle_part_res = ({
        begin,
        end,
        frames,
      }: {
        begin: number;
        end: number;
        frames: any[][];
      }) => {
        if (last_end > -1) {
          console.assert(last_end == begin);
        }
        last_end = end;
        all_frames = all_frames.concat(frames);

        this.$refs.history_metric.update_frames(name, all_frames);
      };
      let step = 200;
      let res = await request.history(name, 0, step).request();
      if (canceller.cancel) return;

      let total = res.data.total;
      // console.log("res", res.data);
      handle_part_res(res.data);
      for (let i = 0; i < total / step; i++) {
        let begin = (i + 1) * step;
        let end = begin + step;

        res = await request.history(name, begin, end).request();
        if (canceller.cancel) return;
        handle_part_res(res.data);
      }

      if (total % step != 0) {
        res = await request.history(name, last_end, total).request();
        if (canceller.cancel) return;
        handle_part_res(res.data);
      }
      local_cache.cache(name, JSON.stringify(all_frames));
      console.log("final res", all_frames);
    },
  },
  unmounted() { },
};
</script>

<template>
  <header>
    <!-- <img alt="Vue logo" class="logo" src="./assets/logo.svg" width="125" height="125" /> -->

    <!-- <div class="wrapper">
      <HelloWorld msg="You did it!" />
    </div> -->
  </header>

  <main>
    <div class="row_container">
      <SideBar :selected_keys="selected_keys" style="width: 200px" ref="sidebar" />
      <div class="right_column">

        <NetworkTopology v-if="current_page === page.TopoPage"></NetworkTopology>
        <HistoryMetric v-if="current_page === page.RecordPage" ref="history_metric"></HistoryMetric>
        <CurrentState v-if="current_page === page.StatePage"></CurrentState>
      </div>
      <!--      <CurrentState class="right_column" v-if="current_state_or_history" />-->
      <!--      <div class="right_column">-->
      <!--        HELLO-->
      <!--&lt;!&ndash;        <HorizontalComparation class="right" />&ndash;&gt;-->
      <!--&lt;!&ndash;        <HistoryMetric class="right" ref="history_metric" />&ndash;&gt;-->
      <!--      </div>-->
      <!-- <HistoryMetric /> -->
    </div>
  </main>
</template>

<style scoped>
.row_container {
  padding: 20px;
  display: flex;
  flex-direction: row;
  height: calc(100vh - 60px);
}

.right {}

.right_column {
  width: calc(100% - 200px);
  padding-left: 20px;
  flex-direction: column;
  height: 100%;
  overflow: scroll;
}

/* .col_item {
  width: 25%;
} */
</style>
