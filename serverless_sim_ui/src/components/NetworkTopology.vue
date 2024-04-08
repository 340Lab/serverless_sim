<template>
  <div class="network-topology">
    <h1>网络拓扑图</h1>
    <div class="topology-container" @mousedown="startDragging" @mousemove="dragging" @mouseup="stopDragging"
      @mouseleave="stopDragging">
      <svg class="connection-lines">
        <g v-for="(link, key) in links" :key="key">
          <line :x1="nodes[link.source[0]].x" :y1="nodes[link.source[0]].y" :x2="nodes[link.source[1]].x"
            :y2="nodes[link.source[1]].y" stroke="red" />
          <rect :x="(nodes[link.source[0]].x + nodes[link.source[1]].x) / 2 - 40"
            :y="(nodes[link.source[0]].y + nodes[link.source[1]].y) / 2 + 10" width="80" height="20" fill="white"
            stroke="black" stroke-width="1" />
          <!--          <text :x="(nodes[link.source[0]].x + nodes[link.source[1]].x) / 2"-->
          <!--                :y="(nodes[link.source[0]].y + nodes[link.source[1]].y) / 2 + 25" text-anchor="middle" @click="showEditBandwidthPopup">修改带宽</text>-->

        </g>
        <text v-for="(link, key) in links" :key="'text_' + key"
          :x="(nodes[link.source[0]].x + nodes[link.source[1]].x) / 2"
          :y="(nodes[link.source[0]].y + nodes[link.source[1]].y) / 2">{{ link.bandwidth }}</text>
      </svg>
      <div v-for="(link, key) in links" :key="'edit-bandwidth-' + key" class="edit-bandwidth" :style="{
      left: ((nodes[link.source[0]].x + nodes[link.source[1]].x) / 2 - 40) + 'px',
      top: ((nodes[link.source[0]].y + nodes[link.source[1]].y) / 2 + 10) + 'px'
    }" :v-show="!editingBandwidth" @click="showEditBandwidthPopup(key)">
        修改带宽
      </div>
      <div v-for="(node, index) in nodes" :key="index"
        :style="{ top: node.y + 'px', left: node.x + 'px', transform: 'translate(-50%, -50%)', zIndex: node.zIndex }"
        class="node" ref="nodes" @mousedown="startNodeDragging(index)" @mouseup="stopNodeDragging">
        {{ node.id }}
        <button @click="removeNode(index)" class="remove-node-btn">删除</button>
      </div>
    </div>

    <button @click="addNode" class="add-node-btn">添加节点</button>
  </div>
  <el-dialog title="修改带宽" v-model="editingBandwidth" width="50%">
    <p>请输入带宽：</p>
    <input type="number" v-model="editedBandwidth">
    <div slot="footer">
      <el-button @click="editingBandwidth = false">取消</el-button>
      <el-button type="primary" @click="confirmEdit">确认</el-button>
    </div>
  </el-dialog>
</template>

<script>
import { UINode } from "@/network_topo";
import { UILink } from "@/network_topo";

export default {
  data() {
    return {
      nodes: [],
      links: {},
      draggingNode: null,
      offset: { x: 0, y: 0 },
      editingBandwidth: false,
      editedBandwidth: null,
      selectedLinkId: null,
    };
  },
  methods: {
    startDragging(event) {
      this.offset.x = event.pageX;
      this.offset.y = event.pageY;
    },
    dragging(event) {
      if (this.draggingNode !== null) {
        let newX = event.pageX - this.offset.x + this.nodes[this.draggingNode].x;
        let newY = event.pageY - this.offset.y + this.nodes[this.draggingNode].y;
        if (newX < 0) { newX = 0 }
        if (newY < 0) { newY = 0 }
        if (newX > 600) { newX = 600 }
        if (newY > 400) { newY = 400 }
        this.nodes[this.draggingNode].x = newX;
        this.nodes[this.draggingNode].y = newY;
        this.offset.x = event.pageX;
        this.offset.y = event.pageY;
      }
    },
    stopDragging() {
      this.draggingNode = null;
    },
    startNodeDragging(index) {
      this.draggingNode = index;
      this.nodes[index].zIndex = 1;
    },
    stopNodeDragging() {
      if (this.draggingNode !== null) {
        this.nodes[this.draggingNode].zIndex = 0;
      }
      this.draggingNode = null;
    },
    removeNode(index) {
      // 从 nodes 数组中删除指定索引的节点
      this.nodes.splice(index, 1);
      // 从 links 对象中删除与该节点相关的所有链接
      Object.keys(this.links).forEach((key) => {
        const link = this.links[key];
        // 如果链接的起始节点或目标节点索引等于被删除节点的索引，则删除该链接
        if (link.source[0] === index || link.source[1] === index) {
          delete this.links[key];
        } else {
          // 更新链接的目标节点索引，以反映节点删除后的新索引
          const source0 = link.source[0] > index ? link.source[0] - 1 : link.source[0];
          const source1 = link.source[1] > index ? link.source[1] - 1 : link.source[1];
          link['source'] = [source0, source1]
        }
      });
    },
    addNode() {
      const newNode = new UINode(Math.random() * 500, Math.random() * 500, 0, this.nodes.length);
      if (newNode.x < 0) { newNode.x = 0 }
      if (newNode.y < 0) { newNode.y = 0 }
      if (newNode.x > 600) { newNode.x = 600 }
      if (newNode.y > 400) { newNode.y = 400 }
      this.nodes.push(newNode);
      this.connectNodes(this.nodes.length - 1);
    },
    connectNodes(nodeIndex) {
      for (let i = 0; i < this.nodes.length; i++) {
        if (i !== nodeIndex) {
          const key = Math.min(nodeIndex, i) + '_' + Math.max(nodeIndex, i);
          if (!(key in this.links)) {
            const newLink = new UILink([nodeIndex, i], 0);
            this.links[key] = newLink
          }
        }
      }
    },
    showEditBandwidthPopup(linkId) {
      console.log("showEditBandwidthPopup", linkId);
      this.selectedLinkId = linkId;
      this.editingBandwidth = true;
    },
    confirmEdit() {
      if (this.selectedLinkId !== null && this.editedBandwidth !== null) {
        this.links[this.selectedLinkId].bandwidth = this.editedBandwidth;
        this.editingBandwidth = false;
        this.editedBandwidth = null;
        this.selectedLinkId = null;
      }
    },
  },
};
</script>

<style scoped>
.network-topology {
  padding: 20px;
}

.topology-container {
  position: relative;
  width: 600px;
  height: 400px;
  border: 1px solid #ccc;
}

.node {
  position: absolute;
  width: 80px;
  height: 80px;
  background-color: #2196F3;
  border-radius: 50%;
  display: flex;
  justify-content: center;
  align-items: center;
  color: white;
  font-size: 16px;
  cursor: move;
}

.connection-lines {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
}

.remove-node-btn {
  position: absolute;
  bottom: -20px;
  left: 50%;
  transform: translateX(-50%);
  cursor: pointer;
}

.add-node-btn {
  position: absolute;
  bottom: 20px;
  left: 50%;
  transform: translateX(-50%);
  cursor: pointer;
}

.edit-bandwidth {
  position: absolute;
  width: 80px;
  height: 20px;
  background-color: white;
  border: 1px solid black;
  text-align: center;
  line-height: 20px;
  cursor: pointer;
}
</style>
