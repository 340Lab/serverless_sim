<template>
  <div class="network-topology">
    <h1>网络拓扑图</h1>
<!--    @mousedown="startDragging"：当鼠标在容器上按下时触发 startDragging 方法，用于开始拖动操作。-->
<!--    @mousemove="dragging"：当鼠标在容器上移动时触发 dragging 方法，用于实时更新拖动节点的位置。-->
<!--    @mouseup="stopDragging"：当鼠标在容器上释放时触发 stopDragging 方法，用于结束拖动操作。-->
<!--    @mouseleave="stopDragging"：当鼠标移出容器时触发 stopDragging 方法，同样用于结束拖动操作。-->
<!--    {{nodes}}-->
    <div
        class="topology-container"
        @mousedown="startDragging"
        @mousemove="dragging"
        @mouseup="stopDragging"
        @mouseleave="stopDragging"
    >
      <!--循环显示所有节点（.node），每个节点的位置根据其数据模型中的x和y坐标动态设置-->
      <div
          v-for="(node, index) in nodes"
          :key="index"
          :style="{ top: node.y + 'px', left: node.x + 'px', transform: 'translate(-50%, -50%)', zIndex: node.zIndex }"
          class="node"
          ref="nodes"
          @mousedown="startNodeDragging(index)"
          @mouseup="stopNodeDragging"
      >
        {{ node.id }}
      </div>
    </div>
    <svg class="connection-lines">
<!--        画连线-->
      <line
          v-for="(link, index) in links"
          :key="index"
          :x1="link.source.x"
          :y1="link.source.y"
          :x2="link.target.x"
          :y2="link.target.y"
          :stroke="link.color"
          stroke-width="2"
      />
    </svg>
  </div>

</template>

<script>
import { request } from "@/request";
import {UINode} from "@/network_topo";

export default {
  data() {
    return {
      nodes: [],
      links: [],
      draggingNode: null,
      offset: { x: 0, y: 0 },
    };
  },
  mounted() {
    // 从后端获取节点拓扑图数据
    const prepare=((data) => {
      // 生成节点数据
      this.nodes = data.nodes.map((node) => ({
        id: node.node_id,
        x: Math.random() * 500, // 随机生成 x 坐标
        y: Math.random() * 500, // 随机生成 y 坐标
        zIndex: 0, // 设置节点层级
      }));

      // 生成连接线数据
      this.links = [];
      data.nodes.forEach((node1, index1) => {
        data.nodes.forEach((node2, index2) => {
          if (index1 < index2) {
            const bandwidth = node1.bandwidth[node2.node_id];
            if (bandwidth > 0) {
              this.links.push({
                source: { x: this.nodes[index1].x, y: this.nodes[index1].y },
                target: { x: this.nodes[index2].x, y: this.nodes[index2].y },
                bandwidth: bandwidth,
                color: "black", // 设置连接线颜色
              });
            }
          }
        });
      });
    });
    this.nodes.push(new UINode(200,300,6,0));
    this.nodes.push(new UINode(50,50,12,1));
    this.nodes.push(new UINode(100,150,5,2));
    this.nodes.push(new UINode(70,80,9,3));

  },
  methods: {
    startDragging(event) {
      // 记录拖动起始点的偏移量
      this.offset.x = event.pageX;
      this.offset.y = event.pageY;
    },
    dragging(event) {
      console.log("dragging ",this.draggingNode)
      // 判断是否在拖动中
      if (this.draggingNode !== null) {
        const newX = event.pageX - this.offset.x + this.nodes[this.draggingNode].x;
        const newY = event.pageY - this.offset.y + this.nodes[this.draggingNode].y;
        // 更新节点位置
        this.nodes[this.draggingNode].x = newX;
        this.nodes[this.draggingNode].y = newY;
        // 更新偏移量
        this.offset.x = event.pageX;
        this.offset.y = event.pageY;
      }
    },
    stopDragging() {
      // 结束拖动
      this.draggingNode = null;
    },
    startNodeDragging(index) {
      // 开始拖动节点
      this.draggingNode = index;
      // 提高节点层级，使其显示在最前面
      this.nodes[index].zIndex = 1;
    },
    nodeDragging(event, index) {
      // 节点拖动过程中的处理
      // event.stopPropagation();
    },
    stopNodeDragging() {
      // 结束节点拖动
      if (this.draggingNode !== null) {
        // 恢复节点层级
        this.nodes[this.draggingNode].zIndex = 0;
      }
      this.draggingNode = null;
    },
  },
};
</script>

<style>
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
  width: 40px;
  height: 40px;
  background-color: #2196F3;
  border-radius: 50%;
  display: flex;
  justify-content: center;
  align-items: center;
  color: white;
  font-size: 16px;
  cursor: move; /* 允许拖动 */
}
.connection-lines {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  pointer-events: none;
}
</style>
