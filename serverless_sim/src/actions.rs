pub enum Action {
    /// 选择一个节点放当前函数节点可以延迟最小
    ExpandGreedy,
    /// 随机选择一个节点放当前函数节点
    ExpandRandom,
    /// 随机选择一个节点缩容
    ShrinkRandom,
    /// 选择一个调用频率较小，函数实例最多的缩容
    ShrinkRuleBased,
    /// 不做操作
    DoNothing,
}
