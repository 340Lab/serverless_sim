#[derive(Debug)]
pub enum Action {
    /// 选择一个节点放当前函数节点可以延迟最小
    ScaleUpWithoutElem,
    /// 随机选择一个节点放当前函数节点
    ScaleUpWithElem,
    /// 随机选择一个节点缩容
    ProactiveScaleDown,
    /// 不做操作
    DoNothing,
}

impl TryFrom<u32> for Action {
    type Error = &'static str;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Action::ScaleUpWithoutElem),
            1 => Ok(Action::ScaleUpWithElem),
            2 => Ok(Action::ProactiveScaleDown),
            3 | 4 => Ok(Action::DoNothing),
            _ => Err("invalid value"),
        }
    }
}
