use enum_as_inner::EnumAsInner;

pub type RawAction = u32;

pub struct RawActionHelper(pub RawAction);
impl RawActionHelper {
    pub fn is_scale_down(&self) -> bool {
        self.0 % 2 == 0
    }
}

#[derive(Debug)]
pub enum Action {
    /// 选择一个节点放当前函数节点可以延迟最小
    ScaleUp(AdjustThres),
    /// 随机选择一个节点缩容
    ScaleDown(AdjustThres),
    /// 不做操作
    DoNothing,
    ///
    AllowAll(AdjustThres),
}

#[derive(EnumAsInner)]
pub enum EFActionWrapper {
    Float(f32),
    Int(u32),
}

#[derive(Debug, Clone, Copy)]
pub enum AdjustThres {
    Up,
    DOwn,
    Keep,
}

impl TryFrom<u32> for Action {
    type Error = &'static str;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        let action_i = value / 3;
        let adjust_i = value % 3;
        let adjust = match adjust_i {
            0 => AdjustThres::Up,
            1 => AdjustThres::DOwn,
            2 => AdjustThres::Keep,
            _ => panic!("adjust out of range"),
        };
        match action_i {
            0 => { Ok(Action::ScaleUp(adjust)) }
            1 => { Ok(Action::ScaleDown(adjust)) }
            2 => { Ok(Action::AllowAll(adjust)) }
            3 => { Ok(Action::DoNothing) }
            _ => panic!("action out of range"),
        }
    }
}

pub enum AdjustEachFnWatchWindow {
    Up,
    Down,
    Keep,
}

impl TryFrom<u32> for AdjustEachFnWatchWindow {
    type Error = &'static str;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(AdjustEachFnWatchWindow::Down),
            1 => Ok(AdjustEachFnWatchWindow::Keep),
            2 => Ok(AdjustEachFnWatchWindow::Up),
            _ => panic!("AdjustEachFnWatchWindow out of range"),
        }
    }
}
