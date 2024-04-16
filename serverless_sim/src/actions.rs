use enum_as_inner::EnumAsInner;

pub type RawAction = u32;

#[derive(EnumAsInner, Clone)]
pub enum ESActionWrapper {
    // Float(f32),
    Int(u32),
}
