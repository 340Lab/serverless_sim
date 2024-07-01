pub mod fifo;
pub mod lru;
pub mod no_evict;

use std::{cell::RefCell, cmp::Eq, collections::HashMap, fmt::Debug, hash::Hash, rc::Rc};

// 双向链表节点
pub struct ListNode<Payload> {
    key: Option<Payload>, // None when dummy
    // value: Option<FnContainer>,
    prev: Option<Rc<RefCell<ListNode<Payload>>>>,
    next: Option<Rc<RefCell<ListNode<Payload>>>>,
}

unsafe impl<Payload> Send for ListNode<Payload> {}
unsafe impl<Payload> Sync for ListNode<Payload> {}

impl<Payload> ListNode<Payload> {
    fn new(key: Option<Payload>) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(ListNode {
            key,
            prev: None,
            next: None,
        }))
    }
}
pub trait InstanceCachePolicy<Payload: Eq + Hash + Clone + Debug>: Send {
    fn get(&mut self, key: Payload) -> Option<Payload>;

    /// can_be_evict: check if the payload is pinned
    /// first return: return Some(payload) if one is evcited
    /// second return: return true if put success
    fn put(
        &mut self,
        key: Payload,
        can_be_evict: Box<dyn FnMut(&Payload) -> bool>,
    ) -> (Option<Payload>, bool);
    fn remove_all(&mut self, key: &Payload) -> bool;
}
