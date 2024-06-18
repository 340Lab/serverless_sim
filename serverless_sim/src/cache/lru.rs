use std::{
    cell::{Ref, RefCell, RefMut},
    clone,
    cmp::{Eq, Ordering},
    collections::{BTreeSet, HashMap, HashSet, LinkedList},
    fmt::Debug,
    hash::Hash,
    ptr::NonNull,
    rc::Rc,
};

use moka::sync::Cache;

use crate::{
    fn_dag::{FnContainer, FnContainerState, FnId, Func},
    request::ReqId,
    sim_env::{self, SimEnv},
    util, NODE_CNT, NODE_LEFT_MEM_THRESHOLD, NODE_SCORE_CPU_WEIGHT, NODE_SCORE_MEM_WEIGHT,
};

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

// LRU缓存结构
pub struct LRUCache<Payload: Eq + Hash + Clone + Debug> {
    capacity: usize,
    cache: HashMap<Payload, Rc<RefCell<ListNode<Payload>>>>,
    head: Rc<RefCell<ListNode<Payload>>>,
    tail: Rc<RefCell<ListNode<Payload>>>,
    // dummy: Rc<RefCell<ListNode<Payload>>>,
}

unsafe impl<Payload: Eq + Hash + Clone + Debug> Send for LRUCache<Payload> {}

impl<Payload: Eq + Hash + Clone + Debug> LRUCache<Payload> {
    pub fn new(capacity: usize) -> Self {
        // let dummy = ListNode::new(None);
        // dummy.borrow_mut().prev = Some(dummy.clone());
        // dummy.borrow_mut().next = Some(dummy.clone());
        let head = ListNode::new(None);
        let tail = ListNode::new(None);
        //let head_borrow_mut = head.borrow_mut();
        //let tail_borrow_mut = tail.borrow_mut();
        head.borrow_mut().next = Some(tail.clone());
        tail.borrow_mut().prev = Some(head.clone());
        LRUCache {
            capacity,
            cache: HashMap::new(),
            head,
            tail,
            // dummy,
        }
    }
    pub fn get(&mut self, key: Payload) -> Option<Payload> {
        if let Some(rc_node) = self.cache.get(&key) {
            let node: Rc<RefCell<ListNode<Payload>>> = rc_node.clone();
            //let value = Some(node.borrow().value.clone());
            self.removeNode(node.clone());
            self.moveToHead(node);
            return Some(key);
        }
        None
    }

    // return Some(payload) if one is evcited
    pub fn put(
        &mut self,
        key: Payload,
        mut can_be_evict: impl FnMut(&Payload) -> bool,
    ) -> (Option<Payload>, bool) {
        if self.cache.contains_key(&key) {
            let listnode = self.cache.get(&key).unwrap().clone();
            //listnode.borrow_mut().value = Some(value);
            self.removeNode(listnode.clone());
            self.moveToHead(listnode);
            return (None, true);
            //找到了，id为None，put成功
        }
        let lsnode = ListNode::new(Some(key.clone()));
        self.cache.insert(key.clone(), lsnode.clone());
        self.moveToHead(lsnode.clone()); // 放在最上面
        if self.cache.len() > self.capacity {
            let mut back_node = self.tail.borrow().prev.clone().unwrap();
            while back_node.borrow().key.is_some() {
                if can_be_evict(back_node.borrow().key.as_ref().unwrap()) {
                    // 取出并返回被淘汰节点的键（Payload），以便外部使用
                    let key_to_remove = back_node.borrow().key.clone().unwrap();
                    self.cache.remove(&key_to_remove);
                    self.removeNode(back_node);
                    return (Some(key_to_remove), true);
                    //找到要删除的，返回id，put成功
                } else {
                    let next_back_node = back_node.borrow().prev.clone().unwrap();
                    back_node = next_back_node;
                }
            }
            self.removeNode(lsnode);
            self.cache.remove(&key);
            return (None, false);
        }
        (None, true)
    }

    //包括removeNode和别的删除
    // pub fn removeAll(&mut self, fnid: FnId) {
    //     self.removeNode(self.get(fnid));
    //     self.cache.remove(node.borrow().key.as_ref().unwrap());
    // }

    /// 从 LRU 缓存中删除一个节点
    pub fn removeAll(&mut self, key: &Payload) -> bool {
        if let Some(node) = self.cache.remove(key) {
            self.removeNode(node);
            return true;
        }
        false
    }

    fn moveToHead(&mut self, node: Rc<RefCell<ListNode<Payload>>>) {
        let next = self.head.borrow().next.clone();
        node.borrow_mut().prev = Some(self.head.clone());
        node.borrow_mut().next = next.clone();
        self.head.borrow_mut().next = Some(node.clone());
        next.unwrap().borrow_mut().prev = Some(node);
    }

    fn removeNode(&mut self, node: Rc<RefCell<ListNode<Payload>>>) {
        let prev = node.borrow().prev.clone().unwrap();
        let next = node.borrow().next.clone().unwrap();
        prev.borrow_mut().next = Some(next.clone());
        next.borrow_mut().prev = Some(prev);
    }

    #[cfg(test)]
    pub fn cmp_list(&self, list: Vec<Payload>) {
        assert_eq!(self.cache.len(), list.len());
        let mut cur = self.head.borrow().next.clone();
        for i in &list {
            if let Some(n) = cur {
                assert_eq!(i, n.borrow().key.as_ref().unwrap());
                cur = n.borrow().next.clone();
            } else {
                panic!();
            }
        }
        assert!(cur.unwrap().borrow().key.is_none());
    }

    #[cfg(test)]
    fn print_list(&self) {
        let mut cur = Some(self.head.clone());
        while let Some(n) = cur {
            println!("{:?}", n.borrow().key);
            cur = n.borrow().next.clone();
        }
    }
}
