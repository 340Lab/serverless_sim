use std::{cell::RefCell, cmp::Eq, collections::HashMap, fmt::Debug, hash::Hash, rc::Rc};

use super::InstanceCachePolicy;
use super::ListNode;

// Fifo缓存结构
pub struct FifoCache<Payload: Eq + Hash + Clone + Debug> {
    capacity: usize,
    cache: HashMap<Payload, Rc<RefCell<ListNode<Payload>>>>,
    head: Rc<RefCell<ListNode<Payload>>>,
    tail: Rc<RefCell<ListNode<Payload>>>,
}

impl<Payload: Eq + Hash + Clone + Debug> InstanceCachePolicy<Payload> for FifoCache<Payload> {
    fn get(&mut self, key: Payload) -> Option<Payload> {
        if let Some(rc_node) = self.cache.get(&key) {
            let node: Rc<RefCell<ListNode<Payload>>> = rc_node.clone();
            //let value = Some(node.borrow().value.clone());
            self.remove_node(node.clone());
            self.move_to_head(node);
            return Some(key);
        }
        None
    }

    // return Some(payload) if one is evcited
    fn put(
        &mut self,
        key: Payload,
        mut can_be_evict: Box<dyn FnMut(&Payload) -> bool>,
    ) -> (Option<Payload>, bool) {
        if self.cache.contains_key(&key) {
            let listnode = self.cache.get(&key).unwrap().clone();
            //listnode.borrow_mut().value = Some(value);
            return (None, true);
            //找到了，id为None，put成功
        }

        let mut res = (None, true);
        if self.cache.len() == self.capacity {
            let mut back_node = self.tail.borrow().prev.clone().unwrap();
            while back_node.borrow().key.is_some() {
                if can_be_evict(back_node.borrow().key.as_ref().unwrap()) {
                    // 取出并返回被淘汰节点的键（Payload），以便外部使用
                    let key_to_remove = back_node.borrow().key.clone().unwrap();
                    self.cache.remove(&key_to_remove);
                    self.remove_node(back_node);
                    res = (Some(key_to_remove), true);
                    break;
                    //找到要删除的，返回id，put成功
                } else {
                    let next_back_node = back_node.borrow().prev.clone().unwrap();
                    back_node = next_back_node;
                }
            }
            if res.0.is_none() {
                return (None, false);
            }
        }

        // insert should happen after check
        let lsnode = ListNode::new(Some(key.clone()));
        self.cache.insert(key.clone(), lsnode.clone());
        self.move_to_head(lsnode.clone()); // 放在最上面

        res
    }

    /// 从 Fifo 缓存中删除一个节点
    fn remove_all(&mut self, key: &Payload) -> bool {
        if let Some(node) = self.cache.remove(key) {
            self.remove_node(node);
            return true;
        }
        false
    }
}

unsafe impl<Payload: Eq + Hash + Clone + Debug> Send for FifoCache<Payload> {}

impl<Payload: Eq + Hash + Clone + Debug> FifoCache<Payload> {
    pub fn new(capacity: usize) -> Self {
        let head = ListNode::new(None);
        let tail = ListNode::new(None);
        head.borrow_mut().next = Some(tail.clone());
        tail.borrow_mut().prev = Some(head.clone());
        FifoCache {
            capacity,
            cache: HashMap::new(),
            head,
            tail,
        }
    }

    //包括removeNode和别的删除
    // pub fn removeAll(&mut self, fnid: FnId) {
    //     self.removeNode(self.get(fnid));
    //     self.cache.remove(node.borrow().key.as_ref().unwrap());
    // }

    fn move_to_head(&mut self, node: Rc<RefCell<ListNode<Payload>>>) {
        let next = self.head.borrow().next.clone();
        node.borrow_mut().prev = Some(self.head.clone());
        node.borrow_mut().next = next.clone();
        self.head.borrow_mut().next = Some(node.clone());
        next.unwrap().borrow_mut().prev = Some(node);
    }

    fn remove_node(&mut self, node: Rc<RefCell<ListNode<Payload>>>) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    // 测试用例开始
    #[test]
    fn test_fifo_cache_put_get() {
        let mut cache = FifoCache::<usize>::new(3); // 创建一个容量为3的FifoCache

        // 测试put操作
        assert_eq!(cache.put(1, Box::new(|_| true)), (None, true));
        assert_eq!(cache.put(2, Box::new(|_| true)), (None, true));
        assert_eq!(cache.put(3, Box::new(|_| true)), (None, true));

        // 当缓存已满时再put应该导致最老的元素被移除
        assert_eq!(cache.put(4, Box::new(|_| true)), ((Some(1), true)));

        // 测试get操作
        assert_eq!(cache.get(2), Some(2)); // 应该找到元素2
        assert_eq!(cache.get(5), None); // 不应该找到元素5

        // 再次put一个新元素，应该移除最老的元素
        assert_eq!(cache.put(5, Box::new(|_| true)), ((Some(3), true)));
    }

    #[test]
    fn test_fifo_cache_remove_all() {
        let mut cache = FifoCache::<usize>::new(5); // 创建一个容量为5的FifoCache

        // 插入一些元素
        cache.put(1, Box::new(|_| true)).0;
        cache.put(2, Box::new(|_| true)).0;
        cache.put(3, Box::new(|_| true)).0;
        cache.put(4, Box::new(|_| true)).0;

        // 删除一个存在的元素
        assert_eq!(cache.remove_all(&2), true);
        // 尝试删除一个不存在的元素
        assert_eq!(cache.remove_all(&6), false);
    }

    #[test]
    fn test_fifo_cache_eviction_policy() {
        let mut cache = FifoCache::<usize>::new(2); // 创建一个容量为2的FifoCache

        // 插入三个元素，第三个元素应导致第一个被驱逐
        cache.put(1, Box::new(|_| true)).0;
        cache.put(2, Box::new(|_| true)).0;
        cache.put(3, Box::new(|_| true)).0;

        // 确认第一个元素已被驱逐
        assert_eq!(cache.get(1), None);

        // 确认后两个元素还在
        assert_eq!(cache.get(2), Some(2));
        assert_eq!(cache.get(3), Some(3));
    }

    #[test]
    fn test_fifo_cache_list_integrity() {
        let mut cache = FifoCache::<usize>::new(3); // 创建一个容量为3的FifoCache

        // 插入元素
        cache.put(1, Box::new(|_| true)).0;
        cache.put(2, Box::new(|_| true)).0;
        cache.put(3, Box::new(|_| true)).0;

        // 验证链表顺序
        cache.cmp_list(vec![3, 2, 1]);

        // 再插入一个元素导致第一个被驱逐
        cache.put(4, Box::new(|_| true)).0;
        cache.cmp_list(vec![4, 3, 2]);
    }
}
