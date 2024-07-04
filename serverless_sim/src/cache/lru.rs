use std::{cell::RefCell, cmp::Eq, collections::HashMap, fmt::Debug, hash::Hash, rc::Rc};

use super::InstanceCachePolicy;
use super::ListNode;

// LRU缓存结构
pub struct LRUCache<Payload: Eq + Hash + Clone + Debug> {
    capacity: usize,
    cache: HashMap<Payload, Rc<RefCell<ListNode<Payload>>>>,
    head: Rc<RefCell<ListNode<Payload>>>,
    tail: Rc<RefCell<ListNode<Payload>>>,
    // dummy: Rc<RefCell<ListNode<Payload>>>,
}

impl<Payload: Eq + Hash + Clone + Debug> InstanceCachePolicy<Payload> for LRUCache<Payload> {
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
            self.remove_node(listnode.clone());
            self.move_to_head(listnode);
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

    /// 从 LRU 缓存中删除一个节点
    fn remove_all(&mut self, key: &Payload) -> bool {
        if let Some(node) = self.cache.remove(key) {
            self.remove_node(node);
            return true;
        }
        false
    }
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
    

    // 测试LRU缓存的基本插入和获取功能
    #[test]
    fn test_lru_cache_basic_operations() {
        let mut cache = LRUCache::new(3);
        let keys = vec![1, 2, 3];
        for key in &keys {
            assert_eq!(cache.put(key.clone(), Box::new(|_| true)), (None, true));
        }
        for key in &keys {
            assert_eq!(cache.get(key.clone()), Some(key.clone()));
        }
    }

    // 测试缓存容量限制和逐出策略
    #[test]
    fn test_lru_cache_capacity_limit() {
        let mut cache = LRUCache::new(3);
        let keys = vec![1, 2, 3, 4, 5];
        let expected_evictions = vec![None, None, None, Some(1), Some(2)];
        for (i, key) in keys.into_iter().enumerate() {
            assert_eq!(
                cache.put(key, Box::new(|_| true)),
                (expected_evictions[i], true)
            );
        }
        // 确认缓存中剩余的元素
        cache.cmp_list(vec![5, 4, 3]);
    }

    // 测试缓存中元素的删除
    #[test]
    fn test_lru_cache_remove() {
        let mut cache = LRUCache::new(3);
        let keys = vec![1, 2, 3];
        for key in &keys {
            cache.put(key.clone(), Box::new(|_| true)).0;
        }
        assert!(cache.remove_all(&2));
        cache.cmp_list(vec![3, 1]);
    }

    // 测试缓存的遍历和顺序
    #[test]
    fn test_lru_cache_order() {
        let mut cache = LRUCache::new(3);
        let keys = vec![1, 2, 3];
        for key in &keys {
            cache.put(key.clone(), Box::new(|_| true)).0;
        }
        // 访问中间的元素，以改变其位置
        cache.get(2);
        cache.cmp_list(vec![2, 3, 1]);
    }

    // 测试缓存的遍历打印
    #[test]
    fn test_lru_cache_print() {
        let mut cache = LRUCache::new(3);
        let keys = vec![1, 2, 3];
        for key in &keys {
            cache.put(key.clone(), Box::new(|_| true)).0;
        }
        cache.print_list();
        // 这个测试主要是为了观察输出，实际上没有断言
    }
}
