use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;

use super::InstanceCachePolicy;

pub struct NoEvict<Payload: Eq + Hash + Clone + Debug + Send> {
    _a: PhantomData<Payload>,
}

impl<Payload: Eq + Hash + Clone + Debug + Send> NoEvict<Payload> {
    pub fn new() -> Self {
        NoEvict { _a: PhantomData }
    }
}

impl<Payload: Eq + Hash + Clone + Debug + Send> InstanceCachePolicy<Payload> for NoEvict<Payload> {
    fn get(&mut self, key: Payload) -> Option<Payload> {
        Some(key)
    }

    fn put(
        &mut self,
        _key: Payload,
        _can_be_evict: Box<dyn FnMut(&Payload) -> bool>,
    ) -> (Option<Payload>, bool) {
        (None, true)
    }

    fn remove_all(&mut self, key: &Payload) -> bool {
        true
    }
}
