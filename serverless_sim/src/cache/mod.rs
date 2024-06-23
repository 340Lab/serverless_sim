pub mod lru;
pub mod no_evict;

use std::fmt::Debug;
use std::hash::Hash;

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
