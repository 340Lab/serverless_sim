use rand::Rng;

pub fn rand_f(begin: f32, end: f32) -> f32 {
    let a = rand::thread_rng().gen_range(begin..end);
    a
}
pub fn rand_i(begin: usize, end: usize) -> usize {
    let a = rand::thread_rng().gen_range(begin..end);
    a
}
