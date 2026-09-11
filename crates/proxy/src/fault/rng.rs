use rand::{RngExt, SeedableRng};
use rand_chacha::ChaCha8Rng;
pub struct FaultRng {
    rng: std::sync::Mutex<ChaCha8Rng>,
}

impl FaultRng {
    pub fn from_seed(seed: u64) -> Self {
        FaultRng {
            rng: std::sync::Mutex::new(ChaCha8Rng::seed_from_u64(seed)),
        }
    }

    pub fn next(&self) -> f64 {
        self.rng.lock().unwrap().random::<f64>()
    }

    pub fn range_ms(&self, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            return lo;
        }
        self.rng.lock().unwrap().random_range(lo..hi)
    }
    pub fn chance(&self, p: f64) -> bool {
        if p >= 1.0 {
            return true;
        }
        if p <= 0.0 {
            return false;
        }
        self.next() < p
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn is_same_stream() {
        let n: u8 = 255;

        let seed = 42;
        let fault_rng = FaultRng::from_seed(seed);
        let fault_test = FaultRng::from_seed(seed);

        for _ in 1..n {
            if fault_rng.next() != fault_test.next() {
                panic!("Same seed generated different results");
            }
        }
    }

    #[test]
    fn different_seed_gen() {
        let n: u8 = 255;

        let fault_rng = FaultRng::from_seed(10);
        let fault_test = FaultRng::from_seed(42);

        let mut different = false;
        for _ in 1..n {
            different |= fault_rng.next() != fault_test.next();
            if different {
                return;
            }
        }
        panic!("Different seeds generated the same results");
    }
}
