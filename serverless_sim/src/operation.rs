use std::fmt;

enum OperationRecord {
    // from to
    Scale(usize, usize),
    // req fn on node
    // Schedule()
}

impl fmt::Debug for OperationRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OperationRecord::Scale(from, to) => write!(f, "Scale {} -> {}", from, to),
        }
    }
}
