use sha2::{Digest, Sha256};

pub fn calculate_hash(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

pub fn verify_hash(data: &[u8], hash: &[u8; 32]) -> bool {
    let calculated_hash = calculate_hash(data);
    &calculated_hash == hash
}

