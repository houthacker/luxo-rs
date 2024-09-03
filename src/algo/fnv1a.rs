/// FNV1-a is a fast, non-cryptographic hashing algorithm.
///
/// A description of the internals of the FNV1a hash can be found on [Wikipedia](https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function).
pub(crate) struct FNV1a {
    /// The current hash value.
    state: u64,
}

pub(crate) trait FNV1aIterator {
    fn fnv1a_iterate<'a>(&self, algorithm: &'a mut FNV1a) -> &'a mut FNV1a;
}

impl FNV1a {
    /// The initial hash value.
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;

    /// The value to multiply with when hashing a given value.
    const FNV_PRIME: u64 = 0x100000001b3;

    /// Creates a new [`FNV1a`] that is initialized with the offset basis.
    pub fn create_new() -> Self {
        Self::create_init(Self::OFFSET_BASIS)
    }

    /// Creates a new [`FNV1a`] instance that is initialized with the given `state`.
    pub fn create_init(state: u64) -> Self {
        Self { state }
    }

    /// Returns the current hashing state.
    pub fn state(&self) -> u64 {
        self.state
    }

    /// Adds the given `byte` to the current hash state. An [u8] is the smallest unit
    /// that can be added to this hash, acts as a building block for calculating the FNV1a-hash
    /// of larger-sized types like slice of bytes.
    pub fn iterate(&mut self, byte: &u8) {
        self.state ^= u64::from(*byte);
        self.state = (self.state as u128 * FNV1a::FNV_PRIME as u128) as u64;
    }
}

impl FNV1aIterator for bool {
    /// Appends the boolean value to the state of `algorithm`.
    fn fnv1a_iterate<'a>(&self, algorithm: &'a mut FNV1a) -> &'a mut FNV1a {
        // These hard-coded values are copied from Java's Boolean.hashcode()
        match *self {
            true => 1231.fnv1a_iterate(algorithm),
            false => 1237.fnv1a_iterate(algorithm),
        }
    }
}

impl FNV1aIterator for &[u8] {
    /// Appends all bytes from this slice to the state of `algorithm`.
    fn fnv1a_iterate<'a>(&self, algorithm: &'a mut FNV1a) -> &'a mut FNV1a {
        for b in self.iter() {
            algorithm.iterate(b);
        }

        algorithm
    }
}

#[doc(hidden)]
// This is a private macro to prevent code duplication in the `FNV1aIterator` implementations.
macro_rules! fnv1a_iterate {
    () => {
        /// Appends the little-endian bytes from this value to the state of `algorithm`.
        fn fnv1a_iterate<'a>(&self, algorithm: &'a mut FNV1a) -> &'a mut FNV1a {
            for b in self.to_le_bytes() {
                algorithm.iterate(&b);
            }

            algorithm
        }
    };
}

impl FNV1aIterator for i32 {
    fnv1a_iterate!();
}

impl FNV1aIterator for u32 {
    fnv1a_iterate!();
}

impl FNV1aIterator for i64 {
    fnv1a_iterate!();
}

impl FNV1aIterator for u64 {
    fnv1a_iterate!();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_new() {
        let instance = FNV1a::create_new();
        assert_eq!(
            instance.state(),
            0xcbf29ce484222325,
            "Expect a new FNV1a instance to be initialized with FNV1a::OFFSET_BASIS"
        );
    }

    #[test]
    fn create_init() {
        let instance = FNV1a::create_init(1337);
        assert_eq!(instance.state(), 1337);
    }

    #[test]
    fn iterate_i32() {
        let mut instance = FNV1a::create_new();
        assert_eq!(
            1337i32.fnv1a_iterate(&mut instance).state(),
            0x9358f934873276db
        );
    }

    #[test]
    fn iterate_u32() {
        let mut instance = FNV1a::create_new();
        assert_eq!(
            1337u32.fnv1a_iterate(&mut instance).state(),
            0x9358f934873276db
        );
    }

    #[test]
    fn iterate_i64() {
        let mut instance = FNV1a::create_new();
        assert_eq!(
            1337i64.fnv1a_iterate(&mut instance).state(),
            0x41ff8641d035260b
        );
    }

    #[test]
    fn iterate_u64() {
        let mut instance = FNV1a::create_new();
        assert_eq!(
            1337u64.fnv1a_iterate(&mut instance).state(),
            0x41ff8641d035260b
        );
    }

    #[test]
    fn iterate_bool() {
        let b = false;
        let mut instance = FNV1a::create_new();

        assert_eq!(b.fnv1a_iterate(&mut instance).state(), 0x4b91cd1c0e0a959c);
    }

    #[test]
    fn iterate_bytes() {
        let bytes = [b'd', b'e', b'a', b'd', b'b', b'e', b'e', b'f'];
        let mut instance = FNV1a::create_new();

        assert_eq!(
            (&bytes[..]).fnv1a_iterate(&mut instance).state(),
            0xcd4f3b6f56d93515
        );
    }
}
