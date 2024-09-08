//! This module contains specialized versions of generic search algorithms that are used for B+Tree operations.
use std::cmp::Ordering;

#[doc(hidden)]
/// Recursively searches for the given needle and returns its index within the haystack.
/// `haystack` must be sorted in natural order of [U] prior to calling this function.
///
/// * `haystack` - A slice of [T] containing the elements that must be searched.
/// * `needle` - The needle that is searched for.
/// * `resolver` - A function that resolves a given element [&T] to a [&U].
///
/// Returns [Some(usize)] if a matching element has been found, [None] otherwise.
pub(crate) fn binary_search<T, U: Ord, Resolver>(
    haystack: &[T],
    needle: &U,
    resolver: Resolver,
) -> Option<usize>
where
    Resolver: Fn(&T) -> &U,
{
    if haystack.is_empty() {
        None
    } else {
        binary_search_internal(haystack, 0, haystack.len() - 1, needle, resolver)
    }
}

#[doc(hidden)]
/// Recursively searches for the given needle and returns its index within the haystack.
/// `haystack` must be sorted in natural order of [U] prior to calling this function.
///
/// * `haystack` - A slice of [T] containing the elements that must be searched.
/// * `low` - The lowest index to search.
/// * `high` - The highest index to search.
/// * `needle` - The needle that is searched for.
/// * `resolver` - A function that resolves the given [&T] to a [&U].
///
/// Returns [Some(usize)] if a matching element has been found, [None] otherwise.
fn binary_search_internal<T, U: Ord, Resolver>(
    haystack: &[T],
    low: usize,
    high: usize,
    needle: &U,
    resolver: Resolver,
) -> Option<usize>
where
    Resolver: Fn(&T) -> &U,
{
    if high >= low {
        let index = low + (high - low) / 2;
        let middle_element = resolver(&haystack[index]);

        match middle_element.cmp(needle) {
            Ordering::Equal => Some(index),
            Ordering::Less => binary_search_internal(haystack, index + 1, high, needle, resolver),
            Ordering::Greater => binary_search_internal(haystack, low, index - 1, needle, resolver),
        }
    } else {
        None
    }
}

#[doc(hidden)]
/// Recursively searches the greatest element that might equal but not exceed the given `needle`.
/// `haystack` must be sorted in the natural order of [U] prior to calling this function.
///
/// * `haystack` - A slice of [T] containing the elements that must be searched.
/// * `needle` - The needle that is searched for.
/// * `resolver` - A function that resolves a given element [&T] to a [&U].
///
/// Returns [Some(usize)] if a matching element has been found, [None] otherwise.
pub(crate) fn greatest_not_exceeding<T, U: Ord, Resolver>(
    haystack: &[T],
    needle: &U,
    resolver: Resolver,
) -> Option<usize>
where
    Resolver: Fn(&T) -> &U,
{
    if haystack.is_empty() {
        None
    } else {
        greatest_not_exceeding_internal(haystack, 0, haystack.len() - 1, needle, resolver)
    }
}

#[doc(hidden)]
/// Recursively searches the greatest element that might equal but not exceed the given `needle`.
/// `haystack` must be sorted in the natural order of [U] prior to calling this function.
///
/// * `haystack` - A slice of [T] containing the elements that must be searched.
/// * `low` - The lowest index to search.
/// * `high` - The highest index to search.
/// * `needle` - The needle that is searched for.
/// * `resolver` - A function that resolves the given [&T] to a [&U].
///
/// Returns [Some(usize)] if a matching element has been found, [None] otherwise.
fn greatest_not_exceeding_internal<T, U: Ord, Resolver>(
    haystack: &[T],
    low: usize,
    high: usize,
    needle: &U,
    resolver: Resolver,
) -> Option<usize>
where
    Resolver: Fn(&T) -> &U,
{
    let index = low + (high - low) / 2;
    let middle_element = resolver(&haystack[index]);

    if index == low && needle < middle_element {
        None
    } else {
        let is_candidate = middle_element <= needle;
        if is_candidate && (index == high || needle < resolver(&haystack[index + 1])) {
            Some(index)
        } else if is_candidate {
            greatest_not_exceeding_internal(haystack, index + 1, high, needle, resolver)
        } else {
            greatest_not_exceeding_internal(haystack, low, index, needle, resolver)
        }
    }
}

#[doc(hidden)]
/// Recursively searches the first element which is considered larger than `needle`.
/// `haystack` must be sorted in the natural order of [U] prior to calling this function.
///
/// <p>Note: `needle` does not need to reside in `haystack`, which allows for searching
/// sparse haystacks.
///
/// * `haystack` - A slice of [T] containing the elements that must be searched.
/// * `needle` - The needle that is searched for.
/// * `resolver` - A function that resolves a given element [&T] to a [&U].
///
/// Returns [Some(usize)] if a matching element has been found, [None] otherwise.
pub(crate) fn next_largest<T, U: Ord, Resolver>(
    haystack: &[T],
    needle: &U,
    resolver: Resolver,
) -> Option<usize>
where
    Resolver: Fn(&T) -> &U,
{
    if haystack.is_empty() {
        None
    } else {
        next_largest_internal(haystack, 0, haystack.len() - 1, needle, resolver)
    }
}

#[doc(hidden)]
/// Recursively searches the first element which is considered larger than `needle`.
/// `haystack` must be sorted in the natural order of [U] prior to calling this function.
///
/// <p>Note: `needle` does not need to reside in `haystack`, which allows for searching
/// sparse haystacks.
///
/// * `haystack` - A slice of [T] containing the elements that must be searched.
/// * `low` - The lowest index to search.
/// * `high` - The highest index to search.
/// * `needle` - The needle that is searched for.
/// * `resolver` - A function that resolves the given [&T] to a [&U].
///
/// Returns [Some(usize)] if a matching element has been found, [None] otherwise.
fn next_largest_internal<T, U: Ord, Resolver>(
    haystack: &[T],
    low: usize,
    high: usize,
    needle: &U,
    resolver: Resolver,
) -> Option<usize>
where
    Resolver: Fn(&T) -> &U,
{
    let index = low + (high - low) / 2;
    let middle_element = resolver(&haystack[index]);

    if low != high {
        if middle_element <= needle {
            next_largest_internal(haystack, index + 1, high, needle, resolver)
        } else {
            next_largest_internal(haystack, low, index, needle, resolver)
        }
    } else if needle < middle_element {
        Some(index)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct S {
        value: i32,
    }

    #[test]
    fn binary_search_empty_vec() {
        let vec = Vec::<S>::new();
        let needle = 3;
        assert_eq!(
            binary_search(&vec[..], &needle, |t: &S| -> &i32 { &t.value }),
            None
        );
    }

    #[test]
    fn binary_search_middle() {
        let vec = vec![
            S { value: 1 },
            S { value: 3 },
            S { value: 5 },
            S { value: 7 },
            S { value: 9 },
        ];

        // Set needle to an existing value.
        let needle: i32 = 5;
        assert_eq!(
            binary_search(&vec[..], &needle, |t: &S| -> &i32 { &t.value }),
            Some(2)
        );
    }

    #[test]
    fn binary_search_first() {
        let vec = vec![
            S { value: 1 },
            S { value: 3 },
            S { value: 5 },
            S { value: 7 },
            S { value: 9 },
        ];

        // Set needle to an existing value.
        let needle: i32 = 1;
        assert_eq!(
            binary_search(&vec[..], &needle, |t: &S| -> &i32 { &t.value }),
            Some(0)
        );
    }

    #[test]
    fn binary_search_last() {
        let vec = vec![
            S { value: 1 },
            S { value: 3 },
            S { value: 5 },
            S { value: 7 },
            S { value: 9 },
        ];

        // Set needle to an existing value.
        let needle: i32 = 9;
        assert_eq!(
            binary_search(&vec[..], &needle, |t: &S| -> &i32 { &t.value }),
            Some(4)
        );
    }

    #[test]
    fn binary_search_non_existing() {
        let vec = vec![
            S { value: 1 },
            S { value: 3 },
            S { value: 5 },
            S { value: 7 },
            S { value: 9 },
        ];

        // Set needle to a non-existing value.
        let needle: i32 = 11;
        assert_eq!(
            binary_search(&vec[..], &needle, |t: &S| -> &i32 { &t.value }),
            None
        );
    }

    #[test]
    fn greatest_not_exceeding_empty_vec() {
        let vec = Vec::<S>::new();
        let needle = 3;

        assert_eq!(
            greatest_not_exceeding(&vec[..], &needle, |t: &S| -> &i32 { &t.value }),
            None
        );
    }

    #[test]
    fn greatest_not_exceeding_middle() {
        let vec = vec![
            S { value: 1 },
            S { value: 3 },
            S { value: 5 },
            S { value: 7 },
            S { value: 9 },
        ];

        // Set needle to an existing value.
        let needle: i32 = 6;
        assert_eq!(
            greatest_not_exceeding(&vec[..], &needle, |t: &S| -> &i32 { &t.value }),
            Some(2)
        );
    }

    #[test]
    fn greatest_not_exceeding_first() {
        let vec = vec![
            S { value: 1 },
            S { value: 3 },
            S { value: 5 },
            S { value: 7 },
            S { value: 9 },
        ];

        // Set needle to an existing value.
        let needle: i32 = 1;
        assert_eq!(
            greatest_not_exceeding(&vec[..], &needle, |t: &S| -> &i32 { &t.value }),
            Some(0)
        );
    }

    #[test]
    fn greatest_not_exceeding_last() {
        let vec = vec![
            S { value: 1 },
            S { value: 3 },
            S { value: 5 },
            S { value: 7 },
            S { value: 9 },
        ];

        // Set needle to an existing value.
        let needle: i32 = 10;
        assert_eq!(
            greatest_not_exceeding(&vec[..], &needle, |t: &S| -> &i32 { &t.value }),
            Some(4)
        );
    }

    #[test]
    fn greatest_not_exceeding_non_existing() {
        let vec = vec![
            S { value: 1 },
            S { value: 3 },
            S { value: 5 },
            S { value: 7 },
            S { value: 9 },
        ];

        // Set needle to an existing value.
        let needle: i32 = 0;
        assert_eq!(
            greatest_not_exceeding(&vec[..], &needle, |t: &S| -> &i32 { &t.value }),
            None
        );
    }

    #[test]
    fn next_largest_empty_ved() {
        let vec = Vec::<S>::new();

        let needle = 3;
        assert_eq!(
            next_largest(&vec[..], &needle, |t: &S| -> &i32 { &t.value }),
            None
        );
    }

    #[test]
    fn next_largest_middle() {
        let vec = vec![
            S { value: 1 },
            S { value: 3 },
            S { value: 5 },
            S { value: 7 },
            S { value: 9 },
        ];

        // Set needle to an existing value.
        let needle: i32 = 3;
        assert_eq!(
            next_largest(&vec[..], &needle, |t: &S| -> &i32 { &t.value }),
            Some(2)
        );
    }

    #[test]
    fn next_largest_first() {
        let vec = vec![
            S { value: 1 },
            S { value: 3 },
            S { value: 5 },
            S { value: 7 },
            S { value: 9 },
        ];

        // Set needle to an existing value.
        let needle: i32 = 0;
        assert_eq!(
            next_largest(&vec[..], &needle, |t: &S| -> &i32 { &t.value }),
            Some(0)
        );
    }

    #[test]
    fn next_largest_last() {
        let vec = vec![
            S { value: 1 },
            S { value: 3 },
            S { value: 5 },
            S { value: 7 },
            S { value: 9 },
        ];

        // Set needle to an existing value.
        let needle: i32 = 8;
        assert_eq!(
            next_largest(&vec[..], &needle, |t: &S| -> &i32 { &t.value }),
            Some(4)
        );
    }

    #[test]
    fn next_largest_non_existing() {
        let vec = vec![
            S { value: 1 },
            S { value: 3 },
            S { value: 5 },
            S { value: 7 },
            S { value: 9 },
        ];

        // Set needle to an existing value.
        let needle: i32 = 9;
        assert_eq!(
            next_largest(&vec[..], &needle, |t: &S| -> &i32 { &t.value }),
            None
        );
    }
}
