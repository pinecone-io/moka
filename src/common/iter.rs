use std::{hash::Hash, sync::Arc};

use crate::common::concurrent::{arc::MiniArc, ValueEntry};

/// An item collected by scanning a segment of the concurrent hash table.
pub(crate) enum ScannedEntry<K, V> {
    Key(Arc<K>),
    Entry {
        key: Arc<K>,
        entry: MiniArc<ValueEntry<K, V>>,
    },
}

// This trait is implemented by `sync::BaseCache` and `sync::Cache`.
pub(crate) trait ScanningGet<K, V> {
    /// Returns the number of segments in the concurrent hash table.
    fn num_cht_segments(&self) -> usize;

    /// Returns a _clone_ of the value corresponding to the key.
    ///
    /// Unlike the `get` method of cache, this method is not considered a cache read
    /// operation, so it does not update the historic popularity estimator or reset
    /// the idle timer for the key.
    fn scanning_get(&self, key: &Arc<K>) -> Option<V>;

    /// Returns a vec of keys in a specified segment of the concurrent hash table.
    fn keys(&self, cht_segment: usize) -> Option<Vec<Arc<K>>>;

    /// Returns a vec of scanned entries in a specified segment of the concurrent hash
    /// table.
    ///
    /// The default implementation returns the keys only, so each key is looked up
    /// with `scanning_get` when it is yielded.
    fn scanned_entries(&self, cht_segment: usize) -> Option<Vec<ScannedEntry<K, V>>> {
        self.keys(cht_segment)
            .map(|keys| keys.into_iter().map(ScannedEntry::Key).collect())
    }

    /// Returns a _clone_ of the value of a scanned entry.
    ///
    /// If the entry has been removed or replaced since the scan, this method looks up
    /// the key with `scanning_get` instead. Like `scanning_get`, this method is not
    /// considered a cache read operation.
    fn scanned_value(&self, key: &Arc<K>, _entry: &MiniArc<ValueEntry<K, V>>) -> Option<V> {
        self.scanning_get(key)
    }
}

/// Iterator visiting all key-value pairs in a cache in arbitrary order.
///
/// Call [`Cache::iter`](./struct.Cache.html#method.iter) method to obtain an `Iter`.
pub struct Iter<'i, K, V> {
    entries: Option<Vec<ScannedEntry<K, V>>>,
    cache_segments: Box<[&'i dyn ScanningGet<K, V>]>,
    num_cht_segments: usize,
    cache_seg_index: usize,
    cht_seg_index: usize,
    is_done: bool,
}

impl<'i, K, V> Iter<'i, K, V> {
    pub(crate) fn with_single_cache_segment(
        cache: &'i dyn ScanningGet<K, V>,
        num_cht_segments: usize,
    ) -> Self {
        Self {
            entries: None,
            cache_segments: Box::new([cache]),
            num_cht_segments,
            cache_seg_index: 0,
            cht_seg_index: 0,
            is_done: false,
        }
    }

    #[cfg(feature = "sync")]
    pub(crate) fn with_multiple_cache_segments(
        cache_segments: Box<[&'i dyn ScanningGet<K, V>]>,
        num_cht_segments: usize,
    ) -> Self {
        Self {
            entries: None,
            cache_segments,
            num_cht_segments,
            cache_seg_index: 0,
            cht_seg_index: 0,
            is_done: false,
        }
    }
}

impl<K, V> Iterator for Iter<'_, K, V>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    type Item = (Arc<K>, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_done {
            return None;
        }

        while let Some(scanned) = self.next_scanned() {
            let item = match scanned {
                ScannedEntry::Key(key) => self.cache().scanning_get(&key).map(|v| (key, v)),
                ScannedEntry::Entry { key, entry } => {
                    self.cache().scanned_value(&key, &entry).map(|v| (key, v))
                }
            };
            if item.is_some() {
                return item;
            }
        }

        self.is_done = true;
        None
    }
}

impl<'i, K, V> Iter<'i, K, V>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn cache(&self) -> &'i dyn ScanningGet<K, V> {
        self.cache_segments[self.cache_seg_index]
    }

    fn next_scanned(&mut self) -> Option<ScannedEntry<K, V>> {
        while let Some(entries) = self.current_entries() {
            if let scanned @ Some(_) = entries.pop() {
                return scanned;
            }
        }
        None
    }

    fn current_entries(&mut self) -> Option<&mut Vec<ScannedEntry<K, V>>> {
        // If entries is none or some but empty, try to get next entries.
        while self.entries.as_ref().map_or(true, Vec::is_empty) {
            // Adjust indices.
            if self.cht_seg_index >= self.num_cht_segments {
                self.cache_seg_index += 1;
                self.cht_seg_index = 0;
                if self.cache_seg_index >= self.cache_segments.len() {
                    // No more cache segments left.
                    return None;
                }
            }

            let cache_segment = self.cache_segments[self.cache_seg_index];
            self.entries = cache_segment.scanned_entries(self.cht_seg_index);
            self.num_cht_segments = cache_segment.num_cht_segments();

            self.cht_seg_index += 1;
        }

        self.entries.as_mut()
    }
}

// Clippy beta 0.1.83 (f41c7ed9889 2024-10-31) warns about unused lifetimes on 'a.
// This seems a false positive. The lifetimes are used in the Send and Sync impls.
// Let's suppress the warning.
// https://rust-lang.github.io/rust-clippy/master/index.html#extra_unused_lifetimes
#[allow(clippy::extra_unused_lifetimes)]
unsafe impl<'a, K, V> Send for Iter<'_, K, V>
where
    K: 'a + Eq + Hash + Send,
    V: 'a + Send,
{
}

// Clippy beta 0.1.83 (f41c7ed9889 2024-10-31) warns about unused lifetimes on 'a.
// This seems a false positive. The lifetimes are used in the Send and Sync impls.
// Let's suppress the warning.
// https://rust-lang.github.io/rust-clippy/master/index.html#extra_unused_lifetimes
#[allow(clippy::extra_unused_lifetimes)]
unsafe impl<'a, K, V> Sync for Iter<'_, K, V>
where
    K: 'a + Eq + Hash + Sync,
    V: 'a + Sync,
{
}
