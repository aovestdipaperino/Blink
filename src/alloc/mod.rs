// Memory tracking allocator for Rust
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
use mimalloc::MiMalloc;
use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicUsize, Ordering};

/// A memory tracking allocator that wraps the system allocator
/// and keeps track of current and maximum allocated memory.
pub struct TrackingAllocator {
    /// The underlying MiMalloc allocator
    inner: MiMalloc,
    /// Current allocated memory in bytes
    current_allocated: AtomicUsize,
    /// Maximum allocated memory in bytes (high water mark)
    max_allocated: AtomicUsize,
    /// Total number of allocations made
    total_allocations: AtomicUsize,
    /// Total number of deallocations made
    total_deallocations: AtomicUsize,
}

impl TrackingAllocator {
    /// Create a new tracking allocator
    pub const fn new() -> Self {
        Self {
            inner: MiMalloc,
            current_allocated: AtomicUsize::new(0),
            max_allocated: AtomicUsize::new(0),
            total_allocations: AtomicUsize::new(0),
            total_deallocations: AtomicUsize::new(0),
        }
    }

    /// Get the current allocated memory in bytes
    pub fn current_allocated(&self) -> usize {
        self.current_allocated.load(Ordering::Relaxed)
    }

    /// Get the maximum allocated memory in bytes (high water mark)
    pub fn max_allocated(&self) -> usize {
        self.max_allocated.load(Ordering::Relaxed)
    }

    /// Update current allocation and max if needed
    fn update_allocation(&self, size: usize) {
        // Increment current allocated memory
        let new_current = self.current_allocated.fetch_add(size, Ordering::Relaxed) + size;

        // Update max allocated if necessary
        let mut current_max = self.max_allocated.load(Ordering::Relaxed);
        while new_current > current_max {
            match self.max_allocated.compare_exchange_weak(
                current_max,
                new_current,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual_max) => current_max = actual_max,
            }
        }

        // Increment allocation counter
        self.total_allocations.fetch_add(1, Ordering::Relaxed);
    }

    /// Update deallocation
    fn update_deallocation(&self, size: usize) {
        // Decrement current allocated memory
        self.current_allocated.fetch_sub(size, Ordering::Relaxed);

        // Increment deallocation counter
        self.total_deallocations.fetch_add(1, Ordering::Relaxed);
    }
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = self.inner.alloc(layout);
        if !ptr.is_null() {
            self.update_allocation(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.inner.dealloc(ptr, layout);
        self.update_deallocation(layout.size());
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = self.inner.alloc_zeroed(layout);
        if !ptr.is_null() {
            self.update_allocation(layout.size());
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = self.inner.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            // Handle reallocation: remove old size, add new size
            self.update_deallocation(layout.size());
            self.update_allocation(new_size);
        }
        new_ptr
    }
}

impl Default for TrackingAllocator {
    fn default() -> Self {
        Self::new()
    }
}

// Global instance of the tracking allocator
#[global_allocator]
pub static GLOBAL_ALLOCATOR: TrackingAllocator = TrackingAllocator::new();

/// Get a reference to the global tracking allocator
pub fn global_allocator() -> &'static TrackingAllocator {
    &GLOBAL_ALLOCATOR
}
