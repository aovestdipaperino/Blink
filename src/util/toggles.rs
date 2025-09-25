// Feature toggle utilities
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
use once_cell::sync::Lazy;
use std::sync::RwLock;

pub(crate) struct Toggles {
    pub enable_purge: bool,
}


static TOGGLES: Lazy<RwLock<Toggles>> = Lazy::new(|| RwLock::new(Toggles { enable_purge: true }));


impl Toggles {
    pub(crate) fn get_enable_purge() -> bool {
        TOGGLES.read().unwrap().enable_purge
    }

    pub(crate) fn set_enable_purge(enable_purge: bool) {
        TOGGLES.write().unwrap().enable_purge = enable_purge;
    }
}
