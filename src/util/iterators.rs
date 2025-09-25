// Custom iterator utilities
//
// (c) 2025 Cleafy S.p.A.
// Author: Enzo Lombardi
//
use crate::kafka::storage::RecordBatchEntry;
use kafka_protocol::messages::TopicName;
use std::cell::RefCell;
use std::iter::Peekable;
use std::rc::Rc;
use uuid::Uuid;

pub struct GroupedIterator<I>
where
    I: Iterator<Item = (Uuid, TopicName, i32, RecordBatchEntry, i64)>,
{
    iter: Rc<RefCell<Peekable<I>>>,
}

pub struct GroupValueIterator<I>
where
    I: Iterator<Item = (Uuid, TopicName, i32, RecordBatchEntry, i64)>,
{
    iter: Rc<RefCell<Peekable<I>>>,
    key: Uuid,
}

impl<I> GroupedIterator<I>
where
    I: Iterator<Item = (Uuid, TopicName, i32, RecordBatchEntry, i64)>,
{
    pub fn new(iter: I) -> Self {
        Self {
            iter: Rc::new(RefCell::new(iter.peekable())),
        }
    }
}

impl<I> Iterator for GroupValueIterator<I>
where
    I: Iterator<Item = (Uuid, TopicName, i32, RecordBatchEntry, i64)>,
{
    type Item = (i32, RecordBatchEntry, i64);

    fn next(&mut self) -> Option<Self::Item> {
        let mut iter = self.iter.borrow_mut();

        if let Some((next_key, _, _, _, _)) = iter.peek() {
            if *next_key == self.key {
                let v = iter.next().unwrap(); // Consume the item
                return Some((v.2, v.3, v.4)); // Return the value and cloned record
            }
        }
        None
    }
}

impl<I> Iterator for GroupedIterator<I>
where
    I: Iterator<Item = (Uuid, TopicName, i32, RecordBatchEntry, i64)>,
{
    type Item = (Uuid, TopicName, GroupValueIterator<I>);

    fn next(&mut self) -> Option<Self::Item> {
        let mut iter = self.iter.borrow_mut();
        if let Some((key, topic_name, _, _, _)) = iter.peek() {
            let value_iterator = GroupValueIterator {
                iter: Rc::clone(&self.iter),
                key: *key,
            };
            Some((*key, topic_name.clone(), value_iterator))
        } else {
            None
        }
    }
}
