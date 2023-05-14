#![allow(dead_code)]
/*
 * Copyright 2023 Lexi Robinson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use crate::message::Message;
use redis::{from_redis_value, Commands, ConnectionLike, ErrorKind as RedisErrorKind, RedisResult};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

#[derive(Debug, PartialEq, Eq)]
pub enum HandlerFilter {
    AllMessages,
    SomeMessages(HashSet<String>),
}
pub type HandlerFilters = HashMap<String, HandlerFilter>;

pub fn get_handler_filters<C: ConnectionLike>(
    filter_key: &str,
    con: &mut C,
) -> RedisResult<HandlerFilters> {
    let res = con.hgetall(filter_key)?;
    let res: HashMap<String, String> = from_redis_value(&res)?;
    Ok(res
        .iter()
        .map(|(k, v)| {
            let v = v.trim();
            if v.is_empty() {
                return (k.to_owned(), HandlerFilter::AllMessages);
            }
            let filters = v.split(',').map(|v| v.trim().to_owned());
            (k.to_owned(), HandlerFilter::SomeMessages(filters.collect()))
        })
        .collect())
}

fn get_queues_for_message<'a>(
    message: &'a Message,
    handlers: &'a HandlerFilters,
) -> Vec<&'a String> {
    let only_for = message.only_for();
    if let Some(only_for) = only_for {
        return only_for
            .iter()
            .filter_map(|queue_name| {
                handlers
                    .contains_key(*queue_name)
                    .then_some(*queue_name)
                    .or_else(|| {
                        log::warn!(
                            "Message {} ({}) has 'only_for' containing unknown queue '{}'",
                            message.message_type,
                            message.uuid,
                            queue_name
                        );
                        None
                    })
            })
            .collect();
    };

    return handlers
        .iter()
        .filter_map(|(queue_name, filter)| match filter {
            HandlerFilter::AllMessages => Some(queue_name),
            HandlerFilter::SomeMessages(filter) => {
                filter.contains(&message.message_type).then_some(queue_name)
            }
        })
        .collect();
}

pub fn process_one<C: ConnectionLike>(
    queue_key: &str,
    timeout: usize,
    con: &mut C,
    handlers: &HandlerFilters,
) -> RedisResult<()> {
    let message: Message = from_redis_value(&con.brpop(queue_key, timeout)?)?;

    log::trace!(
        "Recieved {} message: {:?}",
        message.message_type,
        message.raw,
    );

    let queues = get_queues_for_message(&message, handlers);
    let size = queues.len();
    if size == 0 {
        log::trace!(
            "Message {} ({}) doesn't match any handlers",
            message.message_type,
            message.uuid
        );
        return Ok(());
    }

    let mut pipe = redis::Pipeline::with_capacity(size);

    for queue_name in queues {
        log::trace!(
            "Sending message {} ({}) to {}",
            message.message_type,
            message.uuid,
            queue_name
        );
        let mut message_to_send = message.raw.clone();
        message_to_send["only_for"] = json!(queue_name);
        pipe.lpush(queue_name, message_to_send.to_string()).ignore();
    }

    pipe.query(con)?;

    Ok(())
}

fn process_executing<C: ConnectionLike>(
    key: &str,
    retry_after: Duration,
    con: &mut C,
    pipe: &mut redis::Pipeline,
) -> RedisResult<()> {
    log::trace!("Checking message at {} to see if it's timed out", key);
    let res = from_redis_value::<Message>(&con.get(key)?);
    let message = match res {
        Ok(message) => message,
        Err(err) => match err.kind() {
            RedisErrorKind::Serialize | RedisErrorKind::TypeError => {
                log::warn!("The message at {} is invalid and has been deleted", key);
                pipe.del(key);
                return Ok(());
            }
            _ => return Err(err),
        },
    };
    log::trace!(
        "Found a {} message with uuid {}",
        message.message_type,
        message.uuid
    );
    let start_time = match message.start_time() {
        Some(v) => v,
        None => {
            log::warn!(
                "The {} ({}) message at {} doesn't have a valid start_time and has been deleted",
                message.message_type,
                message.uuid,
                key
            );
            pipe.del(key);
            return Ok(());
        }
    };

    log::trace!("Processing started at {}", start_time);
    let now = chrono::Utc::now();
    let grace_period = chrono::Duration::from_std(retry_after).unwrap();
    // Possibly this will break in 2038 but I think that'll be the least of our
    // worries if this code is still somehow in production
    if start_time.checked_add_signed(grace_period).unwrap() > now {
        // Still plenty of time to process the message
        log::trace!("Ignoring the message for now");
        return Ok(());
    }
    log::trace!("Message is too old! Retrying!");

    // Delete the message so we don't reprocess it
    pipe.del(key);

    if let Some(only_for) = message.only_for() {
        let mut message = message.raw.clone();
        // "//Override start_time" (??? - the service will do this itself)
        message["start_time"] = json!(now.to_rfc3339());
        let message = message.to_string();

        // There should only be one value in `only_for` but there's no harm if
        // it has more than one and it's less fuss to just iterate than attempt
        // to deal with the edge case
        for queue_name in only_for {
            pipe.lpush(queue_name, message.clone());
        }
    } else {
        // On the one hand, the `key` variable straight up has the queue name in
        // it which we could extract, but on the other hand "this should never
        // happen" since the broker sets this field on every message it sends
        // out so I guess it's fine to have this case and Sentry should alert us
        // if this warning starts going off
        log::warn!(
            "The {} ({}) message at {} doesn't have a valid 'only_for' field so we can't retry it",
            message.message_type,
            message.uuid,
            key
        );
    }

    Ok(())
}

fn check_executing_batch<C: ConnectionLike>(
    last_cursor: u64,
    retry_after: Duration,
    con: &mut C,
) -> RedisResult<u64> {
    let res = redis::cmd("SCAN")
        .arg(last_cursor)
        .arg("MATCH")
        .arg("q:*:executing:*")
        .arg("TYPE")
        .arg("string")
        .query(con)?;

    let (next_cursor, batch) = from_redis_value::<(u64, Vec<String>)>(&res)?;
    let batch_size = batch.len();
    log::trace!("Got {} keys with cursor {}", batch_size, next_cursor);

    if batch_size == 0 {
        log::trace!("All the keys were filtered out!");
        return Ok(next_cursor);
    }

    let mut pipe = redis::Pipeline::with_capacity(batch_size * 2);

    for key in batch {
        process_executing(&key, retry_after, con, &mut pipe)?;
    }

    if pipe.cmd_iter().count() > 0 {
        pipe.query(con)?;
    } else {
        log::trace!("No keys needed to be retried");
    }

    Ok(next_cursor)
}

pub fn check_for_retries<C: ConnectionLike>(retry_after: Duration, con: &mut C) -> RedisResult<()> {
    let mut cursor = 0;
    log::trace!("Searching for currently executing entries");
    loop {
        cursor = check_executing_batch(cursor, retry_after, con)?;
        if cursor == 0 {
            break;
        }
    }
    log::trace!("Done");
    Ok(())
}

#[cfg(test)]
mod test_get_handler_filters {
    use super::{get_handler_filters, HandlerFilter};
    use redis::Value as RedisValue;
    use redis_test::{IntoRedisValue, MockCmd, MockRedisConnection};

    fn v2<V: IntoRedisValue + Copy>(vs: Vec<V>) -> RedisValue {
        RedisValue::Bulk(vs.iter().map(|v| v.into_redis_value()).collect())
    }

    const FILTER_KEY: &str = "q:example";

    #[test]
    fn it_loads_filters() {
        let mut con = MockRedisConnection::new(vec![MockCmd::new(
            redis::cmd("HGETALL").arg(FILTER_KEY),
            Ok(v2(vec!["q:example", "example_type"])),
        )]);

        let res = get_handler_filters(FILTER_KEY, &mut con).unwrap();

        assert_eq!(res.len(), 1);
        assert!(res.contains_key("q:example"))
    }

    #[test]
    fn empty_strings_match_all() {
        let mut con = MockRedisConnection::new(vec![MockCmd::new(
            redis::cmd("HGETALL").arg(FILTER_KEY),
            Ok(v2(vec!["q:example", ""])),
        )]);

        let res = get_handler_filters(FILTER_KEY, &mut con).unwrap();

        assert!(matches!(res["q:example"], HandlerFilter::AllMessages));
    }

    #[test]
    fn single_handler() {
        let mut con = MockRedisConnection::new(vec![MockCmd::new(
            redis::cmd("HGETALL").arg(FILTER_KEY),
            Ok(v2(vec!["q:example", "example_type"])),
        )]);

        let res = get_handler_filters(FILTER_KEY, &mut con).unwrap();

        let example = &res["q:example"];
        assert!(matches!(example, HandlerFilter::SomeMessages(_)));

        let HandlerFilter::SomeMessages(handlers) = example else { panic!() };
        assert!(handlers.contains("example_type"));
    }

    #[test]
    fn multiple_handlers() {
        let mut con = MockRedisConnection::new(vec![MockCmd::new(
            redis::cmd("HGETALL").arg(FILTER_KEY),
            Ok(v2(vec!["q:example", "example_type1,example_type2"])),
        )]);

        let res = get_handler_filters(FILTER_KEY, &mut con).unwrap();

        let example = &res["q:example"];
        assert!(matches!(example, HandlerFilter::SomeMessages(_)));

        let HandlerFilter::SomeMessages(handlers) = example else { panic!() };
        assert!(handlers.contains("example_type1"));
        assert!(handlers.contains("example_type2"));
    }

    #[test]
    fn crazy_spaces() {
        let mut con = MockRedisConnection::new(vec![MockCmd::new(
            redis::cmd("HGETALL").arg(FILTER_KEY),
            Ok(v2(vec![
                "q:example1",
                "   example_type1 , example_type2  ",
                "q:example2",
                "   ",
            ])),
        )]);

        let res = get_handler_filters(FILTER_KEY, &mut con).unwrap();

        let example1 = &res["q:example1"];
        assert!(matches!(example1, HandlerFilter::SomeMessages(_)));

        let HandlerFilter::SomeMessages(handlers) = example1 else { panic!() };
        assert!(handlers.contains("example_type1"));
        assert!(handlers.contains("example_type2"));

        let example2 = &res["q:example2"];
        assert!(matches!(example2, HandlerFilter::AllMessages));
    }
}
