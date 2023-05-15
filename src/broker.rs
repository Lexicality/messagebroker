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
use redis::{from_redis_value, Commands, ConnectionLike, RedisResult, Value as RedisValue};
use serde_json::json;
use std::collections::{HashMap, HashSet};

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
    log::debug!("Fetching handlers");
    let res = con.hgetall(filter_key)?;
    let res: HashMap<String, String> = from_redis_value(&res)?;
    log::trace!("Found handlers {:?}", res);
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
    let res: RedisValue = con.brpop(queue_key, timeout)?;
    if res == RedisValue::Nil {
        // BRPOP returns nil on timeout so immediately return control upstream
        return Ok(());
    }
    let (_, message) = from_redis_value::<(String, Message)>(&res)?;

    log::trace!(
        "Recieved {} message: {:?}",
        message.message_type,
        message.raw,
    );

    let queues = get_queues_for_message(&message, handlers);
    let size = queues.len();
    if size == 0 {
        log::debug!(
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
