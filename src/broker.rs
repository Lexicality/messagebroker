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
use redis::{from_redis_value, Commands, ConnectionLike, RedisResult};
use serde_json::{json, Value as JSONValue};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

const BROKER_QUEUE_KEY: &str = "q:broker";
const FILTER_KEY: &str = "q:list";
const POP_TIMEOUT: usize = Duration::from_secs(10).as_secs() as usize;

pub enum HandlerFilter {
    AllMessages,
    SomeMessages(HashSet<String>),
}
pub type HandlerFilters = HashMap<String, HandlerFilter>;

pub fn get_handler_filters<C: ConnectionLike>(con: &mut C) -> RedisResult<HandlerFilters> {
    let res = con.hgetall(FILTER_KEY)?;
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
) -> Box<dyn Iterator<Item = &'a String> + 'a> {
    let only_for = message.get("only_for");
    if let Some(only_for) = only_for {
        match only_for {
            JSONValue::String(queue_name) => {
                log::trace!("Message specifies it's only for {}", queue_name);
                if handlers.contains_key(queue_name) {
                    return Box::new(std::iter::once(queue_name));
                }
                log::warn!(
                    "Message {} ({}) has 'only_for' set to unknown queue '{}'",
                    message.message_type,
                    message.uuid,
                    queue_name
                );
                return Box::new(std::iter::empty());
            }
            JSONValue::Array(only_for) => {
                log::trace!("Message specifies it's only for {:?}", only_for);
                return Box::new(only_for.iter().filter_map(|queue_name| {
                    match queue_name {
                        JSONValue::String(queue_name) => handlers
                            .contains_key(queue_name)
                            .then_some(queue_name)
                            .or_else(|| {
                                log::warn!(
                                    "Message {} ({}) has 'only_for' containing unknown queue '{}'",
                                    message.message_type,
                                    message.uuid,
                                    queue_name
                                );
                                None
                            }),
                        _ => {
                            log::warn!(
                                "Message {} ({}) has 'only_for' containing non-string value: '{:?}'",
                                message.message_type,
                                message.uuid,
                                queue_name
                            );
                            None
                        }
                    }
                }));
            }
            _ => {
                log::warn!(
                    "Message {} ({}) has invalid 'only_for' field: '{:?}'",
                    message.message_type,
                    message.uuid,
                    only_for
                );
            }
        }
    }

    return Box::new(
        handlers
            .iter()
            .filter_map(|(queue_name, filter)| match filter {
                HandlerFilter::AllMessages => Some(queue_name),
                HandlerFilter::SomeMessages(filter) => {
                    filter.contains(&message.message_type).then_some(queue_name)
                }
            }),
    );
}

pub fn process_one<C: ConnectionLike>(con: &mut C, handlers: &HandlerFilters) -> RedisResult<()> {
    let message: Message = from_redis_value(&con.brpop(BROKER_QUEUE_KEY, POP_TIMEOUT)?)?;

    log::trace!(
        "Recieved {} message: {:?}",
        message.message_type,
        message.raw,
    );

    let queues = get_queues_for_message(&message, handlers);
    let size = queues.size_hint();

    let mut pipe = redis::Pipeline::with_capacity(size.1.unwrap_or(size.0));

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

    if pipe.cmd_iter().count() == 0 {
        log::trace!(
            "Message {} ({}) doesn't match any handlers",
            message.message_type,
            message.uuid
        );
        return Ok(());
    }

    pipe.query(con)?;

    Ok(())
}
