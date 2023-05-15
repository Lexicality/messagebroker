/**
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
use std::time::Duration;

/// Loads and processes a single executing key to see if it needs to be retried or
/// possibly deleted
///
/// Commands for retries and deletions will be added to the pipe rather than executed
/// immediately on the connection
fn process_executing<C: ConnectionLike>(
    key: &str,
    retry_after: Duration,
    con: &mut C,
    pipe: &mut redis::Pipeline,
) -> RedisResult<()> {
    log::debug!("Checking message at {} to see if it's timed out", key);

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
        log::debug!("Ignoring the message for now");
        return Ok(());
    }

    log::debug!("Message is too old! Retrying!");

    // Delete the message so we don't reprocess it
    pipe.del(key);

    // Nominally speaking, every message that's being executed should have an `only_for`
    // set by the broker before it even reaches anything that can execute it, but we're
    // working with unstructured JSON here so there's always a chance for things to go
    // horribly horribly wrong
    if let Some(only_for) = message.only_for() {
        let mut message = message.raw.clone();
        // "//Override start_time" (??? - the service will do this itself)
        message["start_time"] = json!(now.to_rfc3339());
        let message = message.to_string();

        // There should only be one value in `only_for` but there's no harm if
        // it has more than one and it's less fuss to just iterate than attempt
        // to deal with the edge case
        for queue_name in only_for {
            log::trace!("Sending message to queue {}", queue_name);
            pipe.lpush(queue_name, message.clone());
        }
    } else {
        // On the one hand, the `key` variable straight up has the queue name in it
        // which we could extract, but on the other hand "this should never happen" so I
        // guess it's fine to have this case and Sentry should alert us if this warning
        // starts going off
        log::warn!(
            "The {} ({}) message at {} doesn't have a valid 'only_for' field so we can't retry it",
            message.message_type,
            message.uuid,
            key
        );
    }

    Ok(())
}

/// Handles a single bulk reply from SCAN
///
/// Specifically, all the actions for the keys in this reply are processed in a single
/// pipeline at the end to ensure we get some level of bulk performance but without the
/// risk of doing too much bulk in one go and having Redis get sad at us
///
/// This is its own helper function purely to make the exit conditions for the loop
/// easier to manage
fn check_executing_batch<C: ConnectionLike>(
    last_cursor: u64,
    retry_after: Duration,
    con: &mut C,
) -> RedisResult<u64> {
    let res = redis::cmd("SCAN")
        .arg(last_cursor)
        .arg("MATCH")
        .arg("q:*:executing:*")
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

    // Strictly speaking this should short-circuit the evaluation by itself and not
    // actually talk to Redis but by doing it like this we get a nice log line :)
    if pipe.cmd_iter().count() > 0 {
        pipe.query(con)?;
    } else {
        log::trace!("No keys needed to be retried");
    }

    Ok(next_cursor)
}

/// Scans the entire redis keyspace for currently executing keys and processes all of them.
///
/// A failure here doesn't necessarily mean no keys were processed as they're done in
/// batches and the very last batch could fail while all the previous ones suceeded.
///
/// This function is safe to call again immediately if needed.
pub fn check_for_retries<C: ConnectionLike>(retry_after: Duration, con: &mut C) -> RedisResult<()> {
    let mut cursor = 0;
    log::debug!("Searching for currently executing entries");
    loop {
        cursor = check_executing_batch(cursor, retry_after, con)?;
        if cursor == 0 {
            break;
        }
    }
    log::debug!("Done");
    Ok(())
}
