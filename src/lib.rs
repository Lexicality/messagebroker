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
use redis::ConnectionLike;
use std::time::Duration;

const WAIT_DURATION: Duration = Duration::from_secs(10);

pub mod broker;
pub mod config;
mod message;

pub fn wait_for_redis<C: ConnectionLike>(con: &mut C) {
    log::trace!("Waiting for Redis to be available");
    loop {
        let res = redis::cmd("PING").query::<String>(con);
        match res {
            Ok(pong) => {
                if pong == "PONG" {
                    log::trace!("Connected!");
                    return;
                }
                log::trace!("Got unexpected pong response: {}", pong);
            }
            Err(err) => {
                log::trace!("Got error connecting to redis: {}", err);
            }
        }
        log::trace!("Sleeping for {} seconds", WAIT_DURATION.as_secs());
        std::thread::sleep(WAIT_DURATION);
    }
}
