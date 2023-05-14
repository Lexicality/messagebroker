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
use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const WAIT_DURATION: Duration = Duration::from_secs(10);

pub mod broker;
pub mod config;
mod message;

pub fn get_and_validate_connection(
    client: &mut redis::Client,
    name: &str,
) -> redis::RedisResult<redis::Connection> {
    log::debug!("Attempting to connect");

    let mut con = client.get_connection_with_timeout(CONNECT_TIMEOUT)?;

    log::debug!("Connection successful, setting name");

    // This will fail if we're not actually connected to the server
    redis::cmd("CLIENT")
        .arg("SETNAME")
        .arg(name.replace(' ', "-"))
        .query(&mut con)?;

    log::debug!("Connected!");

    Ok(con)
}

pub fn get_redis_connection(client: &mut redis::Client, name: &str) -> redis::Connection {
    log::info!(
        "Attempting to connect to Redis with info {:?}",
        client.get_connection_info()
    );
    let res = get_and_validate_connection(client, name);
    match res {
        Ok(con) => return con,
        Err(err) => {
            // An invalid client config is non-recoverable
            if err.kind() == redis::ErrorKind::InvalidClientConfig {
                panic!("Could not connect to redis: {}", err);
            }
            log::info!("Failed to connect to redis: {}", err);
        }
    }
    log::info!("Waiting for Redis to be available");
    loop {
        log::trace!("Sleeping for {} seconds", WAIT_DURATION.as_secs());
        std::thread::sleep(WAIT_DURATION);
        let res = get_and_validate_connection(client, name);
        match res {
            Ok(con) => return con,
            Err(err) => {
                // Working on the assumption that we can't get an invalid config
                // error the second time around so it's safe to just log and
                // sleep forever
                log::info!("Failed to connect to redis: {}", err);
            }
        }
    }
}
