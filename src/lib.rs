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
use config::Config;
use std::borrow::Cow;
use std::env;
use std::sync::mpsc;
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
    loop {
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
        log::trace!("Sleeping for {} seconds", WAIT_DURATION.as_secs());
        std::thread::sleep(WAIT_DURATION);
    }
}

/// Sleeps for `duration` unless a shutdown command comes in.
/// Returns `true` if the process should now exit
pub fn sleep_safe(duration: Duration, chan: &mpsc::Receiver<()>) -> bool {
    match chan.recv_timeout(duration) {
        Ok(_) => true,
        Err(err) => match err {
            mpsc::RecvTimeoutError::Disconnected => true,
            mpsc::RecvTimeoutError::Timeout => false,
        },
    }
}

pub fn logging_init() {
    let log_env = env_logger::Env::default().filter_or("LOG_LEVEL", "info");
    // log_env.filter(filter_env)
    let actual_logger = env_logger::Builder::from_env(log_env).build();
    let log_level = actual_logger.filter();
    let sentry_logger = sentry_log::SentryLogger::with_dest(actual_logger);

    log::set_boxed_logger(Box::new(sentry_logger)).expect("Unable to configure logging");
    log::set_max_level(log_level);
}

pub fn sentry_init(config: &Config) -> sentry::ClientInitGuard {
    let release = env::var("SENTRY_RELEASE")
        .or_else(|_| env::var("GIT_COMMIT"))
        .map_or(None, |val| Some(Cow::Owned(val)));

    let environment = env::var("SENTRY_ENVIRONMENT")
        .or_else(|_| env::var("SENTRY_ENV"))
        .unwrap_or("unconfigured".to_owned());

    sentry::init((
        config.sentry_dsn.clone(),
        sentry::ClientOptions {
            release,
            environment: Some(Cow::Owned(environment)),
            ..Default::default()
        },
    ))
}
