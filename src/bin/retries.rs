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

use gethostname::gethostname;
use messagebroker::broker::check_for_retries;
use messagebroker::config::Config;
use messagebroker::{config::get_config, get_redis_connection};
use redis::{Client, RedisResult};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, RecvTimeoutError};

fn do_retry_check(client: &mut Client, config: &Config, client_name: &str) -> RedisResult<()> {
    // This check happens so infrequently it's more efficient to start a new
    // connection every time
    let mut con = get_redis_connection(client, client_name);
    check_for_retries(config.retry_messages_after, &mut con)
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let (tx, rx) = channel();
    let already_killed = AtomicBool::new(false);
    ctrlc::set_handler(move || {
        log::info!("Recieved SIGTERM!");
        if already_killed.swap(true, Ordering::Relaxed) {
            log::error!("Abort!");
            std::process::exit(1);
        }
        tx.send(()).expect("Tried to announce sigterm")
    })
    .expect("Error setting Ctrl-C handler");

    let hostname = gethostname().to_string_lossy().to_lowercase();
    let client_name = format!("broker:retry_handler:{}", hostname);

    let config = get_config();
    let mut client = redis::Client::open(config.redis_url.clone())
        .expect("The REDIS_URL config value must be correct");

    do_retry_check(&mut client, &config, &client_name)
        .expect("The first retry check should succeed");

    log::info!("Starting automatic retry process");
    loop {
        let res = rx.recv_timeout(config.retry_handler_interval);
        match res {
            Ok(_) => {
                log::info!("Shutting down");
                break;
            }
            Err(err) => match err {
                RecvTimeoutError::Disconnected => {
                    log::info!("Shutting down?");
                    break;
                }
                RecvTimeoutError::Timeout => {}
            },
        }
        log::debug!("Still alive - time to scan");
        let res = do_retry_check(&mut client, &config, &client_name);
        if let Err(err) = res {
            log::error!("Retry check failed: {}", err);
            // it's probably fine to continue
            continue;
        }
    }
}
