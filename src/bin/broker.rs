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

use backoff::backoff::Backoff;
use gethostname::gethostname;
use messagebroker::broker::{get_handler_filters, process_one};
use messagebroker::{config::get_config, get_redis_connection};
use messagebroker::{logging_init, sentry_init, sleep_safe};
use redis::{ConnectionLike, ErrorKind as RedisErrorKind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::time::{Duration, Instant};

const MAX_BACKOFF: Duration = Duration::from_secs(30);
const READ_TIMEOUT: usize = 10;

fn main() {
    let config = get_config();
    logging_init();
    let _guard = sentry_init(&config);

    let (tx, rx) = channel();
    let shutdown = Arc::new(AtomicBool::new(false));

    let shutdown_sig = shutdown.clone();
    ctrlc::set_handler(move || {
        log::info!("Recieved SIGTERM!");
        if shutdown_sig.swap(true, Ordering::Relaxed) {
            log::error!("Abort!");
            std::process::exit(1);
        }
        tx.send(()).expect("Tried to announce sigterm")
    })
    .expect("Error setting Ctrl-C handler");

    let hostname = gethostname().to_string_lossy().to_lowercase();
    let client_name = format!("broker:retry_handler:{}", hostname);

    let mut backoff = backoff::ExponentialBackoffBuilder::default()
        .with_max_interval(Duration::from_secs(30))
        .with_max_elapsed_time(None)
        .build();

    let mut client =
        redis::Client::open(config.redis_url).expect("The REDIS_URL config value must be correct");

    let mut con = get_redis_connection(&mut client, &client_name);

    log::info!("Fetching handlers");
    let mut handler_filters = get_handler_filters(&config.filter_key, &mut con)
        .expect("First handler filter fetching should succeed");
    let mut next_filter_fetch = Instant::now() + config.filter_update_interval;
    log::info!("Starting broker");

    loop {
        if shutdown.load(Ordering::Relaxed) {
            log::info!("Shutting down");
            break;
        }

        if !con.check_connection() {
            con = get_redis_connection(&mut client, &client_name);
        }

        let now = Instant::now();
        if next_filter_fetch < now {
            let res = get_handler_filters(&config.filter_key, &mut con);
            match res {
                Ok(v) => handler_filters = v,
                Err(err) => {
                    sentry::capture_error(&err);
                    log::error!("Failed to fetch handler filters: {}", err);
                    let duration = backoff.next_backoff().unwrap_or(MAX_BACKOFF);
                    log::debug!("Sleeping for {} seconds", duration.as_secs());
                    if sleep_safe(duration, &rx) {
                        log::info!("Shutting down");
                        break;
                    }
                    // Try again
                    continue;
                }
            }
            next_filter_fetch = now + config.filter_update_interval;
            backoff.reset();
        }

        let res = process_one(
            &config.broker_queue_key,
            READ_TIMEOUT,
            &mut con,
            &handler_filters,
        );
        if let Err(err) = res {
            match err.kind() {
                RedisErrorKind::Serialize | RedisErrorKind::TypeError => {
                    sentry::capture_error(&err);
                    log::error!("Failed to parse message: {}", err);
                    // That's fine we'll get the next one
                    continue;
                }
                RedisErrorKind::TryAgain => continue,
                _ => {
                    sentry::capture_error(&err);
                    log::error!("Failed to route a message: {}", err);
                    let duration = backoff.next_backoff().unwrap_or(MAX_BACKOFF);
                    log::debug!("Sleeping for {} seconds", duration.as_secs());
                    if sleep_safe(duration, &rx) {
                        log::info!("Shutting down");
                        break;
                    }
                    // Try again
                    continue;
                }
            }
        }
        backoff.reset();
    }
}
