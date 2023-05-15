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

use std::process::ExitCode;

use gethostname::gethostname;
use messagebroker::config::get_config;
use messagebroker::retries::check_for_retries;
use messagebroker::{get_redis_connection, logging_init, sentry_init};

fn main() -> ExitCode {
    let config = get_config();
    logging_init();
    let _guard = sentry_init(&config);

    let hostname = gethostname().to_string_lossy().to_lowercase();
    let client_name = format!("broker:retry_handler:{}", hostname);

    let mut client = redis::Client::open(config.redis_url.clone())
        .expect("The REDIS_URL config value must be correct");

    let mut con = get_redis_connection(&mut client, &client_name);
    let res = check_for_retries(config.retry_messages_after, &mut con);
    match res {
        Ok(_) => ExitCode::SUCCESS,
        Err(err) => {
            sentry::capture_error(&err);
            log::error!("Retry check failed: {}", err);
            ExitCode::FAILURE
        }
    }
}
