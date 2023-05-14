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

use serde::Deserialize;
use std::time::Duration;
use url::Url;

#[derive(Deserialize, Debug)]
pub struct Config {
    #[serde(default = "default_redis_url")]
    pub redis_url: Url,

    #[serde(default = "default_broker_queue_key")]
    pub broker_queue_key: String,

    #[serde(default = "default_filter_key")]
    pub filter_key: String,

    #[serde(default = "default_retry_after")]
    pub retry_messages_after: Duration,

    #[serde(default = "default_retry_interval")]
    pub retry_handler_interval: Duration,

    #[serde(default = "default_filter_update_interval")]
    pub filter_update_interval: Duration,
}

fn default_redis_url() -> Url {
    Url::parse("redis://redis/0").unwrap()
}

fn default_broker_queue_key() -> String {
    "q:broker".to_owned()
}

fn default_filter_key() -> String {
    "q:list".to_owned()
}

fn default_retry_after() -> Duration {
    // An apparently random number chosen
    // "Because 1 hour doesn't seem enough for some methods..."
    //
    // 12 hours seems like it's long enough to cause exciting problems around data
    // consistency but who am I to judge genius?
    Duration::from_secs(12 * 60 * 60)
}

fn default_retry_interval() -> Duration {
    Duration::from_secs(60)
}

fn default_filter_update_interval() -> Duration {
    Duration::from_secs(5)
}

pub fn get_config() -> Config {
    envy::from_env::<Config>().unwrap()
}
