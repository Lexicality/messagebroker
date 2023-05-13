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
use redis::{from_redis_value, FromRedisValue, RedisResult, Value as RedisValue};
use serde_json::{json, Value as JSONValue};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Message {
    pub message_type: String,
    pub uuid: Uuid,
    pub raw: serde_json::Value,
}

impl FromRedisValue for Message {
    fn from_redis_value(raw: &RedisValue) -> RedisResult<Self> {
        let raw: String = from_redis_value(raw)?;
        let mut raw: JSONValue = serde_json::from_str(&raw)?;

        let JSONValue::Object(ref mut data) = raw else {
			return Err((redis::ErrorKind::TypeError, "not a dictionary").into())
		};

        let uuid = match data.get("uuid") {
            None => Uuid::new_v4(),
            Some(value) => match value {
                JSONValue::String(value) => Uuid::parse_str(value).or::<redis::RedisError>(Err(
                    (redis::ErrorKind::TypeError, "uuid field is invalid").into(),
                ))?,
                _ => return Err((redis::ErrorKind::TypeError, "uuid field is not a string").into()),
            },
        };
        data["uuid"] = json!(uuid.to_string());

        let Some(JSONValue::String(message_type )) = data.get("message_type") else {
            return Err((redis::ErrorKind::TypeError, "message_type field is missing or invalid").into());
        };
        log::trace!("Recieved a '{}' message: {:?}", message_type, data);

        Ok(Message {
            message_type: message_type.clone(),
            uuid,
            raw,
        })
    }
}

impl Message {
    pub fn get(&self, key: &'static str) -> Option<&JSONValue> {
        self.raw.get(key)
    }

    pub fn set(&mut self, key: &'static str, value: JSONValue) {
        self.raw[key] = value;
        if key == "only_for" {}
    }
}
