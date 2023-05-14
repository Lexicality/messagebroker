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
use chrono::{DateTime, TimeZone, Utc};
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
        data.insert("uuid".to_owned(), json!(uuid.to_string()));

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

// Because of course we use `str()` over `isoformat` with a naive datetime
const PYTHON_TOSTR_DATETIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f%";

impl Message {
    pub fn get(&self, key: &'static str) -> Option<&JSONValue> {
        self.raw.get(key)
    }

    pub fn only_for(&self) -> Option<Vec<&String>> {
        let value = self.get("only_for")?;
        match value {
            JSONValue::String(str) => Some(vec![str]),
            JSONValue::Array(arr) => Some(
                arr.iter()
                    .filter_map(|value| {
                        match value {
                            JSONValue::String(str) => Some(str),
                            _ => {
                                log::warn!(
                                    "Message {} ({}) has 'only_for' containing non-string value: '{:?}'",
                                    self.message_type,
                                    self.uuid,
                                    value
                                );
                                None
                            }
                        }
                    })
                    .collect(),
            ),
            _ => {
                log::warn!(
                    "Message {} ({}) has non-string 'only_for' value: '{:?}'",
                    self.message_type,
                    self.uuid,
                    value
                );
                None
            },
        }
    }

    pub fn start_time(&self) -> Option<DateTime<Utc>> {
        let start_time = self.get("start_time")?;
        let start_time = match start_time {
            JSONValue::String(v) => v,
            _ => {
                log::warn!(
                    "Message {} ({}) has non-string value for 'start_time': '{:?}'",
                    self.message_type,
                    self.uuid,
                    start_time
                );
                return None;
            }
        };
        log::trace!("Attempting to parse date string {}", start_time);
        // First vainly try parsing as RFC3339 in the hopes that some day our
        // data won't be terrible
        let res = DateTime::parse_from_rfc3339(start_time);
        if let Ok(res) = res {
            // amazing
            log::trace!("It's RFC3339!");
            return Some(res.into());
        }
        log::trace!("It's not RFC3339 :(");

        // Now attempt to parse python's tostring implementation
        let res = Utc.datetime_from_str(start_time, PYTHON_TOSTR_DATETIME_FORMAT);
        if let Ok(res) = res {
            log::trace!("It's python's highly stable tostring output");
            return Some(res);
        }

        log::warn!(
            "Message {} ({}) has invalid date value for 'start_time': '{}'",
            self.message_type,
            self.uuid,
            start_time
        );

        None
    }
}

#[cfg(test)]
mod test_from_redis_value {
    use super::Message;
    use redis::{from_redis_value, ErrorKind as RedisErrorKind, Value as RedisValue};
    use redis_test::IntoRedisValue;
    use serde_json::Value as JSONValue;
    use uuid::Uuid;

    fn v<V: IntoRedisValue>(v: V) -> RedisValue {
        v.into_redis_value()
    }

    #[test]
    fn it_parses() {
        let input = r#"
        {
            "message_type": "example"
        }
        "#;

        let message: Message = from_redis_value(&v(input)).unwrap();

        assert_eq!(message.message_type, "example");
    }

    #[test]
    fn it_fails_on_invalid_json() {
        let input = r#"
        {
            "message_type": "example"
        "#;

        let err = from_redis_value::<Message>(&v(input)).unwrap_err();

        assert!(matches!(err.kind(), RedisErrorKind::Serialize));
    }

    #[test]
    fn it_fails_on_wrong_json() {
        let input = r#"
        [
            "message_type", "example"
        ]
        "#;

        let err = from_redis_value::<Message>(&v(input)).unwrap_err();

        assert!(matches!(err.kind(), RedisErrorKind::TypeError));
    }

    #[test]
    fn it_fails_for_missing_message_type() {
        let input = r#"
        {
            "massage_type": "example"
        }
        "#;

        let err = from_redis_value::<Message>(&v(input)).unwrap_err();

        assert!(matches!(err.kind(), RedisErrorKind::TypeError));
    }

    #[test]
    fn it_parses_uuids() {
        let input = r#"
        {
            "message_type": "example",
            "uuid": "00000000-0000-0000-0000-000000000000"
        }
        "#;

        let message: Message = from_redis_value(&v(input)).unwrap();

        assert_eq!(message.uuid, Uuid::from_u128(0));
    }

    #[test]
    fn it_generates_uuids() {
        let input = r#"
        {
            "message_type": "example"
        }
        "#;

        let message: Message = from_redis_value(&v(input)).unwrap();

        // Can't think of a way to test that this is a random value so I guess
        // make sure it's not the default value?
        assert_ne!(message.uuid, Uuid::nil());

        // It updates the value in the raw event
        let raw_uuid = &message.raw["uuid"];
        let JSONValue::String(raw_uuid)  = raw_uuid else { panic!()};
        assert_eq!(&message.uuid.to_string(), raw_uuid);
    }

    #[test]
    fn it_validates_uuids() {
        let input = r#"
        {
            "message_type": "example",
            "uuid": "meow"
        }
        "#;

        let err = from_redis_value::<Message>(&v(input)).unwrap_err();

        assert!(matches!(err.kind(), RedisErrorKind::TypeError));
    }

    #[test]
    fn it_reformats_uuids() {
        let input = r#"
        {
            "message_type": "example",
            "uuid": "00000000-0000-0000-0000-000000aAaAaA"
        }
        "#;

        let message: Message = from_redis_value(&v(input)).unwrap();

        assert_eq!(message.uuid, Uuid::from_u128(0xAAAAAA));

        // It updates the value in the raw event
        let raw_uuid = &message.raw["uuid"];
        let JSONValue::String(raw_uuid)  = raw_uuid else { panic!()};
        // The uuid is now lowercase
        assert_eq!("00000000-0000-0000-0000-000000aaaaaa", raw_uuid);
    }
}

#[cfg(test)]
mod test_only_for {
    use super::Message;
    use redis::from_redis_value;
    use redis_test::IntoRedisValue;

    fn json_to_msg(input: &str) -> Message {
        from_redis_value(&(input.into_redis_value())).unwrap()
    }

    #[test]
    fn it_works() {
        let message = json_to_msg(
            r#"{
                "message_type": "example",
                "only_for": ["foo"]
            }"#,
        );

        assert_eq!(message.only_for().unwrap(), vec!["foo"]);
    }

    #[test]
    fn it_accepts_strings() {
        let message = json_to_msg(
            r#"{
                "message_type": "example",
                "only_for": "foo"
            }"#,
        );

        assert_eq!(message.only_for().unwrap(), vec!["foo"]);
    }

    #[test]
    fn it_handles_missing() {
        let message = json_to_msg(
            r#"{
                "message_type": "example"
            }"#,
        );

        assert!(message.only_for().is_none());
    }

    #[test]
    fn it_handles_wrong() {
        let message = json_to_msg(
            r#"{
                "message_type": "example",
                "only_for": {"foo": "bar"}
            }"#,
        );

        assert!(message.only_for().is_none());
    }

    #[test]
    fn it_discards_non_strings() {
        let message = json_to_msg(
            r#"{
                "message_type": "example",
                "only_for": ["foo", 7, "bar"]
            }"#,
        );

        assert_eq!(message.only_for().unwrap(), vec!["foo", "bar"]);
    }
}
