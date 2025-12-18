use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct MessageContext {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

impl MessageContext {
    pub fn new(topic: String, partition: i32, offset: i64) -> Self {
        Self {
            topic,
            partition,
            offset,
        }
    }

    pub fn correlation_id(&self) -> String {
        format!("{}:{}:{}", self.topic, self.partition, self.offset)
    }
}
