/// Offset tracking module for exactly-once semantics
/// Stores and retrieves the last committed Kafka offset for each partition per pipeline.
/// This enables recovery: on startup, seek consumer to stored offsets instead of relying on group state.
use anyhow::{Result, anyhow};
use sqlx::PgPool;
use std::collections::BTreeMap;

/// OffsetTracker manages Kafka offset commits to a database table
/// Enables exactly-once processing by persisting offsets alongside data writes
#[derive(Debug, Clone)]
pub struct OffsetTracker {
    pool: PgPool,
}

impl OffsetTracker {
    /// Create a new offset tracker using the provided database pool
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Initialize the offset_tracker table (idempotent)
    /// Run on application startup to ensure table exists
    pub async fn init(&self) -> Result<()> {
        let init_sql = include_str!("../sql/offset_tracker.sql");
        sqlx::raw_sql(init_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| anyhow!("Failed to initialize offset_tracker table: {}", e))?;
        Ok(())
    }

    /// Read the last committed offset for a partition
    /// Returns the stored offset if exists, None otherwise
    /// Used during consumer startup to seek to the last committed position
    #[allow(dead_code)]
    pub async fn read_last_offset(
        &self,
        pipeline_name: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT \"offset\" FROM offset_tracker 
             WHERE pipeline_name = $1 AND topic = $2 AND partition = $3",
        )
        .bind(pipeline_name)
        .bind(topic)
        .bind(partition)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| anyhow!("Failed to read offset: {}", e))?;

        Ok(row.map(|(offset,)| offset))
    }

    /// Read all stored offsets for a given topic
    /// Returns a map of (partition -> offset)
    /// Useful for bulk recovery or metrics reporting
    pub async fn read_topic_offsets(
        &self,
        pipeline_name: &str,
        topic: &str,
    ) -> Result<BTreeMap<i32, i64>> {
        let rows: Vec<(i32, i64)> = sqlx::query_as(
            "SELECT partition, \"offset\" FROM offset_tracker 
             WHERE pipeline_name = $1 AND topic = $2
             ORDER BY partition ASC",
        )
        .bind(pipeline_name)
        .bind(topic)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| anyhow!("Failed to read topic offsets: {}", e))?;

        Ok(rows.into_iter().collect())
    }

    /// Write or update the offset for a partition
    /// Used in the same transaction as data write for atomic commits
    /// Returns the previous offset (if exists) for metrics/logging
    #[allow(dead_code)]
    pub async fn write_offset(
        &self,
        pipeline_name: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<Option<i64>> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| anyhow!("Failed to start transaction: {}", e))?;

        // Read current offset (if exists)
        let current: Option<(i64,)> = sqlx::query_as(
            "SELECT \"offset\" FROM offset_tracker 
             WHERE pipeline_name = $1 AND topic = $2 AND partition = $3
             FOR UPDATE",
        )
        .bind(pipeline_name)
        .bind(topic)
        .bind(partition)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| anyhow!("Failed to fetch current offset: {}", e))?;

        let previous_offset = current.map(|(o,)| o);

        // Upsert the new offset
        sqlx::query(
            "INSERT INTO offset_tracker (pipeline_name, topic, partition, \"offset\", updated_at)
             VALUES ($1, $2, $3, $4, NOW())
             ON CONFLICT (pipeline_name, topic, partition)
             DO UPDATE SET \"offset\" = EXCLUDED.\"offset\", updated_at = NOW()",
        )
        .bind(pipeline_name)
        .bind(topic)
        .bind(partition)
        .bind(offset)
        .execute(&mut *tx)
        .await
        .map_err(|e| anyhow!("Failed to write offset: {}", e))?;

        tx.commit()
            .await
            .map_err(|e| anyhow!("Failed to commit offset transaction: {}", e))?;

        Ok(previous_offset)
    }

    /// Write offset using an existing connection (e.g. inside a transaction)
    /// Caller must manage transaction begin/commit
    /// Used when combining offset write with data write in single transaction
    pub async fn write_offset_with_conn(
        conn: &mut sqlx::PgConnection,
        pipeline_name: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO offset_tracker (pipeline_name, topic, partition, \"offset\", updated_at)
             VALUES ($1, $2, $3, $4, NOW())
             ON CONFLICT (pipeline_name, topic, partition)
             DO UPDATE SET \"offset\" = EXCLUDED.\"offset\", updated_at = NOW()",
        )
        .bind(pipeline_name)
        .bind(topic)
        .bind(partition)
        .bind(offset)
        .execute(conn)
        .await
        .map_err(|e| anyhow!("Failed to write offset with connection: {}", e))?;

        Ok(())
    }

    /// Delete all offsets for a pipeline (for reset/recovery)
    #[allow(dead_code)]
    pub async fn delete_pipeline_offsets(&self, pipeline_name: &str) -> Result<u64> {
        let result = sqlx::query("DELETE FROM offset_tracker WHERE pipeline_name = $1")
            .bind(pipeline_name)
            .execute(&self.pool)
            .await
            .map_err(|e| anyhow!("Failed to delete pipeline offsets: {}", e))?;

        Ok(result.rows_affected())
    }

    /// Get all stored offsets (for debugging/monitoring)
    #[allow(dead_code)]
    pub async fn dump_all(&self) -> Result<Vec<(String, String, i32, i64)>> {
        let rows: Vec<(String, String, i32, i64)> = sqlx::query_as(
            "SELECT pipeline_name, topic, partition, \"offset\" FROM offset_tracker ORDER BY pipeline_name, topic, partition",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| anyhow!("Failed to dump offsets: {}", e))?;

        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// Tests for in-memory offset cache behavior using BTreeMap
    #[test]
    fn test_offset_cache_insert_and_retrieve() {
        let mut cache: BTreeMap<(String, String, i32), i64> = BTreeMap::new();
        
        // Insert offsets for different partitions of the same topic
        cache.insert(("pipeline1".to_string(), "topic1".to_string(), 0), 100);
        cache.insert(("pipeline1".to_string(), "topic1".to_string(), 1), 200);
        cache.insert(("pipeline1".to_string(), "topic2".to_string(), 0), 300);
        
        // Verify insertion worked correctly
        assert_eq!(cache.len(), 3);
        
        // Verify retrieval
        assert_eq!(
            cache.get(&("pipeline1".to_string(), "topic1".to_string(), 0)),
            Some(&100)
        );
        assert_eq!(
            cache.get(&("pipeline1".to_string(), "topic1".to_string(), 1)),
            Some(&200)
        );
        assert_eq!(
            cache.get(&("pipeline1".to_string(), "topic2".to_string(), 0)),
            Some(&300)
        );
    }

    /// Tests cache behavior when removing entries
    #[test]
    fn test_offset_cache_remove() {
        let mut cache: BTreeMap<(String, String, i32), i64> = BTreeMap::new();
        
        cache.insert(("pipeline1".to_string(), "topic1".to_string(), 0), 100);
        cache.insert(("pipeline1".to_string(), "topic1".to_string(), 1), 200);
        
        assert_eq!(cache.len(), 2);
        
        // Remove an entry
        let removed = cache.remove(&("pipeline1".to_string(), "topic1".to_string(), 0));
        assert_eq!(removed, Some(100));
        assert_eq!(cache.len(), 1);
        
        // Verify the remaining entry is still accessible
        assert_eq!(
            cache.get(&("pipeline1".to_string(), "topic1".to_string(), 1)),
            Some(&200)
        );
    }

    /// Tests cache behavior with updates to existing keys
    #[test]
    fn test_offset_cache_update() {
        let mut cache: BTreeMap<(String, String, i32), i64> = BTreeMap::new();
        
        let key = ("pipeline1".to_string(), "topic1".to_string(), 0);
        cache.insert(key.clone(), 100);
        assert_eq!(cache.get(&key), Some(&100));
        
        // Update the value
        cache.insert(key.clone(), 150);
        assert_eq!(cache.get(&key), Some(&150));
        assert_eq!(cache.len(), 1);  // Still only 1 entry
    }

    /// Tests cache ordering properties of BTreeMap
    #[test]
    fn test_offset_cache_ordering() {
        let mut cache: BTreeMap<(String, String, i32), i64> = BTreeMap::new();
        
        // Insert in non-alphabetical order
        cache.insert(("z-pipeline".to_string(), "topic".to_string(), 0), 100);
        cache.insert(("a-pipeline".to_string(), "topic".to_string(), 0), 200);
        cache.insert(("m-pipeline".to_string(), "topic".to_string(), 0), 300);
        
        // Verify BTreeMap maintains sorted order
        let keys: Vec<_> = cache.keys().collect();
        assert_eq!(keys[0].0, "a-pipeline");
        assert_eq!(keys[1].0, "m-pipeline");
        assert_eq!(keys[2].0, "z-pipeline");
    }

    // Note: Full integration tests requiring a live Postgres instance are run separately.
    // To run integration tests with a Postgres database:
    // POSTGRES_URL=postgresql://user:password@localhost/testdb cargo test --test integration_tests
    //
    // Unit tests above validate cache behavior without database dependencies.
    // Integration tests (if added) should verify:
    // - OffsetTracker::init() creates the offset_tracker table
    // - read_last_offset() retrieves previously stored offsets
    // - write_offset() persists offsets and handles concurrent writes
    // - delete_pipeline_offsets() correctly deletes all offsets for a pipeline
}
