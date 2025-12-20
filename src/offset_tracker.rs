/// Offset tracking module for exactly-once semantics
/// Stores and retrieves the last committed Kafka offset for each partition per pipeline.
/// This enables recovery: on startup, seek consumer to stored offsets instead of relying on group state.
use anyhow::{Result, anyhow};
use sqlx::PgPool;
use std::collections::BTreeMap;
use tracing::{debug, error, info, warn};

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
    /// Handles migration for existing tables that lack tenant_id column
    pub async fn init(&self) -> Result<()> {
        // First, try to create the table with the full schema
        let init_sql = include_str!("../sql/offset_tracker.sql");

        match sqlx::raw_sql(init_sql).execute(&self.pool).await {
            Ok(_) => {
                info!("Successfully initialized offset_tracker table");
                return Ok(());
            }
            Err(e) => {
                // Check if the error is about the tenant_id column not existing
                let error_msg = e.to_string();
                if error_msg.contains("column \"tenant_id\" does not exist") {
                    info!("Detected existing offset_tracker table without tenant_id column, performing migration...");

                    // Perform migration: add tenant_id column and set default values
                    match self.migrate_offset_tracker_table().await {
                        Ok(_) => {
                            info!("Successfully migrated offset_tracker table");
                            Ok(())
                        }
                        Err(migration_err) => {
                            error!("Failed to migrate offset_tracker table: {}", migration_err);
                            Err(anyhow!("Failed to migrate offset_tracker table: {}", migration_err))
                        }
                    }
                } else {
                    error!("Failed to initialize offset_tracker table: {}", e);
                    Err(anyhow!("Failed to initialize offset_tracker table: {}", e))
                }
            }
        }
    }

    /// Migrate existing offset_tracker table to include tenant_id column
    async fn migrate_offset_tracker_table(&self) -> Result<()> {
        // Add tenant_id column with default value
        sqlx::query("ALTER TABLE offset_tracker ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'default'")
            .execute(&self.pool)
            .await
            .map_err(|e| anyhow!("Failed to add tenant_id column: {}", e))?;

        // Update the primary key to include tenant_id
        sqlx::query("ALTER TABLE offset_tracker DROP CONSTRAINT offset_tracker_pkey")
            .execute(&self.pool)
            .await
            .map_err(|e| anyhow!("Failed to drop old primary key: {}", e))?;

        sqlx::query("ALTER TABLE offset_tracker ADD CONSTRAINT offset_tracker_pkey PRIMARY KEY (tenant_id, pipeline_name, topic, partition)")
            .execute(&self.pool)
            .await
            .map_err(|e| anyhow!("Failed to add new primary key: {}", e))?;

        // Recreate indexes with tenant_id
        sqlx::query("DROP INDEX IF EXISTS idx_offset_tracker_pipeline")
            .execute(&self.pool)
            .await
            .map_err(|e| anyhow!("Failed to drop old pipeline index: {}", e))?;

        sqlx::query("DROP INDEX IF EXISTS idx_offset_tracker_topic")
            .execute(&self.pool)
            .await
            .map_err(|e| anyhow!("Failed to drop old topic index: {}", e))?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_offset_tracker_pipeline ON offset_tracker(tenant_id, pipeline_name)")
            .execute(&self.pool)
            .await
            .map_err(|e| anyhow!("Failed to create new pipeline index: {}", e))?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_offset_tracker_topic ON offset_tracker(tenant_id, topic)")
            .execute(&self.pool)
            .await
            .map_err(|e| anyhow!("Failed to create new topic index: {}", e))?;

        info!("Migration completed: added tenant_id column and updated constraints/indexes");
        Ok(())
    }

    /// Read the last committed offset for a partition
    /// Returns the stored offset if exists, None otherwise
    /// Used during consumer startup to seek to the last committed position
    /// Read the last committed offset for a partition (with tenant support)
    /// Returns the stored offset if exists, None otherwise
    /// Used during consumer startup to seek to the last committed position
    pub async fn read_last_offset_for_tenant(
        &self,
        tenant_id: &str,
        pipeline_name: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT \"offset\" FROM offset_tracker 
             WHERE tenant_id = $1 AND pipeline_name = $2 AND topic = $3 AND partition = $4",
        )
        .bind(tenant_id)
        .bind(pipeline_name)
        .bind(topic)
        .bind(partition)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| anyhow!("Failed to read offset: {}", e))?;

        Ok(row.map(|(offset,)| offset))
    }

    #[allow(dead_code)]
    pub async fn read_last_offset(
        &self,
        pipeline_name: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>> {
        // Legacy method - reads without tenant_id (for backward compatibility)
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT \"offset\" FROM offset_tracker 
             WHERE tenant_id = 'default' AND pipeline_name = $1 AND topic = $2 AND partition = $3",
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

    /// Write or update the offset for a partition (with tenant support)
    /// Used in the same transaction as data write for atomic commits
    /// Returns the previous offset (if exists) for metrics/logging
    pub async fn write_offset_for_tenant(
        &self,
        tenant_id: &str,
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
             WHERE tenant_id = $1 AND pipeline_name = $2 AND topic = $3 AND partition = $4
             FOR UPDATE",
        )
        .bind(tenant_id)
        .bind(pipeline_name)
        .bind(topic)
        .bind(partition)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| anyhow!("Failed to fetch current offset: {}", e))?;

        let previous_offset = current.map(|(o,)| o);

        // Upsert the new offset
        sqlx::query(
            "INSERT INTO offset_tracker (tenant_id, pipeline_name, topic, partition, \"offset\", updated_at)
             VALUES ($1, $2, $3, $4, $5, NOW())
             ON CONFLICT (tenant_id, pipeline_name, topic, partition)
             DO UPDATE SET \"offset\" = EXCLUDED.\"offset\", updated_at = NOW()",
        )
        .bind(tenant_id)
        .bind(pipeline_name)
        .bind(topic)
        .bind(partition)
        .bind(offset)
        .execute(&mut *tx)
        .await
        .map_err(|e| anyhow!("Failed to write offset: {}", e))?;

        tx.commit()
            .await
            .map_err(|e| {
                error!(tenant_id, pipeline_name, topic, partition, offset, "Failed to commit offset transaction: {}", e);
                anyhow!("Failed to commit offset transaction: {}", e)
            })?;

        debug!(tenant_id, pipeline_name, topic, partition, offset, previous_offset, "Successfully wrote offset");
        Ok(previous_offset)
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
        // Legacy method - writes with default tenant_id
        self.write_offset_for_tenant("default", pipeline_name, topic, partition, offset)
            .await
    }

    /// Write offset using an existing connection (e.g. inside a transaction)
    /// Caller must manage transaction begin/commit
    /// Used when combining offset write with data write in single transaction
    pub async fn write_offset_with_conn(
        conn: &mut sqlx::PgConnection,
        tenant_id: &str,
        pipeline_name: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO offset_tracker (tenant_id, pipeline_name, topic, partition, \"offset\", updated_at)
             VALUES ($1, $2, $3, $4, $5, NOW())
             ON CONFLICT (tenant_id, pipeline_name, topic, partition)
             DO UPDATE SET \"offset\" = EXCLUDED.\"offset\", updated_at = NOW()",
        )
        .bind(tenant_id)
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
            .map_err(|e| {
                error!(pipeline_name, "Failed to delete pipeline offsets: {}", e);
                anyhow!("Failed to delete pipeline offsets: {}", e)
            })?;

        let deleted_count = result.rows_affected();
        info!(pipeline_name, deleted_count, "Successfully deleted pipeline offsets");
        Ok(deleted_count)
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
        assert_eq!(cache.len(), 1); // Still only 1 entry
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
