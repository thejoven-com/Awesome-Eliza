// src/lib.rs
/*
 * DriftRealm Core Library
 * High-performance async and parallel processing
 */

use rayon::prelude::*;
use tokio::sync::{mpsc, RwLock};
use std::sync::Arc;
use serde::{Serialize, Deserialize};
use tracing::{info, debug, error};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, Clone)]
pub struct Config {
    pub workers: usize,
    pub parallel: bool,
    pub batch_size: usize,
    pub timeout_secs: u64,
}

impl Config {
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }
}

#[derive(Default)]
pub struct ConfigBuilder {
    workers: usize,
    parallel: bool,
    batch_size: usize,
    timeout_secs: u64,
}

impl ConfigBuilder {
    pub fn workers(mut self, workers: usize) -> Self {
        self.workers = workers;
        self
    }
    
    pub fn parallel(mut self, parallel: bool) -> Self {
        self.parallel = parallel;
        self
    }
    
    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }
    
    pub fn timeout(mut self, secs: u64) -> Self {
        self.timeout_secs = secs;
        self
    }
    
    pub fn build(self) -> Config {
        Config {
            workers: if self.workers == 0 { 4 } else { self.workers },
            parallel: self.parallel,
            batch_size: if self.batch_size == 0 { 100 } else { self.batch_size },
            timeout_secs: if self.timeout_secs == 0 { 30 } else { self.timeout_secs },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessResult {
    pub success: bool,
    pub items_processed: usize,
    pub duration_ms: u64,
    pub data: Option<serde_json::Value>,
}

pub struct App {
    config: Config,
    stats: Arc<RwLock<ProcessingStats>>,
}

#[derive(Default)]
struct ProcessingStats {
    total_processed: usize,
    errors: usize,
    start_time: Option<std::time::Instant>,
}

impl App {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            stats: Arc::new(RwLock::new(ProcessingStats::default())),
        }
    }
    
    pub async fn run(
        &self,
        input: Option<String>,
        output: Option<String>,
    ) -> Result<()> {
        info!("Starting processing pipeline");
        
        let start = std::time::Instant::now();
        {
            let mut stats = self.stats.write().await;
            stats.start_time = Some(start);
        }
        
        // Generate sample data for processing
        let data: Vec<i32> = (0..1000).collect();
        
        let results = if self.config.parallel {
            self.process_parallel(&data).await?
        } else {
            self.process_sequential(&data).await?
        };
        
        let duration = start.elapsed();
        info!("Processed {} items in {:?}", results.len(), duration);
        
        // Save results if output path provided
        if let Some(output_path) = output {
            let result = ProcessResult {
                success: true,
                items_processed: results.len(),
                duration_ms: duration.as_millis() as u64,
                data: Some(serde_json::to_value(&results)?),
            };
            
            let json = serde_json::to_string_pretty(&result)?;
            tokio::fs::write(&output_path, json).await?;
            info!("Results saved to {}", output_path);
        }
        
        Ok(())
    }
    
    async fn process_parallel(&self, data: &[i32]) -> Result<Vec<i32>> {
        debug!("Using parallel processing with Rayon");
        
        let results: Vec<i32> = data
            .par_iter()
            .map(|&x| {
                // Simulate CPU-intensive work
                x * 2 + 1
            })
            .collect();
        
        Ok(results)
    }
    
    async fn process_sequential(&self, data: &[i32]) -> Result<Vec<i32>> {
        debug!("Using sequential processing");
        
        let results: Vec<i32> = data
            .iter()
            .map(|&x| x * 2 + 1)
            .collect();
        
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_parallel_processing() {
        let config = Config::builder()
            .workers(4)
            .parallel(true)
            .build();
        
        let app = App::new(config);
        let data: Vec<i32> = (0..100).collect();
        let results = app.process_parallel(&data).await.unwrap();
        
        assert_eq!(results.len(), 100);
        assert_eq!(results[0], 1);  // 0 * 2 + 1 = 1
        assert_eq!(results[1], 3);  // 1 * 2 + 1 = 3
    }
}
