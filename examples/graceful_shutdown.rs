//! # 优雅关闭 — shutdown() vs shutdown_graceful()
//!
//! 演示事件总线的两种关闭方式：
//! - `shutdown()`: 立即终止，abort 所有正在处理的任务
//! - `shutdown_graceful(timeout)`: 优雅关闭，等待正在处理的任务完成
//!
//! 运行: `cargo run --example graceful_shutdown`

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anycms_event::prelude::*;

#[derive(Clone, Debug)]
struct ProcessingJob {
    job_id: u64,
    duration_ms: u64,
}

impl Event for ProcessingJob {
    fn event_name() -> &'static str {
        "processing.job"
    }
    fn topic() -> &'static str {
        "jobs"
    }
}

#[tokio::main]
async fn main() {
    println!("=== 优雅关闭示例 ===\n");

    // ── 第一部分: shutdown() 立即终止 ─────────────────────────────────
    println!("--- 第一部分: shutdown() 立即终止 ---\n");

    let bus1 = EventBus::new();
    let processed_count1 = Arc::new(AtomicUsize::new(0));
    let counter1 = processed_count1.clone();

    // 订阅者：模拟耗时任务（每个任务需要 500ms）
    bus1.subscribe(move |e: ProcessingJob| {
        let counter = counter1.clone();
        async move {
            println!("🔄 [Bus1] 开始处理任务 #{} (需要 {}ms)", e.job_id, e.duration_ms);
            tokio::time::sleep(Duration::from_millis(e.duration_ms)).await;
            let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
            println!("✅ [Bus1] 任务 #{} 处理完成 (总计: {})", e.job_id, n);
            Ok(())
        }
    }).await.unwrap();

    // 等待订阅者就绪
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 发布 3 个任务，每个需要 500ms
    println!("发布 3 个任务...\n");
    for i in 1..=3 {
        bus1.publish(ProcessingJob {
            job_id: i,
            duration_ms: 500,
        }).await.unwrap();
    }

    // 等待 200ms 后立即关闭（任务还在处理中）
    println!("等待 200ms 后调用 shutdown()...");
    tokio::time::sleep(Duration::from_millis(200)).await;

    println!("🚫 调用 shutdown() - 立即终止所有任务");
    bus1.shutdown();

    // 再等待一下看看结果
    tokio::time::sleep(Duration::from_millis(100)).await;
    println!("已处理任务数: {}（预期少于 3 个，因为被 abort）\n", processed_count1.load(Ordering::SeqCst));

    // ── 第二部分: shutdown_graceful() 优雅关闭 ────────────────────────
    println!("--- 第二部分: shutdown_graceful() 优雅关闭 ---\n");

    let bus2 = EventBus::new();
    let processed_count2 = Arc::new(AtomicUsize::new(0));
    let counter2 = processed_count2.clone();

    // 订阅者：模拟耗时任务
    bus2.subscribe(move |e: ProcessingJob| {
        let counter = counter2.clone();
        async move {
            println!("🔄 [Bus2] 开始处理任务 #{} (需要 {}ms)", e.job_id, e.duration_ms);
            tokio::time::sleep(Duration::from_millis(e.duration_ms)).await;
            let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
            println!("✅ [Bus2] 任务 #{} 处理完成 (总计: {})", e.job_id, n);
            Ok(())
        }
    }).await.unwrap();

    // 等待订阅者就绪
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 发布 3 个任务，每个需要 300ms
    println!("发布 3 个任务...\n");
    for i in 1..=3 {
        bus2.publish(ProcessingJob {
            job_id: i,
            duration_ms: 300,
        }).await.unwrap();
    }

    // 等待 200ms 后优雅关闭（给足够时间完成）
    println!("等待 200ms 后调用 shutdown_graceful()...");
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 优雅关闭，等待最多 2 秒让任务完成
    println!("🕊️  调用 shutdown_graceful(2s) - 等待任务完成");
    let start = std::time::Instant::now();
    let remaining = bus2.shutdown_graceful(Duration::from_secs(2)).await;
    let elapsed = start.elapsed();

    println!("shutdown_graceful() 耗时: {:?}", elapsed);
    println!("未完成的任务数: {} (0 表示全部完成)", remaining);
    println!("已处理任务数: {}（预期 3 个，全部完成）\n", processed_count2.load(Ordering::SeqCst));

    // ── 第三部分: shutdown_graceful() 超时场景 ────────────────────────
    println!("--- 第三部分: shutdown_graceful() 超时场景 ---\n");

    let bus3 = EventBus::new();
    let processed_count3 = Arc::new(AtomicUsize::new(0));
    let counter3 = processed_count3.clone();

    // 订阅者：模拟耗时任务（每个需要 1000ms）
    bus3.subscribe(move |e: ProcessingJob| {
        let counter = counter3.clone();
        async move {
            println!("🔄 [Bus3] 开始处理任务 #{} (需要 {}ms)", e.job_id, e.duration_ms);
            tokio::time::sleep(Duration::from_millis(e.duration_ms)).await;
            let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
            println!("✅ [Bus3] 任务 #{} 处理完成 (总计: {})", e.job_id, n);
            Ok(())
        }
    }).await.unwrap();

    // 等待订阅者就绪
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 发布 2 个任务，每个需要 1000ms
    println!("发布 2 个长任务...\n");
    for i in 1..=2 {
        bus3.publish(ProcessingJob {
            job_id: i,
            duration_ms: 1000,
        }).await.unwrap();
    }

    // 等待 200ms 后用短超时关闭
    println!("等待 200ms 后调用 shutdown_graceful(500ms)...");
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 优雅关闭，但只等待 500ms（不足以完成任务）
    println!("🕊️  调用 shutdown_graceful(500ms) - 超时演示");
    let start = std::time::Instant::now();
    let remaining = bus3.shutdown_graceful(Duration::from_millis(500)).await;
    let elapsed = start.elapsed();

    println!("shutdown_graceful() 耗时: {:?}", elapsed);
    println!("未完成的任务数: {} (>0 表示超时)", remaining);
    println!("已处理任务数: {}（预期 < 2 个，因为超时）\n", processed_count3.load(Ordering::SeqCst));

    // ── 总结 ──────────────────────────────────────────────────────────
    println!("--- 总结 ---");
    println!("- shutdown(): 立即终止，不等待任务完成");
    println!("- shutdown_graceful(timeout): 等待任务完成，超时后返回未完成数");
    println!("- 优雅关闭适合需要确保数据完整性的场景");
}
