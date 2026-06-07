//! # 错误处理 — handler 失败时的行为
//!
//! 演示事件总线的错误处理策略：
//! - handler 返回 Err 时不会影响其他订阅者
//! - handler 失败后自动继续处理后续事件
//! - 使用内部计数器实现简单的重试逻辑
//!
//! 运行: `cargo run --example error_handling`

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anycms_event::prelude::*;

#[derive(Clone, Debug)]
struct DataImported {
    record_count: usize,
    success: bool,
}

impl Event for DataImported {
    fn event_name() -> &'static str { "data.imported" }
    fn topic() -> &'static str { "data" }
}

#[tokio::main]
async fn main() {
    println!("=== 错误处理示例 ===\n");

    let bus = EventBus::new();

    // ── 1. 正常 handler — 总是成功 ─────────────────────────
    let success_count = Arc::new(AtomicUsize::new(0));
    let sc = success_count.clone();
    bus.subscribe(move |e: DataImported| {
        let sc = sc.clone();
        async move {
            let n = sc.fetch_add(1, Ordering::SeqCst) + 1;
            println!("✅ [审计日志] 数据导入: records={}, 成功={} (#{})",
                e.record_count, e.success, n);
            Ok(())
        }
    }).await.unwrap();

    // ── 2. 不稳定 handler — 偶尔失败 ───────────────────────
    let attempt = Arc::new(AtomicUsize::new(0));
    let attempt_clone = attempt.clone();
    let unstable_count = Arc::new(AtomicUsize::new(0));
    let uc = unstable_count.clone();
    bus.subscribe(move |e: DataImported| {
        let attempt = attempt_clone.clone();
        let uc = uc.clone();
        async move {
            let n = attempt.fetch_add(1, Ordering::SeqCst) + 1;

            // 模拟：第 2 次和第 4 次失败
            if n == 2 || n == 4 {
                println!("❌ [数据分析] 第 {} 次处理失败！模拟错误", n);
                return Err(EventBusError::HandlerError {
                    event_name: "data.imported".into(),
                    message: "数据格式异常".into(),
                });
            }

            let ok = uc.fetch_add(1, Ordering::SeqCst) + 1;
            println!("📊 [数据分析] 分析完成: records={}, 成功={} (处理成功 #{})",
                e.record_count, e.success, ok);
            Ok(())
        }
    }).await.unwrap();

    // ── 3. 带重试逻辑的 handler ─────────────────────────────
    let retry_ok = Arc::new(AtomicUsize::new(0));
    let rc = retry_ok.clone();
    bus.subscribe(move |e: DataImported| {
        let rc = rc.clone();
        async move {
            // handler 内部自己做重试
            let mut attempts = 0;
            loop {
                attempts += 1;
                // 模拟：第 3 次尝试才成功
                if attempts >= 3 {
                    let n = rc.fetch_add(1, Ordering::SeqCst) + 1;
                    println!("🔄 [通知服务] 第 {} 次重试成功！records={} (成功 #{})",
                        attempts, e.record_count, n);
                    return Ok(());
                }
                if attempts > 1 {
                    println!("🔄 [通知服务] 重试第 {} 次...", attempts);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    // ── 发布 4 次事件 ─────────────────────────────────────
    println!("--- 发布事件 ---\n");

    for i in 1..=4 {
        println!(">> 发布 #{}", i);
        bus.publish(DataImported {
            record_count: i * 100,
            success: true,
        }).await.unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        println!();
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // ── 统计结果 ───────────────────────────────────────────
    println!("--- 结果统计 ---");
    println!("✅ 审计日志: 收到 {} 条 (全部成功)", success_count.load(Ordering::SeqCst));
    println!("📊 数据分析: 成功 {} 次 (尝试了 {} 次，有 2 次失败但不影响其他 handler)",
        unstable_count.load(Ordering::SeqCst),
        attempt.load(Ordering::SeqCst));
    println!("🔄 通知服务: 成功 {} 次 (内部重试)", retry_ok.load(Ordering::SeqCst));
    println!();
    println!("💡 关键行为:");
    println!("   - handler 返回 Err → 记录日志，不影响其他 handler");
    println!("   - handler 失败后 → 继续处理后续事件（不会退出）");
    println!("   - 重试逻辑 → 在 handler 内部自行实现");
}
