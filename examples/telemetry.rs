//! # 遥测中间件 — Telemetry
//!
//! 演示如何使用 `Telemetry` trait 和 `EventBus::builder()` 监控事件生命周期。
//!
//! 运行: `cargo run --example telemetry`

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anycms_event::prelude::*;
use anycms_event::telemetry::TracingTelemetry;

// ── 1. 定义事件 ──────────────────────────────────────────────

#[derive(Clone, Debug)]
struct UserRegistered {
    user_id: u64,
    email: String,
}

impl Event for UserRegistered {
    fn event_name() -> &'static str {
        "user.registered"
    }
    fn topic() -> &'static str {
        "user"
    }
}

// ── 2. 自定义遥测实现 ────────────────────────────────────────
// 实现 Telemetry trait，将事件生命周期打印到标准输出

struct ConsoleTelemetry;

impl Telemetry for ConsoleTelemetry {
    fn on_publish(&self, event_name: &str, receivers: usize) {
        println!(
            "📊 [Telemetry] Publishing '{}' to {} receivers",
            event_name, receivers
        );
    }

    fn on_publish_complete(&self, event_name: &str, elapsed: Duration) {
        println!(
            "✅ [Telemetry] '{}' published in {:.2}ms",
            event_name,
            elapsed.as_secs_f64() * 1000.0
        );
    }

    fn on_subscribe(&self, event_name: &str, sub_id: usize) {
        println!(
            "🔔 [Telemetry] Subscriber #{} registered for '{}'",
            sub_id, event_name
        );
    }

    fn on_handler_start(&self, event_name: &str, sub_id: usize) {
        println!(
            "⚡ [Telemetry] Handler #{} started for '{}'",
            sub_id, event_name
        );
    }

    fn on_handler_complete(
        &self,
        event_name: &str,
        sub_id: usize,
        elapsed: Duration,
        error: Option<&str>,
    ) {
        match error {
            Some(err) => println!(
                "❌ [Telemetry] Handler #{} for '{}' failed: {} ({:.2}ms)",
                sub_id,
                event_name,
                err,
                elapsed.as_secs_f64() * 1000.0
            ),
            None => println!(
                "✔️ [Telemetry] Handler #{} for '{}' completed ({:.2}ms)",
                sub_id,
                event_name,
                elapsed.as_secs_f64() * 1000.0
            ),
        }
    }

    fn on_handler_lagged(&self, event_name: &str, sub_id: usize, count: usize) {
        println!(
            "⚠️ [Telemetry] Handler #{} for '{}' lagged {} messages",
            sub_id, event_name, count
        );
    }
}

// ── 3. 主逻辑 ────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    println!("=== 遥测中间件示例 ===\n");

    // ── 第一部分：自定义 ConsoleTelemetry ─────────────────────
    println!("━━━ 第一部分：ConsoleTelemetry（自定义遥测） ━━━\n");

    let bus = EventBus::builder()
        .capacity(2048)
        .telemetry(ConsoleTelemetry)
        .build();

    // 订阅事件（telemetry 的 on_subscribe 会被调用）
    let welcome_count = Arc::new(AtomicUsize::new(0));
    let wc = welcome_count.clone();
    bus.subscribe(move |e: UserRegistered| {
        let wc = wc.clone();
        async move {
            let n = wc.fetch_add(1, Ordering::SeqCst) + 1;
            println!(
                "📧 [欢迎邮件] 发送欢迎邮件给 user_id={}, email={} (#{})",
                e.user_id, e.email, n
            );
            Ok(())
        }
    })
    .await
    .unwrap();

    let audit_count = Arc::new(AtomicUsize::new(0));
    let ac = audit_count.clone();
    bus.subscribe(move |e: UserRegistered| {
        let ac = ac.clone();
        async move {
            let n = ac.fetch_add(1, Ordering::SeqCst) + 1;
            println!(
                "📋 [审计日志] 新用户注册: user_id={}, email={} (#{})",
                e.user_id, e.email, n
            );
            Ok(())
        }
    })
    .await
    .unwrap();

    // 等待订阅者就绪
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 发布事件（telemetry 的 on_publish / on_handler_start / on_handler_complete / on_publish_complete 会被调用）
    println!("--- 发布事件 ---\n");

    for i in 1..=3 {
        println!(">> 发布 #{}", i);
        bus.publish(UserRegistered {
            user_id: i * 100,
            email: format!("user{}@example.com", i),
        })
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(80)).await;
        println!();
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("📊 欢迎邮件发送: {} 次", welcome_count.load(Ordering::SeqCst));
    println!("📋 审计日志记录: {} 次", audit_count.load(Ordering::SeqCst));

    // ── 第二部分：内置 TracingTelemetry ────────────────────────
    println!();
    println!("━━━ 第二部分：TracingTelemetry（内置 tracing 遥测） ━━━\n");

    // 初始化 tracing subscriber，让 TracingTelemetry 的输出可见
    tracing_subscriber::fmt()
        .with_env_filter("anycms_event=debug")
        .with_target(false)
        .init();

    let bus2 = EventBus::builder()
        .capacity(1024)
        .telemetry(TracingTelemetry)
        .build();

    let trace_count = Arc::new(AtomicUsize::new(0));
    let tc = trace_count.clone();
    bus2.subscribe(move |e: UserRegistered| {
        let tc = tc.clone();
        async move {
            let n = tc.fetch_add(1, Ordering::SeqCst) + 1;
            println!(
                "🔍 [Tracing 模式] 处理 user.registered: user_id={} (#{})",
                e.user_id, n
            );
            Ok(())
        }
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    println!("--- 发布事件（TracingTelemetry 模式） ---\n");

    bus2.publish(UserRegistered {
        user_id: 999,
        email: "tracing@example.com".into(),
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    println!();
    println!("💡 小结:");
    println!("   - ConsoleTelemetry: 自定义实现，直接 println 输出");
    println!("   - TracingTelemetry: 内置实现，使用 tracing 结构化日志");
    println!("   - 两种遥测都通过 EventBus::builder().telemetry(...) 配置");
}
