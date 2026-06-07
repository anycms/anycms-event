//! # 重试与死信队列 — Retry & Dead Letter
//!
//! 演示事件总线的重试和死信处理功能：
//! - 使用 `EventBus::builder()` 配置全局重试策略
//! - 实现 `DeadLetterHandler` trait 自定义死信处理
//! - 使用 `subscribe_with_retry()` 为单个订阅者配置重试策略
//! - Handler 失败后自动重试，重试耗尽后调用死信处理器
//!
//! 运行: `cargo run --example retry_dead_letter`

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anycms_event::bus::{DeadLetterHandler, EventBus, RetryPolicy, RetryBackoff};
use anycms_event::prelude::*;

// ── 1. 定义事件 ──────────────────────────────────────────────

#[derive(Clone, Debug)]
struct PaymentProcessed {
    transaction_id: u64,
    amount: f64,
}

impl Event for PaymentProcessed {
    fn event_name() -> &'static str {
        "payment.processed"
    }
    fn topic() -> &'static str {
        "payment"
    }
}

#[derive(Clone, Debug)]
struct NotificationSent {
    user_id: u64,
    message: String,
}

impl Event for NotificationSent {
    fn event_name() -> &'static str {
        "notification.sent"
    }
    fn topic() -> &'static str {
        "notification"
    }
}

// ── 2. 自定义死信处理器 ──────────────────────────────────────

/// 自定义死信处理器：将失败事件记录到内存中
#[derive(Clone)]
struct InMemoryDeadLetter {
    dead_letters: Arc<std::sync::Mutex<Vec<String>>>,
}

impl InMemoryDeadLetter {
    fn new() -> Self {
        Self {
            dead_letters: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn get_dead_letters(&self) -> Vec<String> {
        self.dead_letters.lock().unwrap().clone()
    }
}

impl DeadLetterHandler for InMemoryDeadLetter {
    fn on_dead_letter(&self, event_name: &str, attempts: usize, error: &str) {
        let msg = format!(
            "❌ [死信] 事件 '{}' 在 {} 次尝试后失败: {}",
            event_name, attempts, error
        );
        println!("{}", msg);
        self.dead_letters.lock().unwrap().push(msg);
    }
}

// ── 3. 主逻辑 ────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter("anycms_event=debug")
        .with_target(false)
        .init();

    println!("=== 重试与死信队列示例 ===\n");

    // ── 创建带重试策略的 EventBus ─────────────────────────
    let dead_letter_handler = InMemoryDeadLetter::new();

    // 配置全局重试策略：
    // - 最多重试 3 次
    // - 使用固定间隔 100ms
    // - 每次尝试超时 5 秒
    let global_retry_policy = RetryPolicy {
        max_retries: 3,
        backoff: RetryBackoff::Fixed(Duration::from_millis(100)),
        timeout_per_attempt: Duration::from_secs(5),
    };

    let bus = EventBus::builder()
        .capacity(1024)
        .retry_policy(global_retry_policy.clone())
        .dead_letter_handler(dead_letter_handler.clone())
        .build();

    // ── Handler 1: 前两次失败，第三次成功 ───────────────────
    println!("━━━ Handler 1: 前两次失败，第三次成功 ━━━\n");

    let handler1_attempts = Arc::new(AtomicUsize::new(0));
    let h1a = handler1_attempts.clone();
    let handler1_success = Arc::new(AtomicUsize::new(0));
    let h1s = handler1_success.clone();

    bus.subscribe(move |e: PaymentProcessed| {
        let h1a = h1a.clone();
        let h1s = h1s.clone();
        async move {
            let attempt = h1a.fetch_add(1, Ordering::SeqCst) + 1;

            println!(
                "  💳 [支付通知] 尝试 #{}/3: transaction_id={}, amount={}",
                attempt, e.transaction_id, e.amount
            );

            // 模拟：前两次失败，第三次成功
            if attempt < 3 {
                println!("    ❌ 失败：网络错误");
                return Err(EventBusError::HandlerError {
                    event_name: "payment.processed".into(),
                    message: "网络连接失败".into(),
                });
            }

            let success_count = h1s.fetch_add(1, Ordering::SeqCst) + 1;
            println!(
                "    ✅ 成功！支付通知已发送 (成功 #{})",
                success_count
            );
            Ok(())
        }
    })
    .await
    .unwrap();

    // ── Handler 2: 使用自定义重试策略（指数退避） ──────────
    println!("━━━ Handler 2: 自定义重试策略（指数退避） ━━━\n");

    let handler2_attempts = Arc::new(AtomicUsize::new(0));
    let h2a = handler2_attempts.clone();
    let handler2_success = Arc::new(AtomicUsize::new(0));
    let h2s = handler2_success.clone();

    // 为这个订阅者配置独立的重试策略：
    // - 最多重试 5 次
    // - 指数退避：从 50ms 开始，最大 2 秒
    let custom_retry = RetryPolicy {
        max_retries: 5,
        backoff: RetryBackoff::Exponential {
            base: Duration::from_millis(50),
            max: Duration::from_secs(2),
        },
        timeout_per_attempt: Duration::from_secs(10),
    };

    bus.subscribe_with_retry(
        move |e: PaymentProcessed| {
            let h2a = h2a.clone();
            let h2s = h2s.clone();
            async move {
                let attempt = h2a.fetch_add(1, Ordering::SeqCst) + 1;

                println!(
                    "  📊 [数据分析] 尝试 #{}/5: transaction_id={}",
                    attempt, e.transaction_id
                );

                // 模拟：前 4 次失败，第 5 次成功
                if attempt < 5 {
                    println!("    ❌ 失败：数据格式错误");
                    return Err(EventBusError::HandlerError {
                        event_name: "payment.processed".into(),
                        message: "数据格式不正确".into(),
                    });
                }

                let success_count = h2s.fetch_add(1, Ordering::SeqCst) + 1;
                println!(
                    "    ✅ 成功！数据分析完成 (成功 #{})",
                    success_count
                );
                Ok(())
            }
        },
        custom_retry,
    )
    .await
    .unwrap();

    // ── Handler 3: 总是失败（触发死信） ──────────────────────
    println!("━━━ Handler 3: 总是失败（触发死信） ━━━\n");

    let handler3_attempts = Arc::new(AtomicUsize::new(0));
    let h3a = handler3_attempts.clone();

    bus.subscribe(move |e: NotificationSent| {
        let h3a = h3a.clone();
        async move {
            let attempt = h3a.fetch_add(1, Ordering::SeqCst) + 1;

            println!(
                "  📧 [邮件服务] 尝试 #{}/3: user_id={}, message={}",
                attempt, e.user_id, e.message
            );

            // 这个 handler 总是失败
            println!("    ❌ 失败：邮件服务不可用");
            Err(EventBusError::HandlerError {
                event_name: "notification.sent".into(),
                message: "SMTP 服务连接失败".into(),
            })
        }
    })
    .await
    .unwrap();

    // 等待订阅者就绪
    tokio::time::sleep(Duration::from_millis(100)).await;

    // ── 发布事件 ─────────────────────────────────────────────
    println!("━━━ 发布事件 ━━━\n");

    // 发布支付事件（Handler 1 和 2 会处理）
    println!(">> 发布支付事件 #1");
    bus.publish(PaymentProcessed {
        transaction_id: 1001,
        amount: 99.99,
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(800)).await;
    println!();

    // 发布通知事件（Handler 3 会处理并失败）
    println!(">> 发布通知事件 #1");
    bus.publish(NotificationSent {
        user_id: 42,
        message: "欢迎注册！".to_string(),
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(800)).await;
    println!();

    // 再发布一个支付事件
    println!(">> 发布支付事件 #2");
    bus.publish(PaymentProcessed {
        transaction_id: 1002,
        amount: 149.99,
    })
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(800)).await;
    println!();

    // ── 统计结果 ─────────────────────────────────────────────
    println!("━━━ 结果统计 ━━━\n");

    println!(
        "💳 Handler 1 (支付通知): 成功 {} 次，尝试 {} 次",
        handler1_success.load(Ordering::SeqCst),
        handler1_attempts.load(Ordering::SeqCst)
    );
    println!(
        "📊 Handler 2 (数据分析): 成功 {} 次，尝试 {} 次",
        handler2_success.load(Ordering::SeqCst),
        handler2_attempts.load(Ordering::SeqCst)
    );
    println!(
        "📧 Handler 3 (邮件服务): 尝试 {} 次（全部失败）",
        handler3_attempts.load(Ordering::SeqCst)
    );

    println!("\n━━━ 死信队列 ━━━\n");
    let dead_letters = dead_letter_handler.get_dead_letters();
    if dead_letters.is_empty() {
        println!("（没有死信事件）");
    } else {
        for (i, letter) in dead_letters.iter().enumerate() {
            println!("{}. {}", i + 1, letter);
        }
    }

    println!("\n💡 关键特性:");
    println!("   - 全局重试策略：通过 EventBus::builder().retry_policy() 配置");
    println!("   - 独立重试策略：通过 subscribe_with_retry() 为单个订阅者配置");
    println!("   - 死信处理：重试耗尽后自动调用 DeadLetterHandler");
    println!("   - 退避策略：支持固定间隔和指数退避");
}
