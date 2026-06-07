//! # 基础用法 — 手动实现 Event trait
//!
//! 最简单的 pub/sub 示例，不使用宏，手动定义事件和 Event trait。
//!
//! 运行: `cargo run --example basic_usage`

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anycms_event::prelude::*;

// ── 1. 定义事件结构体 ──────────────────────────────────────────
// Event trait 要求 Clone + Send + Sync + 'static
// 不再需要 Serialize/Deserialize（核心 bus 使用 Arc<dyn Any> 进行类型擦除）

#[derive(Clone, Debug)]
struct UserLoggedIn {
    user_id: u64,
    ip_address: String,
}

// 手动实现 Event trait
impl Event for UserLoggedIn {
    fn event_name() -> &'static str {
        "user.logged_in"
    }
    fn topic() -> &'static str {
        "user"
    }
}

#[derive(Clone, Debug)]
struct PaymentReceived {
    order_id: u64,
    amount: f64,
    currency: String,
}

impl Event for PaymentReceived {
    fn event_name() -> &'static str {
        "payment.received"
    }
    fn topic() -> &'static str {
        "payment"
    }
}

// ── 2. 使用 EventBus ──────────────────────────────────────────

#[tokio::main]
async fn main() {
    println!("=== 基础用法：手动 Event 实现 ===\n");

    // 创建 EventBus（零依赖，纯内存）
    let bus = EventBus::new();

    // ── 订阅事件 ────────────────────────────────────────────
    // subscribe() 接受一个 async 闭包，闭包参数类型决定订阅哪种事件

    bus.subscribe(|e: UserLoggedIn| async move {
        println!("🔐 用户登录: id={}, ip={}", e.user_id, e.ip_address);
        Ok(())
    }).await.unwrap();

    bus.subscribe(|e: PaymentReceived| async move {
        println!("💰 收到支付: order={}, amount={} {}", e.order_id, e.amount, e.currency);
        Ok(())
    }).await.unwrap();

    // 同一个事件可以有多个订阅者
    let login_count = Arc::new(AtomicUsize::new(0));
    let counter = login_count.clone();
    bus.subscribe(move |_: UserLoggedIn| {
        let counter = counter.clone();
        async move {
            let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
            println!("📊 登录计数器: 累计 {} 次", n);
            Ok(())
        }
    }).await.unwrap();

    // 等待订阅者就绪
    tokio::time::sleep(Duration::from_millis(50)).await;

    // ── 发布事件 ────────────────────────────────────────────
    println!("--- 发布事件 ---");

    bus.publish(UserLoggedIn {
        user_id: 1,
        ip_address: "192.168.1.100".into(),
    }).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    bus.publish(PaymentReceived {
        order_id: 1001,
        amount: 299.99,
        currency: "CNY".into(),
    }).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    bus.publish(UserLoggedIn {
        user_id: 2,
        ip_address: "10.0.0.1".into(),
    }).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    println!();
    println!("✅ 总登录次数: {}", login_count.load(Ordering::SeqCst));

    // ── Clone 共享状态 ──────────────────────────────────────
    println!();
    println!("--- Clone 共享 ---");
    println!("EventBus 实现了 Clone，clone 后共享内部状态：");

    let bus2 = bus.clone();
    bus2.publish(UserLoggedIn {
        user_id: 3,
        ip_address: "172.16.0.1".into(),
    }).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    println!("✅ 总登录次数（包括 clone 发布的）: {}", login_count.load(Ordering::SeqCst));
}
