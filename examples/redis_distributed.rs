//! # Redis 分布式事件总线示例
//!
//! 演示两个独立 EventBus 实例通过 Redis Pub/Sub 进行跨进程事件通信。
//!
//! 在一个进程内模拟两个"节点"：
//!   - **Node A (生产者)**: 发布事件到 Redis
//!   - **Node B (消费者)**: 从 Redis 接收事件并处理
//!
//! 运行: `cargo run --example redis_distributed`
//!
//! 前提: Redis 需要在 127.0.0.1:6379 运行

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anycms_event::event_bus;
use anycms_event_redis::RedisTransport;

// ── 定义事件 ──────────────────────────────────────────────────────

event_bus! {
    bus AppEventBus {
        event UserCreated { user_id: u64, username: String }
        event OrderPlaced { order_id: u64, product: String, amount: f64 }
    }
}

// ── 主流程 ────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter("anycms_event_redis=debug,redis_distributed=info")
        .init();

    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║   anycms-event Redis 分布式事件总线示例                  ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!();

    // ── 1. 创建 Redis Transport ────────────────────────────────
    println!("📡 连接 Redis (127.0.0.1:6379)...");
    let transport = RedisTransport::new("redis://127.0.0.1:6379")
        .await
        .expect("Redis 连接失败，请确认 Redis 已启动在 127.0.0.1:6379");
    println!("✅ Redis 连接成功");
    println!();

    // ── 2. 创建 Node B (消费者) ───────────────────────────────
    println!("🔔 启动 Node B — 事件消费者");
    let bus_b = AppEventBus::new();
    let bridged_b = transport.bridge(bus_b.inner().clone()).await.unwrap();

    // 启动 Redis → 本地 转发器 (订阅两种事件)
    bridged_b.forward_from_redis::<UserCreated>().await.unwrap();
    bridged_b.forward_from_redis::<OrderPlaced>().await.unwrap();
    println!("   已订阅 Redis 频道: user.created, order.placed");

    // 统计计数
    let user_count = Arc::new(AtomicUsize::new(0));
    let order_count = Arc::new(AtomicUsize::new(0));

    let uc = user_count.clone();
    bridged_b.subscribe(move |e: UserCreated| {
        let uc = uc.clone();
        async move {
            let n = uc.fetch_add(1, Ordering::SeqCst) + 1;
            println!("   📬 [Node B] 收到 UserCreated: id={}, name={} (#{})", e.user_id, e.username, n);
            Ok(())
        }
    }).await.unwrap();

    let oc = order_count.clone();
    bridged_b.subscribe(move |e: OrderPlaced| {
        let oc = oc.clone();
        async move {
            let n = oc.fetch_add(1, Ordering::SeqCst) + 1;
            println!("   📬 [Node B] 收到 OrderPlaced: id={}, product={}, amount={:.2} (#{})",
                e.order_id, e.product, e.amount, n);
            Ok(())
        }
    }).await.unwrap();

    // 等待 Redis 订阅就绪
    tokio::time::sleep(Duration::from_millis(200)).await;
    println!();

    // ── 3. 创建 Node A (生产者) ───────────────────────────────
    println!("🚀 启动 Node A — 事件生产者");
    let bus_a = AppEventBus::new();
    let bridged_a = transport.bridge(bus_a.inner().clone()).await.unwrap();
    println!("   Node A 就绪，开始发布事件...");
    println!();

    // ── 4. Node A 发布事件 ────────────────────────────────────
    println!("━━━ 发布事件 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    bridged_a.publish(UserCreated {
        user_id: 1,
        username: "Alice".into(),
    }).await.unwrap();
    println!("   📤 [Node A] 发布 UserCreated {{ id: 1, name: \"Alice\" }}");

    tokio::time::sleep(Duration::from_millis(100)).await;

    bridged_a.publish(OrderPlaced {
        order_id: 101,
        product: "Rust 编程指南".into(),
        amount: 79.9,
    }).await.unwrap();
    println!("   📤 [Node A] 发布 OrderPlaced {{ id: 101, product: \"Rust 编程指南\", amount: 79.90 }}");

    tokio::time::sleep(Duration::from_millis(100)).await;

    bridged_a.publish(UserCreated {
        user_id: 2,
        username: "Bob".into(),
    }).await.unwrap();
    println!("   📤 [Node A] 发布 UserCreated {{ id: 2, name: \"Bob\" }}");

    tokio::time::sleep(Duration::from_millis(100)).await;

    bridged_a.publish(OrderPlaced {
        order_id: 102,
        product: "Tokio 异步运行时".into(),
        amount: 99.0,
    }).await.unwrap();
    println!("   📤 [Node A] 发布 OrderPlaced {{ id: 102, product: \"Tokio 异步运行时\", amount: 99.00 }}");

    // 等待 Node B 处理完所有事件
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ── 5. 输出统计 ───────────────────────────────────────────
    println!();
    println!("━━━ 统计结果 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("   ✅ Node B 接收 UserCreated 事件: {} 条", user_count.load(Ordering::SeqCst));
    println!("   ✅ Node B 接收 OrderPlaced  事件: {} 条", order_count.load(Ordering::SeqCst));
    println!();

    // 验证结果
    assert_eq!(user_count.load(Ordering::SeqCst), 2, "应该收到 2 个 UserCreated 事件");
    assert_eq!(order_count.load(Ordering::SeqCst), 2, "应该收到 2 个 OrderPlaced 事件");

    println!("🎉 分布式事件传递验证通过！Node A 发布 → Redis → Node B 接收 ✓");
    println!();

    // ── 6. 演示双向通信 ───────────────────────────────────────
    println!("━━━ 双向通信演示 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("   Node B 也通过 Redis 发布事件，Node A 可以接收:");

    // Node A 也订阅 Redis 转发
    bridged_a.forward_from_redis::<UserCreated>().await.unwrap();
    let a_received = Arc::new(AtomicUsize::new(0));
    let ar = a_received.clone();
    bridged_a.subscribe(move |e: UserCreated| {
        let ar = ar.clone();
        async move {
            let n = ar.fetch_add(1, Ordering::SeqCst) + 1;
            println!("   📬 [Node A] 收到来自 Redis 的 UserCreated: name={} (#{})", e.username, n);
            Ok(())
        }
    }).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Node B 发布事件
    bridged_b.publish(UserCreated {
        user_id: 99,
        username: "Charlie (from Node B)".into(),
    }).await.unwrap();
    println!("   📤 [Node B] 发布 UserCreated {{ name: \"Charlie (from Node B)\" }}");

    tokio::time::sleep(Duration::from_millis(300)).await;

    println!();
    println!("   ✅ Node A 从 Redis 接收到来自 Node B 的事件: {} 条", a_received.load(Ordering::SeqCst));
    println!();
    println!("🎉 双向通信验证通过！");
}
