//! # Topic 通配符订阅
//!
//! 演示 event_bus! 宏中的 topic 分组和通配符订阅功能。
//!
//! 运行: `cargo run --example topic_subscription`

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anycms_event::event_bus;

// ── 定义事件总线 ──────────────────────────────────────────────
// event_bus! 宏一次性定义所有事件 + topic 分组

event_bus! {
    bus ShopEventBus {
        // 用户相关事件
        event UserRegistered { user_id: u64, email: String }
        event UserActivated  { user_id: u64 }

        // 订单相关事件
        event OrderCreated { order_id: u64, user_id: u64, total: f64 }
        event OrderShipped { order_id: u64, tracking: String }
        event OrderCancelled { order_id: u64, reason: String }

        // topic 分组定义
        topic user_events => [UserRegistered, UserActivated]
        topic orders => [OrderCreated, OrderShipped, OrderCancelled]
    }
}

#[tokio::main]
async fn main() {
    println!("=== Topic 通配符订阅 ===\n");

    let bus = ShopEventBus::new();

    // ── 1. 精确订阅 — 只监听一种事件 ──────────────────────
    println!("📌 精确订阅: UserRegistered");
    bus.subscribe(|e: UserRegistered| async move {
        println!("   📧 发送欢迎邮件给: {} (id={})", e.email, e.user_id);
        Ok(())
    }).await.unwrap();

    // ── 2. Topic 通配符订阅 — 监听一组事件 ─────────────────
    // subscribe_topic_user_events() 订阅 "user.*" 下的所有事件
    // handler 接受 ShopEventBusTopicEvent 枚举
    // 返回 Vec<Result<Subscription>>，每个事件类型一个订阅

    println!("📌 Topic 订阅: user.* (UserRegistered + UserActivated)");
    let _user_subs = bus.subscribe_topic_user_events(|e: ShopEventBusTopicEvent| async move {
        match e {
            ShopEventBusTopicEvent::UserRegistered(ev) => {
                println!("   📋 [用户模块] 注册: id={}, email={}", ev.user_id, ev.email);
            }
            ShopEventBusTopicEvent::UserActivated(ev) => {
                println!("   📋 [用户模块] 激活: id={}", ev.user_id);
            }
            _ => {} // 其他 user 事件
        }
        Ok(())
    }).await;

    println!("📌 Topic 订阅: order.* (OrderCreated + OrderShipped + OrderCancelled)");
    let order_count = Arc::new(AtomicUsize::new(0));
    let order_count_display = order_count.clone();
    bus.subscribe_topic_orders(move |e: ShopEventBusTopicEvent| {
        let oc = order_count.clone();
        async move {
            let n = oc.fetch_add(1, Ordering::SeqCst) + 1;
            match e {
                ShopEventBusTopicEvent::OrderCreated(ev) => {
                    println!("   📦 [订单模块] 创建: order={}, user={}, total={:.2} (#{})",
                        ev.order_id, ev.user_id, ev.total, n);
                }
                ShopEventBusTopicEvent::OrderShipped(ev) => {
                    println!("   🚚 [订单模块] 发货: order={}, tracking={} (#{})",
                        ev.order_id, ev.tracking, n);
                }
                ShopEventBusTopicEvent::OrderCancelled(ev) => {
                    println!("   ❌ [订单模块] 取消: order={}, reason={} (#{})",
                        ev.order_id, ev.reason, n);
                }
                _ => {}
            }
            Ok(())
        }
    }).await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // ── 3. 发布事件，观察分发 ──────────────────────────────
    println!();
    println!("--- 发布事件 ---\n");

    // UserRegistered → 精确订阅 + topic "user.*" 都会收到
    bus.publish(UserRegistered {
        user_id: 1,
        email: "alice@example.com".into(),
    }).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    println!();

    // UserActivated → 只有 topic "user.*" 会收到
    bus.publish(UserActivated { user_id: 1 }).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    println!();

    // OrderCreated → 只有 topic "order.*" 会收到
    bus.publish(OrderCreated {
        order_id: 1001,
        user_id: 1,
        total: 299.0,
    }).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    println!();

    bus.publish(OrderShipped {
        order_id: 1001,
        tracking: "SF1234567890".into(),
    }).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    println!();

    bus.publish(OrderCancelled {
        order_id: 1002,
        reason: "用户取消".into(),
    }).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    println!();
    println!("--- 统计 ---");
    println!("✅ Topic order.* 共收到 {} 条事件", order_count_display.load(Ordering::SeqCst));

    // ── 4. Topic 匹配规则说明 ──────────────────────────────
    println!();
    println!("--- Topic 匹配规则 ---");
    println!("  event_bus! 宏中定义:");
    println!("    topic \"user.*\"  => [UserRegistered, UserActivated]");
    println!("    topic \"order.*\" => [OrderCreated, OrderShipped, OrderCancelled]");
    println!();
    println!("  通配符规则 (topic::matches):");
    println!("    \"user.*\"  匹配单层: user.created ✓, user.foo.bar ✗");
    println!("    \"user.**\" 匹配多层: user.created ✓, user.foo.bar ✓");
    println!("    \"**\"       匹配所有");
}
