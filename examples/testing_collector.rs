//! # 测试工具 — EventCollector
//!
//! 演示如何使用 `EventCollector` 收集和断言事件，替代不可靠的 `sleep()` 等待。
//!
//! 运行: `cargo run --example testing_collector --features testing`

use std::time::Duration;

use anycms_event::prelude::*;
use anycms_event::testing::EventCollector;

// ── 1. 定义事件结构体 ──────────────────────────────────────────

#[derive(Clone, Debug)]
struct OrderPlaced {
    order_id: u64,
    product: String,
    quantity: u32,
}

impl Event for OrderPlaced {
    fn event_name() -> &'static str {
        "order.placed"
    }
    fn topic() -> &'static str {
        "order"
    }
}

// ── 2. 创建 EventBus 和 EventCollector ─────────────────────────

#[tokio::main]
async fn main() {
    println!("=== 测试工具：EventCollector ===\n");

    let bus = EventBus::new();

    // EventCollector 订阅指定类型的事件，在 new() 返回前即已生效
    let collector = EventCollector::<OrderPlaced>::new(&bus).await;

    // ── 3. 发布若干事件 ──────────────────────────────────────
    println!("📦 发布订单事件...\n");

    bus.publish(OrderPlaced {
        order_id: 1001,
        product: "Rust Book".into(),
        quantity: 2,
    })
    .await
    .unwrap();

    bus.publish(OrderPlaced {
        order_id: 1002,
        product: "Mechanical Keyboard".into(),
        quantity: 1,
    })
    .await
    .unwrap();

    bus.publish(OrderPlaced {
        order_id: 1003,
        product: "USB-C Hub".into(),
        quantity: 3,
    })
    .await
    .unwrap();

    // ── 4. wait_for() — 等待指定数量的事件 ─────────────────────
    println!("⏳ 使用 wait_for() 等待 3 个事件（超时 5 秒）...");

    let events = collector.wait_for(3, Duration::from_secs(5)).await;
    println!("✅ 收集到 {} 个事件:\n", events.len());
    for e in &events {
        println!(
            "   📋 order_id={}, product=\"{}\", quantity={}",
            e.order_id, e.product, e.quantity
        );
    }
    println!();

    // ── 5. collect_now() — 获取当前快照 ────────────────────────
    println!("📸 使用 collect_now() 获取当前快照...");
    let snapshot = collector.collect_now();
    println!("✅ 快照中有 {} 个事件\n", snapshot.len());

    // ── 6. assert_count() — 断言事件数量 ──────────────────────
    println!("🔢 使用 assert_count() 断言事件数量...");
    collector.assert_count(3);
    println!("✅ assert_count(3) 通过\n");

    // ── 7. assert_contains() — 断言包含匹配条件的事件 ──────────
    println!("🔍 使用 assert_contains() 断言包含特定产品...");
    collector.assert_contains(|e| e.product == "Rust Book");
    println!("✅ assert_contains(|e| e.product == \"Rust Book\") 通过");

    collector.assert_contains(|e| e.order_id == 1002);
    println!("✅ assert_contains(|e| e.order_id == 1002) 通过\n");

    // ── 8. assert_not_contains() — 断言不包含匹配条件的事件 ────
    println!("🚫 使用 assert_not_contains() 断言不包含特定产品...");
    collector.assert_not_contains(|e| e.product == "Nonexistent");
    println!("✅ assert_not_contains(|e| e.product == \"Nonexistent\") 通过\n");

    // ── 9. 总结 ──────────────────────────────────────────────
    println!("--- 总结 ---");
    println!("💡 EventCollector 核心能力:");
    println!("   - wait_for(n, timeout) → 等待 n 个事件，替代不可靠的 sleep()");
    println!("   - collect_now()        → 立即获取当前所有事件的快照");
    println!("   - assert_count(n)      → 断言收集到 n 个事件");
    println!("   - assert_contains(pred) → 断言存在满足条件的事件");
    println!("   - assert_not_contains(pred) → 断言不存在满足条件的事件");
    println!();
    println!("🎉 示例完成！");
}
