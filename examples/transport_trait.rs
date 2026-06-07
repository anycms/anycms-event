//! # Transport Trait 抽象示例
//!
//! 演示如何使用 Transport trait 抽象来支持不同的消息传输后端。
//!
//! 本示例展示:
//! - Transport trait 的核心 API
//! - 如何使用 trait 对象 (`Box<dyn Transport>`) 实现运行时多态
//! - RedisTransport 实现 Transport trait
//! - clone_box() 方法用于克隆 trait 对象
//! - 如何为自定义后端实现 Transport trait
//!
//! 运行: `cargo check --example transport_trait`
//!
//! 注意: 这是一个 API 演示示例，不需要运行中的 Redis 实例。

use anycms_event::transport::{Transport, TransportError};

// Import is used in the example code demonstration output
#[allow(unused_imports)]
use anycms_event_redis::RedisTransport;

// ── 示例 1: 使用 RedisTransport 作为 trait 对象 ─────────────────────────

/// 演示将 RedisTransport 作为 trait 对象使用
async fn use_redis_as_trait_object() {
    println!("━━━ 示例 1: RedisTransport 作为 trait 对象 ━━━━━━━━━━━━━━━");
    println!();

    // 注意: 这里不实际连接 Redis，仅演示 API
    println!("// 创建 RedisTransport (需要 Redis 连接)");
    println!("let redis_transport = RedisTransport::new(\"redis://127.0.0.1:6379\").await.unwrap();");
    println!();

    println!("// 将 RedisTransport 转换为 trait 对象");
    println!("let transport: Box<dyn Transport> = Box::new(redis_transport);");
    println!();

    println!("// 通过 trait 对象调用 publish() 方法");
    println!("transport.publish(");
    println!("    \"user.created\",");
    println!("    r#{{\"user_id\":1,\"username\":\"Alice\"}}#");
    println!(").await.unwrap();");
    println!();

    println!("✅ Transport trait 允许通过 `Box<dyn Transport>` 接口使用 RedisTransport");
    println!("   这意味着可以在运行时切换不同的传输实现，而无需修改业务代码");
    println!();
}

// ── 示例 2: trait 对象的克隆 ───────────────────────────────────────────

/// 演示使用 clone_box() 方法克隆 trait 对象
async fn clone_transport_trait_object() {
    println!("━━━ 示例 2: 克隆 Transport trait 对象 ━━━━━━━━━━━━━━━━━━━━━");
    println!();

    println!("// trait 对象不能直接实现 Clone trait");
    println!("// 必须使用 clone_box() 方法");
    println!();
    println!("let transport: Box<dyn Transport> = Box::new(redis_transport);");
    println!("let cloned_transport = transport.clone_box();");
    println!();

    println!("✅ clone_box() 返回一个新的 Box<dyn Transport>");
    println!("   底层实现通过 Box::new(self.clone()) 完成");
    println!();
}

// ── 示例 3: 将 trait 对象传递给函数 ─────────────────────────────────────

/// 接受任意 Transport 实现的函数
#[allow(dead_code)]
async fn publish_event(transport: &dyn Transport, event_name: &str, payload: &str) {
    match transport.publish(event_name, payload).await {
        Ok(_) => println!("✅ 事件发布成功"),
        Err(e) => println!("❌ 事件发布失败: {}", e),
    }
}

/// 演示将 trait 对象传递给接受 trait bound 的函数
async fn pass_transport_to_function() {
    println!("━━━ 示例 3: 将 trait 对象传递给函数 ━━━━━━━━━━━━━━━━━━━━");
    println!();

    println!("// 定义一个接受任意 Transport 的函数");
    println!("async fn publish_event(transport: &dyn Transport, event_name: &str, payload: &str) {{");
    println!("    transport.publish(event_name, payload).await");
    println!("}}");
    println!();

    println!("// 可以传递任何实现了 Transport trait 的类型");
    println!("let redis_transport = RedisTransport::new(...).await.unwrap();");
    println!("publish_event(&redis_transport, \"order.created\", payload).await;");
    println!();

    println!("// 也可以传递 trait 对象");
    println!("let transport: Box<dyn Transport> = Box::new(redis_transport);");
    println!("publish_event(&*transport, \"payment.received\", payload).await;");
    println!();

    println!("✅ 这种模式允许编写与具体传输实现解耦的代码");
    println!();
}

// ── 示例 4: 运行时多态 ─────────────────────────────────────────────────

/// 演示在运行时选择不同的传输实现
async fn runtime_polymorphism() {
    println!("━━━ 示例 4: 运行时多态 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!();

    println!("// 根据配置选择传输实现");
    println!("let transport: Box<dyn Transport> = match config.transport_type {{");
    println!("    TransportType::Redis => {{");
    println!("        Box::new(RedisTransport::new(&config.redis_url).await.unwrap())");
    println!("    }}");
    println!("    TransportType::Kafka => {{");
    println!("        Box::new(KafkaTransport::new(&config.kafka_brokers).await.unwrap())");
    println!("    }}");
    println!("    TransportType::Nats => {{");
    println!("        Box::new(NatsTransport::new(&config.nats_url).await.unwrap())");
    println!("    }}");
    println!("}};");
    println!();

    println!("// 后续代码完全不需要知道具体使用的是哪个传输实现");
    println!("transport.publish(event_name, payload).await.unwrap();");
    println!();

    println!("✅ 可以在运行时根据配置或环境切换传输后端");
    println!();
}

// ── 示例 5: 实现自定义 Transport ────────────────────────────────────────

/// 演示如何为自定义后端实现 Transport trait (注释掉的骨架)
fn show_custom_transport_skeleton() {
    println!("━━━ 示例 5: 实现自定义 Transport ━━━━━━━━━━━━━━━━━━━━");
    println!();

    println!("```rust");
    println!("use anycms_event::transport::{{Transport, TransportError, TransportFuture}};");
    println!();
    println!("// 自定义传输实现 (例如: Kafka, NATS, RabbitMQ 等)");
    println!("pub struct MyCustomTransport {{");
    println!("    // ... 字段定义");
    println!("}}");
    println!();
    println!("impl MyCustomTransport {{");
    println!("    pub async fn new(config: &str) -> Result<Self, Error> {{");
    println!("        // 初始化连接等");
    println!("        Ok(Self {{ }})");
    println!("    }}");
    println!("}}");
    println!();
    println!("// 实现 Clone (clone_box 需要这个)");
    println!("impl Clone for MyCustomTransport {{");
    println!("    fn clone(&self) -> Self {{");
    println!("        Self {{ }}");
    println!("    }}");
    println!("}}");
    println!();
    println!("// 实现 Transport trait");
    println!("impl Transport for MyCustomTransport {{");
    println!("    fn publish(&self, event_name: &str, payload: &str) -> TransportFuture<'_> {{");
    println!("        let event_name = event_name.to_string();");
    println!("        let payload = payload.to_string();");
    println!("        Box::pin(async move {{");
    println!("            // 将 payload 发布到自定义后端");
    println!("            // event_name 可以用作路由键");
    println!("            // 返回 Ok(()) 或 Err(TransportError::Publish(...))");
    println!("            Ok(())");
    println!("        }})");
    println!("    }}");
    println!();
    println!("    fn clone_box(&self) -> Box<dyn Transport> {{");
    println!("        Box::new(self.clone())");
    println!("    }}");
    println!("}}");
    println!("```");
    println!();

    println!("💡 只需实现这两个方法，任何消息后端都可以与 EventBus 集成");
    println!();
}

// ── 示例 6: 类型擦除与依赖注入 ─────────────────────────────────────────

/// 演示如何在应用结构体中使用 trait 对象
#[allow(dead_code)]
struct Application {
    transport: Box<dyn Transport>,
}

#[allow(dead_code)]
impl Application {
    fn new(transport: Box<dyn Transport>) -> Self {
        Self { transport }
    }

    async fn publish_event(&self, event_name: &str, payload: &str) -> Result<(), TransportError> {
        self.transport.publish(event_name, payload).await
    }
}

async fn dependency_injection_example() {
    println!("━━━ 示例 6: 依赖注入模式 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!();

    println!("```rust");
    println!("struct Application {{");
    println!("    transport: Box<dyn Transport>,");
    println!("}}");
    println!();
    println!("impl Application {{");
    println!("    fn new(transport: Box<dyn Transport>) -> Self {{");
    println!("        Self {{ transport }}");
    println!("    }}");
    println!();
    println!("    async fn publish_event(&self, event_name: &str, payload: &str)");
    println!("        -> Result<(), TransportError>");
    println!("    {{");
    println!("        self.transport.publish(event_name, payload).await");
    println!("    }}");
    println!("}}");
    println!("```");
    println!();

    println!("// 使用时注入具体的传输实现");
    println!("let app = Application::new(Box::new(redis_transport));");
    println!("// 或者");
    println!("let app = Application::new(Box::new(kafka_transport));");
    println!();

    println!("✅ 应用代码与传输实现完全解耦，便于测试和切换实现");
    println!();
}

// ── 主流程 ────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter("transport_trait=info")
        .init();

    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║   anycms-event Transport Trait 抽象示例                 ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!();

    println!("💡 Transport trait 为分布式事件总线提供统一的传输抽象");
    println!("   允许在不同的消息后端 (Redis, Kafka, NATS 等) 之间无缝切换");
    println!();

    use_redis_as_trait_object().await;
    clone_transport_trait_object().await;
    pass_transport_to_function().await;
    runtime_polymorphism().await;
    show_custom_transport_skeleton();
    dependency_injection_example().await;

    println!("━━━ 总结 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!();
    println!("Transport trait 的核心优势:");
    println!("  1. ✅ 抽象化: 统一不同消息后端的接口");
    println!("  2. ✅ 多态性: 使用 trait 对象实现运行时多态");
    println!("  3. ✅ 可扩展性: 轻松添加新的传输实现");
    println!("  4. ✅ 可测试性: 注入 mock 实现进行单元测试");
    println!("  5. ✅ 解耦: 业务代码与传输实现完全解耦");
    println!();
    println!("🔧 核心方法:");
    println!("  • publish(event_name, payload) -> TransportFuture");
    println!("  • clone_box() -> Box<dyn Transport>");
    println!();
    println!("📚 已实现的传输:");
    println!("  • RedisTransport (anycms-event-redis)");
    println!("  • 可自行实现: Kafka, NATS, RabbitMQ, SQS 等");
    println!();
}
