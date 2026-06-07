//! # 真实场景 — 用户注册流程的事件驱动架构
//!
//! 模拟一个典型的用户注册流程，展示事件总线如何解耦业务逻辑：
//!   1. 用户提交注册 → 发布 UserRegistered
//!   2. 多个模块各自订阅并独立处理：
//!      - 邮件服务：发送验证邮件
//!      - 审计模块：记录操作日志
//!      - 积分系统：发放新用户积分
//!      - 统计服务：更新注册计数
//!
//! 运行: `cargo run --example real_world`

use std::sync::Arc;
use std::time::Duration;

use anycms_event::event_bus;

// ── 事件定义 ──────────────────────────────────────────────────

event_bus! {
    bus UserEventBus {
        // 用户注册
        event UserRegistered { user_id: String, email: String, username: String }
        // 邮件验证完成
        event EmailVerified { user_id: String }
        // 用户资料完善
        event ProfileCompleted { user_id: String, level: u32 }

        // topic 分组：审计模块一次订阅所有用户事件
        topic user_events => [UserRegistered, EmailVerified, ProfileCompleted]
    }
}

// ── 模拟服务 ──────────────────────────────────────────────────

struct AuditService;
impl AuditService {
    async fn record(event: &str, user_id: &str, detail: &str) {
        // 模拟数据库写入延迟
        tokio::time::sleep(Duration::from_millis(10)).await;
        println!("   🔒 [审计] {} | user={} | {}", event, user_id, detail);
    }
}

struct EmailService;
impl EmailService {
    async fn send_welcome(email: &str, username: &str) {
        tokio::time::sleep(Duration::from_millis(20)).await;
        println!("   📧 [邮件] 欢迎邮件已发送: {} -> {}", username, email);
    }

    async fn send_verified(user_id: &str) {
        tokio::time::sleep(Duration::from_millis(10)).await;
        println!("   📧 [邮件] 验证确认邮件已发送: user={}", user_id);
    }
}

struct PointsService;
impl PointsService {
    async fn grant_welcome_bonus(user_id: &str) {
        tokio::time::sleep(Duration::from_millis(5)).await;
        println!("   ⭐ [积分] 新用户奖励 +100 积分: user={}", user_id);
    }

    async fn grant_profile_bonus(user_id: &str, level: u32) {
        tokio::time::sleep(Duration::from_millis(5)).await;
        let bonus = level * 50;
        println!("   ⭐ [积分] 资料完善奖励 +{} 积分: user={}", bonus, user_id);
    }
}

struct StatsService {
    registered: Arc<std::sync::atomic::AtomicUsize>,
    verified: Arc<std::sync::atomic::AtomicUsize>,
    completed: Arc<std::sync::atomic::AtomicUsize>,
}

impl StatsService {
    fn new() -> Self {
        Self {
            registered: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            verified: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            completed: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    fn report(&self) {
        println!("   📊 [统计] 注册={}, 验证={}, 完善={}",
            self.registered.load(std::sync::atomic::Ordering::SeqCst),
            self.verified.load(std::sync::atomic::Ordering::SeqCst),
            self.completed.load(std::sync::atomic::Ordering::SeqCst));
    }
}

// ── 主流程 ────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║   真实场景：用户注册流程的事件驱动架构                    ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!();

    let bus = UserEventBus::new();
    let stats = StatsService::new();

    // ── 注册各模块的事件处理器 ──────────────────────────────

    // 1) 邮件服务 → 监听注册和验证事件
    bus.subscribe(|e: UserRegistered| async move {
        EmailService::send_welcome(&e.email, &e.username).await;
        Ok(())
    }).await.unwrap();

    bus.subscribe(|e: EmailVerified| async move {
        EmailService::send_verified(&e.user_id).await;
        Ok(())
    }).await.unwrap();

    // 2) 审计服务 → 监听所有用户事件（通过 topic）
    bus.subscribe_topic_user_events(|e: UserEventBusTopicEvent| async move {
        match e {
            UserEventBusTopicEvent::UserRegistered(ev) => {
                AuditService::record("用户注册", &ev.user_id, &format!("email={}", ev.email)).await;
            }
            UserEventBusTopicEvent::EmailVerified(ev) => {
                AuditService::record("邮件验证", &ev.user_id, "验证通过").await;
            }
            UserEventBusTopicEvent::ProfileCompleted(ev) => {
                AuditService::record("资料完善", &ev.user_id, &format!("level={}", ev.level)).await;
            }
        }
        Ok(())
    }).await;

    // 3) 积分服务 → 监听注册和资料完善
    bus.subscribe(|e: UserRegistered| async move {
        PointsService::grant_welcome_bonus(&e.user_id).await;
        Ok(())
    }).await.unwrap();

    bus.subscribe(|e: ProfileCompleted| async move {
        PointsService::grant_profile_bonus(&e.user_id, e.level).await;
        Ok(())
    }).await.unwrap();

    // 4) 统计服务 → 监听所有事件
    let reg = stats.registered.clone();
    bus.subscribe(move |_: UserRegistered| {
        let reg = reg.clone();
        async move { reg.fetch_add(1, std::sync::atomic::Ordering::SeqCst); Ok(()) }
    }).await.unwrap();

    let ver = stats.verified.clone();
    bus.subscribe(move |_: EmailVerified| {
        let ver = ver.clone();
        async move { ver.fetch_add(1, std::sync::atomic::Ordering::SeqCst); Ok(()) }
    }).await.unwrap();

    let comp = stats.completed.clone();
    bus.subscribe(move |_: ProfileCompleted| {
        let comp = comp.clone();
        async move { comp.fetch_add(1, std::sync::atomic::Ordering::SeqCst); Ok(()) }
    }).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // ── 模拟用户注册流程 ───────────────────────────────────

    println!("━━━ 用户 Alice 注册 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    bus.publish(UserRegistered {
        user_id: "u_001".into(),
        email: "alice@example.com".into(),
        username: "Alice".into(),
    }).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    println!();

    println!("━━━ Alice 验证邮箱 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    bus.publish(EmailVerified { user_id: "u_001".into() }).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    println!();

    println!("━━━ Alice 完善资料 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    bus.publish(ProfileCompleted { user_id: "u_001".into(), level: 3 }).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    println!();

    println!("━━━ 用户 Bob 注册 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    bus.publish(UserRegistered {
        user_id: "u_002".into(),
        email: "bob@example.com".into(),
        username: "Bob".into(),
    }).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    println!();

    // ── 最终统计 ───────────────────────────────────────────
    println!("━━━ 最终统计 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    stats.report();
    println!();
    println!("💡 架构优势:");
    println!("   - 各模块独立订阅，互不干扰");
    println!("   - 新增模块只需 subscribe，无需修改发布端");
    println!("   - topic 订阅让审计服务一次监听所有用户事件");
    println!("   - EventBus clone 后可安全跨 task/线程共享");
}
