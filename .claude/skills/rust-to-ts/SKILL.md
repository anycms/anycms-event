---
name: "rust-to-ts"
description: "Convert Rust code to TypeScript, 转化 rust 的 model , dto / request /response 实体为 typescript"
---

- 通过执行 cargo test export_bindings 生成 bindings 目录下的文件
- 合理命名,文件名kebab-case，类名PascalCase，SEA-ORM 的 model ，Rust 中统一都是 pub struct Model, 所以 typescript 必须重命名
- serde_json::Value 的字段使用  #[ts(type = "unknown")] pub metadata: Option<Value>


## Model 示例
```rust
#[sea_orm::model]
#[derive(
    Clone, Debug, PartialEq, Eq, DeriveEntityModel, serde::Serialize, serde::Deserialize, ts_rs::TS,
)]
#[sea_orm(table_name = "agent_agent_meta")]
#[ts(export, rename = "AgentMeta", export_to = "model/agentmeta.ts")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,

    /// 名称
    pub title: String,

    /// 图标
    pub icon: Option<String>,

    /// 描述信息
    pub description: Option<String>,

    /// 执行程序
    pub exec: Option<String>,

    /// Agent 镜像
    pub image: Option<String>,

    /// 创建时间
    #[sea_orm(column_name = "created_time")]
    pub created_time: i64,

    /// 更新时间
    #[sea_orm(column_name = "updated_time")]
    pub updated_time: i64,
}
```

## DTO 示例
```rust
#[derive(Debug, Clone, Serialize, Deserialize, Default,ts_rs::TS)]
#[ts(export,export_to="dto/chat-completion-response.ts")]
pub struct ChatCompletionResponse {
    /// 响应ID
    pub id: String,
    /// 对象类型
    pub object: Option<String>,
    /// 创建时间戳
    pub created: i64,
    /// 使用的模型
    pub model: Option<String>,
    /// 选择列表
    pub choices: Vec<Choice>,
    // 通知信息
    pub notifications: Option<Vec<Notification>>,
    /// 系统指纹
    pub system_fingerprint: Option<String>,
    /// 使用情况统计
    pub usage: Option<Usage>,
}

```

