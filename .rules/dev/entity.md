# SEA-ORM 实体模型示例代码

不用系统外键关联，使用 xx_id 字段逻辑关联，不使用实体关系
id 字段使用 String 类型
日期类型使用 i64 类型
## Entity 示例代码
```rust
use async_trait::async_trait;
use sea_orm::{ActiveValue::Set, entity::prelude::*};
use uuid::Uuid;
use std::time::{SystemTime, UNIX_EPOCH};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, serde::Serialize)]
#[sea_orm(table_name = "sessions")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub title: String,
    pub model: String,
    pub device_id: String,
    /// 来源: user, mcp
    pub source: String,
    /// 状态: 0-初始化, 1-进行中, 2-完成, 3-错误
    pub status: i32,
    /// 错误原因
    pub error_reason: Option<String>,
    /// 完成时间
    pub completed_time: Option<i64>,
    pub created_time: i64,
    pub updated_time: i64,
}
#[async_trait]
impl ActiveModelBehavior for ActiveModel {
    fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        Self {
            id: Set(Uuid::new_v4().to_string()),
            source: Set("user".to_string()), // 默认来源为 user
            status: Set(0), // 初始化状态
            error_reason: Set(None),
            completed_time: Set(None),
            created_time: Set(now),
            updated_time: Set(now),
            ..ActiveModelTrait::default()
        }
    }

    async fn before_save<C>(self, _db: &C, _insert: bool) -> Result<Self, DbErr>
    where
        C: ConnectionTrait,
    {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        Ok(Self {
            updated_time: Set(now),
            ..self
        })
    }
}
```