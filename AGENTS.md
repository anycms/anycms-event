# Rust 开发约定

## 项目开发约定
- 不修改 Cargo.toml ,如需添加修改依赖，需要用户手动添加
- 使用 anyhow 和 thiserror 进行错误处理
- 使用 tokio 作为异步运行时
- 使用 cargo check 检测代码错误
- 使用 reqwest 请求 API
- orm 使用 sea-orm 2.0.0 版本
- 异步 trait 使用 async-trait 实现
- 使用 cargo doc {crate} 来生成文档

## web 项目开发约定
- 使用 actix-web 框架
- 模块目录结构: module/web/ 存放 api 接口代码, module/service/ 存放业务代码 , module/model/ 存放数据模型代码
- 在 module/mod.rs 中 web_config 注册接口
- api 接口编写符合 openapi 规范的注释
- api 接口返回
```rust
use actix_web::{HttpServer, Responder};
use app_core::DefaultResult;
返回类型 DefaultResult<impl Responder> 
```
- 在 @/docs/api/ 文件夹中添加 模块.http 测试文件
- 项目监听为: http://0.0.0.0:8081

## 具体开发参考
- 实体设计 @/.rules/dev/entity.md
- web api 接口 @/.rules/dev/web.md