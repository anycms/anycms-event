---
name: rust-api-http
description: 为 Rust API 接口创建 http 文档
---
- 文档保存位置  @/docs/api/
- 命名根据 api 所在的 controller 命名
- 严格分析 api 文档，返回内容字段，参数字段信息，不可随意编写
- 需要给每个接口添加返回示例的注释
- 文档增加必须的变量： @base_url
- 增加其他合理必要的变量来优化接口文档
- 返回内容字段不清楚的使用  cargo doc 生成文档查看
- 完整分析 ApiResult 类型的字段