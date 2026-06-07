# web 模块约定
api 接口层中
dto 中日期类型使用 i64
在 api/mod.rs 中使用
```rust
pub fn web_config(cfg: &mut web::ServiceConfig) {
 // 注册接口
}
```
- web::Data 对象包裹的都是 Arc<T> 格式
- post 请求使用 web::Json 获取参数
- query 请求使用 web::Query 获取参数
- 返回类型使用 DefautResult<impl Responder> , 使用 ApiResult.value(data).into() 封装 
- 错误处理使用 ApiResult.fail("message").into() 封装返回

## 示例代码
```rust
/// 根据 API URL 和模型名称查找供应商
/// 
/// 根据 API URL 和模型名称查找匹配的 LLM 供应商。
/// 
/// # OpenAPI 规范
/// 
/// ```yaml
/// /api/llm/providers/search:
///   get:
///     summary: 查找 LLM 供应商
///     description: 根据 API URL 和模型名称查找匹配的 LLM 供应商
///     parameters:
///       - name: api_url
///         in: query
///         required: true
///         schema:
///           type: string
///       - name: model_name
///         in: query
///         required: true
///         schema:
///           type: string
///     responses:
///       '200':
///         description: 成功找到供应商
///         content:
///           application/json:
///             schema:
///               $ref: '#/components/schemas/LlmProviderResponse'
///       '404':
///         description: 未找到匹配的供应商
///       '500':
///         description: 服务器内部错误
/// ```
#[get("/providers/search")]
async fn find_provider(
    db: web::Data<Arc<DatabaseConnection>>,
    query: web::Query<FindProviderQuery>,
) -> DefaultResult<impl Responder> {
    let service = LlmProviderService::new(db.get_ref().clone());
    let response = service.find_provider_by_url_and_model(&query.api_url, &query.model_name).await?;
    ApiResult::value(response).into()
}
```