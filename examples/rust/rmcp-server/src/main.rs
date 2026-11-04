use agent_supervisor::{
    mcp::{run_stdio_server_with_config, McpCallPolicy},
    SandboxPolicy,
};
use rmcp::{tool, tool_router, transport::stdio, ServiceExt};
use std::{error::Error, time::Duration};
#[derive(Clone)]
struct Server;
#[tool_router(server_handler)]
impl Server {
    #[tool(description = "A visible tool")]
    fn greet(&self) -> String {
        "hello".into()
    }
    #[tool(description = "A denied tool")]
    fn hidden(&self) -> String {
        "hidden".into()
    }
    #[tool(description = "A slow tool")]
    async fn slow(&self) -> String {
        tokio::time::sleep(Duration::from_secs(5)).await;
        "late".into()
    }
    #[tool(description = "A large result")]
    fn large(&self) -> String {
        "x".repeat(1024)
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let controls = McpCallPolicy {
        denied_tools: ["hidden".to_owned()].into_iter().collect(),
        call_timeout: Duration::from_millis(150),
        max_concurrent_calls: 1,
        max_result_bytes: 256,
        ..Default::default()
    };
    run_stdio_server_with_config(SandboxPolicy::default(), controls, || async {
        let server = Server.serve(stdio()).await?;
        server.waiting().await?;
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    })
    .await
}
