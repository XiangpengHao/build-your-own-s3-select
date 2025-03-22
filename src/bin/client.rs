use build_your_own_s3_select::PushdownTable;
use datafusion::prelude::{SessionConfig, SessionContext};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut session_config = SessionConfig::from_env()?;
    session_config
        .options_mut()
        .execution
        .parquet
        .pushdown_filters = true;
    let ctx = Arc::new(SessionContext::new_with_config(session_config));

    let cache_server = "http://localhost:50051";
    let table_name = "aws-edge-locations";
    let table_url = "./aws-edge-locations.parquet";
    let sql = format!("SELECT \"city\" FROM \"{table_name}\" WHERE \"country\" = 'United States'");
    let table = PushdownTable::create(cache_server, table_name, table_url).await;
    ctx.register_table(table_name, Arc::new(table))?;

    ctx.sql(&sql).await?.show().await?;

    Ok(())
}
