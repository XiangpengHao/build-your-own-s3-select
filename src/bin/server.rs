use arrow_flight::{
    Action, FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
    encode::FlightDataEncoderBuilder,
    flight_service_server::{FlightService, FlightServiceServer},
    sql::{
        Any, CommandGetDbSchemas, CommandStatementQuery, ProstMessageExt, server::FlightSqlService,
    },
};
use build_your_own_s3_select::{FetchResults, RegisterTableRequest};
use bytes::Bytes;
use datafusion::{
    physical_plan::{ExecutionPlan, ExecutionPlanProperties},
    prelude::*,
};
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, atomic},
};
use tonic::{Request, Response, Status, transport::Server};

struct PushdownServer {
    execution_plans: Mutex<HashMap<u64, Arc<dyn ExecutionPlan>>>,
    next_id: atomic::AtomicU64,
    ctx: SessionContext,
}

impl PushdownServer {
    fn new() -> Self {
        Self {
            execution_plans: Mutex::new(HashMap::new()),
            next_id: atomic::AtomicU64::new(0),
            ctx: SessionContext::new(),
        }
    }
}

#[tonic::async_trait]
impl FlightSqlService for PushdownServer {
    type FlightService = PushdownServer;

    async fn register_sql_info(&self, _id: i32, _result: &arrow_flight::sql::SqlInfo) {}

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let table_name = query.db_schema_filter_pattern.unwrap();
        let schema = self.ctx.table_provider(&table_name).await.unwrap().schema();
        let info = FlightInfo::new().try_with_schema(&schema).unwrap();
        Ok(Response::new(info))
    }

    async fn get_flight_info_statement(
        &self,
        cmd: CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        println!("planing query");
        let query = cmd.query.as_str();
        let (state, logical_plan) = self.ctx.sql(query).await.unwrap().into_parts();
        let plan = state.optimize(&logical_plan).unwrap();
        let physical_plan = state.create_physical_plan(&plan).await.unwrap();
        let partition_count = physical_plan.output_partitioning().partition_count();
        let schema = physical_plan.schema();
        let id = self.next_id.fetch_add(1, atomic::Ordering::Relaxed);
        self.execution_plans
            .lock()
            .unwrap()
            .insert(id, physical_plan);
        let mut info = FlightInfo::new().try_with_schema(&schema).unwrap();
        for partition in 0..partition_count {
            let fetch = FetchResults {
                handle: id,
                partition: partition as u32,
            };
            let buf = fetch.as_any().encode_to_vec().into();
            let ticket = Ticket { ticket: buf };
            let endpoint = FlightEndpoint::new().with_ticket(ticket.clone());
            info = info.with_endpoint(endpoint);
        }

        Ok(Response::new(info))
    }

    async fn do_get_fallback(
        &self,
        _request: Request<Ticket>,
        message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let fetch_results: FetchResults = message.unpack().unwrap().unwrap();
        let plan_lock = self.execution_plans.lock().unwrap();
        let physical_plan = plan_lock.get(&fetch_results.handle).unwrap().clone();
        let stream = physical_plan
            .execute(fetch_results.partition as usize, self.ctx.task_ctx())
            .unwrap()
            .map_err(|e| arrow_flight::error::FlightError::ExternalError(Box::new(e)));
        let encoder = FlightDataEncoderBuilder::new().build(stream);
        let response_stream =
            encoder.map(|result| result.map_err(|e| Status::internal(e.to_string())));
        Ok(Response::new(Box::pin(response_stream)))
    }

    async fn do_action_fallback(
        &self,
        request: Request<Action>,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let action = request.into_inner();
        let any = Any::decode(action.body).unwrap();
        let request = any.unpack::<RegisterTableRequest>().unwrap().unwrap();
        _ = self
            .ctx
            .register_parquet(request.table_name, request.url, Default::default())
            .await;
        let output = futures::stream::iter(vec![Ok(arrow_flight::Result {
            body: Bytes::default(),
        })]);
        Ok(Response::new(Box::pin(output)))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50051".parse()?;
    Server::builder()
        .add_service(FlightServiceServer::new(PushdownServer::new()))
        .serve(addr)
        .await?;
    Ok(())
}
