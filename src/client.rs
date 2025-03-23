use arrow::{
    array::RecordBatch,
    datatypes::{SchemaRef, ToByteSlice},
};
use arrow_flight::{
    FlightClient, FlightEndpoint, FlightInfo, Ticket,
    flight_service_client::FlightServiceClient,
    sql::{CommandGetDbSchemas, client::FlightSqlServiceClient},
};
use datafusion::{
    catalog::{Session, TableProvider},
    common::{ToDFSchema, project_schema},
    datasource::{DefaultTableSource, TableType, empty},
    error::{DataFusionError, Result},
    execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext},
    logical_expr::{LogicalPlan, TableProviderFilterPushDown, TableScan},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
    prelude::*,
    sql::{
        TableReference,
        unparser::{Unparser, dialect::PostgreSqlDialect},
    },
};
use futures::{Stream, TryStreamExt, future::BoxFuture};
use std::task::{Context, Poll, ready};
use std::{any::Any, pin::Pin, sync::Arc};
use tonic::{async_trait, transport::Channel};

#[derive(Clone, Debug)]
pub struct FlightExec {
    server: String,
    partitions: Arc<[FlightPartition]>,
    plan_properties: PlanProperties,
}

impl FlightExec {
    pub(crate) fn try_new(
        output_schema: SchemaRef,
        info: FlightInfo,
        projection: Option<&Vec<usize>>,
        origin: &str,
    ) -> Result<Self> {
        let partitions: Arc<[FlightPartition]> = info
            .endpoint
            .iter()
            .map(|endpoint| FlightPartition::new(endpoint, origin.to_string()))
            .collect();
        let schema = project_schema(&output_schema, projection).expect("Error projecting schema");

        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(partitions.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Ok(Self {
            server: origin.to_string(),
            partitions,
            plan_properties,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FlightPartition {
    locations: String,
    ticket: FlightTicket,
}

#[derive(Clone, Eq, PartialEq)]
struct FlightTicket(Arc<[u8]>);

impl From<Option<&Ticket>> for FlightTicket {
    fn from(ticket: Option<&Ticket>) -> Self {
        let bytes = match ticket {
            Some(t) => t.ticket.to_byte_slice().into(),
            None => [].into(),
        };
        Self(bytes)
    }
}

impl std::fmt::Debug for FlightTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[..{} bytes..]", self.0.len())
    }
}

impl FlightPartition {
    fn new(endpoint: &FlightEndpoint, location: String) -> Self {
        Self {
            locations: location,
            ticket: endpoint.ticket.as_ref().into(),
        }
    }
}

async fn flight_client(source: impl Into<String>) -> Result<FlightClient> {
    let channel = flight_channel(source).await?;
    let inner_client = FlightServiceClient::new(channel);
    let client = FlightClient::new_from_inner(inner_client);
    Ok(client)
}

async fn flight_stream(
    partition: FlightPartition,
    schema: SchemaRef,
) -> Result<SendableRecordBatchStream> {
    let mut client = flight_client(partition.locations).await?;
    let ticket = Ticket::new(partition.ticket.0.to_vec());
    let stream = client.do_get(ticket).await.unwrap().map_err(to_df_err);
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        schema.clone(),
        stream,
    )))
}

pub(crate) fn to_df_err<E: std::error::Error + Send + Sync + 'static>(err: E) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

pub(crate) async fn flight_channel(source: impl Into<String>) -> Result<Channel> {
    let endpoint = Channel::from_shared(source.into()).map_err(to_df_err)?;
    endpoint.connect().await.map_err(to_df_err)
}

impl DisplayAs for FlightExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FlightExec: origin={}, streams={}",
            self.server,
            self.partitions.len(),
        )
    }
}

impl ExecutionPlan for FlightExec {
    fn name(&self) -> &str {
        "FlightExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let future_stream = flight_stream(self.partitions[partition].clone(), self.schema());
        Ok(Box::pin(FlightStream {
            state: FlightStreamState::Init,
            future_stream: Some(Box::pin(future_stream)),
            schema: self.schema(),
        }))
    }
}

enum FlightStreamState {
    Init,
    GetStream(BoxFuture<'static, Result<SendableRecordBatchStream>>),
    Processing(SendableRecordBatchStream),
}

struct FlightStream {
    state: FlightStreamState,
    future_stream: Option<BoxFuture<'static, Result<SendableRecordBatchStream>>>,
    schema: SchemaRef,
}

impl Stream for FlightStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result: Poll<Option<Result<RecordBatch>>> = loop {
            match &mut self.state {
                FlightStreamState::Init => {
                    self.state = FlightStreamState::GetStream(self.future_stream.take().unwrap());
                    continue;
                }
                FlightStreamState::GetStream(fut) => {
                    let stream = ready!(fut.as_mut().poll(cx)).unwrap();
                    self.state = FlightStreamState::Processing(stream);
                    continue;
                }
                FlightStreamState::Processing(stream) => {
                    let result = stream.as_mut().poll_next(cx);
                    break result;
                }
            }
        };
        match result {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(Ok(batch))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(e))) => {
                panic!("Error reading flight stream: {}", e);
            }
            _ => Poll::Pending,
        }
    }
}

impl RecordBatchStream for FlightStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[derive(Debug)]
pub struct FlightTable {
    channel: Channel,
    server: String,
    table_name: TableReference,
    schema: SchemaRef,
}

impl FlightTable {
    pub async fn create(server: &str, table_name: &str, table_url: &str) -> Self {
        let channel = flight_channel(server).await.unwrap();

        let metadata = {
            let mut client = FlightSqlServiceClient::new(channel.clone());
            let request = CommandGetDbSchemas {
                db_schema_filter_pattern: Some(table_name.to_string()),
                catalog: Some(table_url.to_string()),
            };
            client.get_db_schemas(request).await.unwrap()
        };

        Self {
            channel,
            server: server.to_string(),
            table_name: table_name.to_string().into(),
            schema: Arc::new(metadata.try_decode_schema().unwrap()),
        }
    }
}

#[async_trait]
impl TableProvider for FlightTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let unparsed_sql = {
            // we don't care about actual source for the purpose of unparsing the sql.
            let empty_table_provider = empty::EmptyTable::new(self.schema().clone());
            let table_source = Arc::new(DefaultTableSource::new(Arc::new(empty_table_provider)));

            let logical_plan = TableScan {
                table_name: self.table_name.clone(),
                source: table_source,
                projection: projection.map(|p| p.to_vec()),
                filters: filters.to_vec(),
                fetch: limit,
                projected_schema: Arc::new(self.schema().as_ref().clone().to_dfschema().unwrap()),
            };
            let unparser = Unparser::new(&PostgreSqlDialect {});
            let unparsed_sql = unparser
                .plan_to_sql(&LogicalPlan::TableScan(logical_plan))
                .unwrap();
            unparsed_sql.to_string()
        };

        println!("SQL to pushdown: \n-------\n{}\n-------\n", unparsed_sql);

        let mut client = FlightSqlServiceClient::new(self.channel.clone());
        let info = client.execute(unparsed_sql, None).await.unwrap();

        Ok(Arc::new(FlightExec::try_new(
            self.schema.clone(),
            info,
            projection,
            &self.server,
        )?))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let filter_push_down: Vec<TableProviderFilterPushDown> = filters
            .iter()
            .map(
                |f| match Unparser::new(&PostgreSqlDialect {}).expr_to_sql(f) {
                    Ok(_) => TableProviderFilterPushDown::Exact,
                    Err(_) => TableProviderFilterPushDown::Unsupported,
                },
            )
            .collect();
        Ok(filter_push_down)
    }
}

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
    let sql = format!(
        "SELECT DISTINCT \"city\" FROM \"{table_name}\" WHERE \"country\" = 'United States'"
    );
    println!("SQL to run: \n-------\n{}\n-------\n", sql);
    let table = FlightTable::create(cache_server, table_name, table_url).await;
    ctx.register_table(table_name, Arc::new(table))?;
    ctx.sql(&sql).await?.show().await?;
    Ok(())
}
