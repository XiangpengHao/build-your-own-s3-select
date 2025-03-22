use arrow::{
    array::RecordBatch,
    datatypes::{SchemaRef, ToByteSlice},
};
use arrow_flight::{
    Action, FlightClient, FlightEndpoint, FlightInfo, Ticket,
    error::FlightError,
    flight_service_client::FlightServiceClient,
    sql::{CommandGetDbSchemas, ProstMessageExt, client::FlightSqlServiceClient},
};
use datafusion::{
    catalog::{Session, TableProvider},
    common::{ToDFSchema, project_schema},
    datasource::{
        DefaultTableSource, TableType,
        empty::EmptyTable,
        schema_adapter::{DefaultSchemaAdapterFactory, SchemaMapper},
    },
    error::{DataFusionError, Result},
    execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext},
    logical_expr::{LogicalPlan, TableProviderFilterPushDown, TableScan},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
    prelude::Expr,
    sql::{
        TableReference,
        unparser::{Unparser, dialect::PostgreSqlDialect},
    },
};
use futures::{Stream, TryStreamExt, future::BoxFuture};
use std::{
    any::Any,
    error::Error,
    fmt::{Debug, Formatter},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
};
use tonic::{Request, async_trait, transport::Channel};

#[derive(Clone, Debug)]
pub(crate) struct FlightMetadata {
    info: FlightInfo,
    schema: SchemaRef,
}

impl FlightMetadata {
    pub fn new(info: FlightInfo, schema: SchemaRef) -> Self {
        Self { info, schema }
    }

    #[allow(clippy::result_large_err)]
    pub fn try_new(info: FlightInfo) -> arrow_flight::error::Result<Self> {
        let schema = Arc::new(info.clone().try_decode_schema()?);
        Ok(Self::new(info, schema))
    }
}

#[derive(Clone, Debug)]
pub struct FlightExec {
    config: FlightConfig,
    plan_properties: PlanProperties,
    flight_schema: SchemaRef,
}

impl FlightExec {
    /// Creates a FlightExec with the provided [FlightMetadata]
    /// and origin URL (used as fallback location as per the protocol spec).
    pub(crate) fn try_new(
        output_schema: SchemaRef,
        metadata: FlightMetadata,
        projection: Option<&Vec<usize>>,
        origin: &str,
        limit: Option<usize>,
    ) -> Result<Self> {
        let partitions = metadata
            .info
            .endpoint
            .iter()
            .map(|endpoint| FlightPartition::new(endpoint, origin.to_string()))
            .collect();
        let schema = project_schema(&output_schema, projection).expect("Error projecting schema");
        let config = FlightConfig {
            origin: origin.into(),
            partitions,
            limit,
        };

        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(config.partitions.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Ok(Self {
            config,
            plan_properties,
            flight_schema: output_schema,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FlightConfig {
    origin: String,
    partitions: Arc<[FlightPartition]>,
    limit: Option<usize>,
}

/// The minimum information required for fetching a flight stream.
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

impl Debug for FlightTicket {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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

pub(crate) fn to_df_err<E: Error + Send + Sync + 'static>(err: E) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

pub(crate) async fn flight_channel(source: impl Into<String>) -> Result<Channel> {
    let endpoint = Channel::from_shared(source.into()).map_err(to_df_err)?;
    endpoint.connect().await.map_err(to_df_err)
}

impl DisplayAs for FlightExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "FlightExec: origin={}, streams={}",
            self.config.origin,
            self.config.partitions.len(),
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
        let future_stream = flight_stream(self.config.partitions[partition].clone(), self.schema());
        let (schema_adapter, _) = DefaultSchemaAdapterFactory::from_schema(self.schema())
            .map_schema(self.flight_schema.as_ref())
            .unwrap();
        Ok(Box::pin(FlightStream {
            _partition: partition,
            state: FlightStreamState::Init,
            future_stream: Some(Box::pin(future_stream)),
            schema: self.schema(),
            schema_mapper: schema_adapter,
        }))
    }
}

enum FlightStreamState {
    Init,
    GetStream(BoxFuture<'static, Result<SendableRecordBatchStream>>),
    Processing(SendableRecordBatchStream),
}

struct FlightStream {
    _partition: usize,
    state: FlightStreamState,
    future_stream: Option<BoxFuture<'static, Result<SendableRecordBatchStream>>>,
    schema: SchemaRef,
    schema_mapper: Arc<dyn SchemaMapper>,
}

impl FlightStream {
    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        loop {
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
                    return result;
                }
            }
        }
    }
}

impl Stream for FlightStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = self.poll_inner(cx);
        match result {
            Poll::Ready(Some(Ok(batch))) => {
                let new_batch = self.schema_mapper.map_batch(batch).unwrap();
                Poll::Ready(Some(Ok(new_batch)))
            }
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

/// Default Flight SQL driver.
#[derive(Clone, Debug, Default)]
pub(crate) struct FlightSqlDriver {}

impl FlightSqlDriver {
    pub async fn metadata(
        &self,
        channel: Channel,
        table_name: &str,
        table_url: &str,
    ) -> std::result::Result<FlightMetadata, FlightError> {
        let mut client = FlightSqlServiceClient::new(channel);

        {
            let request = RegisterTableRequest {
                url: table_url.to_string(),
                table_name: table_name.to_string(),
            };
            let request = request.into();
            let request = Request::new(request);
            client.do_action(request).await?;
        }

        let request = CommandGetDbSchemas {
            db_schema_filter_pattern: Some(table_name.to_string()),
            ..Default::default()
        };
        let info = client.get_db_schemas(request).await?;
        FlightMetadata::try_new(info)
    }

    pub async fn run_sql(
        &self,
        channel: Channel,
        sql: &str,
    ) -> std::result::Result<FlightMetadata, FlightError> {
        let mut client = FlightSqlServiceClient::new(channel);
        let info = client.execute(sql.to_string(), None).await?;
        FlightMetadata::try_new(info)
    }
}

#[derive(Debug)]
pub struct PushdownTable {
    driver: Arc<FlightSqlDriver>,
    channel: Channel,
    origin: String,
    table_name: TableReference,
    output_schema: SchemaRef,
}

impl PushdownTable {
    pub async fn create(server: &str, table_name: &str, table_url: &str) -> Self {
        let channel = flight_channel(server).await.unwrap();
        let driver = Arc::new(FlightSqlDriver {});
        let metadata = driver
            .metadata(channel.clone(), table_name, table_url)
            .await
            .unwrap();

        Self {
            driver,
            channel,
            origin: server.to_string(),
            table_name: table_name.to_string().into(),
            output_schema: metadata.schema,
        }
    }
}

#[async_trait]
impl TableProvider for PushdownTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
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
            let empty_table_provider = EmptyTable::new(self.schema().clone());
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

        println!("SQL send to cache: \n{}", unparsed_sql);

        let metadata = self
            .driver
            .run_sql(self.channel.clone(), &unparsed_sql)
            .await
            .map_err(to_df_err)?;

        Ok(Arc::new(FlightExec::try_new(
            self.output_schema.clone(),
            metadata,
            projection,
            &self.origin,
            limit,
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

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RegisterTableRequest {
    #[prost(string, tag = "1")]
    pub url: ::prost::alloc::string::String,

    #[prost(string, tag = "2")]
    pub table_name: ::prost::alloc::string::String,
}

impl From<RegisterTableRequest> for Action {
    fn from(request: RegisterTableRequest) -> Self {
        use prost::Message;
        Action {
            r#type: "RegisterTable".to_string(),
            body: request.as_any().encode_to_vec().into(),
        }
    }
}

impl ProstMessageExt for RegisterTableRequest {
    fn type_url() -> &'static str {
        "type.googleapis.com/datafusion.example.com.sql.ActionRegisterTableRequest"
    }

    fn as_any(&self) -> arrow_flight::sql::Any {
        arrow_flight::sql::Any {
            type_url: RegisterTableRequest::type_url().to_string(),
            value: ::prost::Message::encode_to_vec(self).into(),
        }
    }
}
