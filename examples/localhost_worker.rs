use datafusion::execution::context::SessionContext;
use datafusion_distributed::{DistributedExt, Worker, WorkerQueryContext};
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use structopt::StructOpt;
use tonic::transport::Server;

use datafusion_distributed::luxdb;

#[derive(StructOpt)]
#[structopt(name = "localhost_worker", about = "A localhost DataFusion worker")]
struct Args {
    #[structopt(default_value = "8080")]
    port: u16,

    /// LuxDB segments directories (must exist on this machine).
    #[structopt(
        long = "lux-segments-dirs",
        use_delimiter = true,
        default_value = "/Users/admin/datafusion-distributed/generated_data/demo_db/shard_0/segments,/Users/admin/datafusion-distributed/generated_data/demo_db/shard_1/segments,/Users/admin/datafusion-distributed/generated_data/demo_db/shard_2/segments"
    )]
    lux_segments_dirs: Vec<String>,

    /// Number of scan partitions for lux_segments.
    #[structopt(long = "lux-partitions", default_value = "3")]
    lux_partitions: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::from_args();

    let segments_dirs = args.lux_segments_dirs.clone();
    let partitions = args.lux_partitions;

    // Build a worker that registers lux_segments in its SessionState.
    let worker = Worker::from_session_builder(move |ctx: WorkerQueryContext| {
        let segments_dirs = segments_dirs.clone();
        async move {
            // The WorkerQueryContext provides a pre-configured builder.
            // We build a SessionContext from it to register our table.
            let state = ctx
                .builder
                .with_distributed_user_codec(luxdb::segments::LuxSegmentsCodec)
                .build();
            let session_ctx = SessionContext::new_with_state(state);

            session_ctx.register_table(
                "lux_segments",
                Arc::new(luxdb::LuxSegmentsTableProvider::new_multi(
                    segments_dirs,
                    partitions,
                )),
            )?;

            Ok(session_ctx.state())
        }
    });

    println!(
        "Starting worker on port {} with {} luxdb partitions...",
        args.port,
        args.lux_partitions
    );

    Server::builder()
        .add_service(worker.into_flight_server())
        .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), args.port))
        .await?;

    Ok(())
}
