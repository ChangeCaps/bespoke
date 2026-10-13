use std::{
    fs::File,
    io::{Seek, SeekFrom},
    net::SocketAddr,
    path::{self, PathBuf},
    sync::{Arc, Mutex},
};

use axum::{
    BoxError, Router,
    body::{Body, Bytes},
    extract::{Path, State},
    response::IntoResponse,
    routing::get,
};
use clap::Parser;
use dashmap::DashMap;
use eyre::Context;
use tokio::{
    net::TcpListener,
    sync::mpsc::{UnboundedSender, unbounded_channel},
};
use tokio_stream::wrappers::UnboundedReceiverStream;

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(long, default_value = "data.bs")]
    path: PathBuf,

    #[arg(long, default_value = "0.0.0.0:3000")]
    addr: SocketAddr,
}

pub async fn serve(args: Args) -> eyre::Result<()> {
    let state = AppState::new(args.path).await?;
    state.store()?;

    let app = Router::new().nest("/v1", v1()).with_state(Arc::new(state));

    let listener = TcpListener::bind(args.addr).await?;

    axum::serve(listener, app).await?;

    Ok(())
}

struct AppState {
    file: Mutex<File>,
    values: DashMap<Bytes, Bytes>,
    subscribers: DashMap<Bytes, Vec<Subscriber>>,
}

type Subscriber = UnboundedSender<Result<Bytes, BoxError>>;

impl AppState {
    async fn new(path: impl AsRef<path::Path>) -> eyre::Result<Self> {
        let (file, values) = if path.as_ref().exists() {
            let file = File::options().read(true).write(true).open(path)?;

            let pairs: Vec<[Bytes; 2]> =
                rmp_serde::decode::from_read(&file).wrap_err("deserialization of data failed")?;

            let values = pairs.into_iter().map(|[key, value]| (key, value)).collect();

            (file, values)
        } else {
            let file = File::create(path)?;
            (file, DashMap::new())
        };

        let subscribers = DashMap::new();

        Ok(Self {
            file: Mutex::new(file),
            values,
            subscribers,
        })
    }

    fn store(&self) -> eyre::Result<()> {
        let pairs: Vec<[Bytes; 2]> = self
            .values
            .iter()
            .map(|pair| [pair.key().clone(), pair.value().clone()])
            .collect::<Vec<_>>();

        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(0))?;
        rmp_serde::encode::write(&mut *file, &pairs)?;

        Ok(())
    }
}

fn v1() -> Router<Arc<AppState>> {
    async fn get_value(
        State(state): State<Arc<AppState>>,
        Path(key): Path<String>,
    ) -> impl IntoResponse {
        let (sender, receiver) = unbounded_channel::<Result<Bytes, BoxError>>();

        let key = Bytes::from(key);
        if let Some(value) = state.values.get(&key) {
            let _ = sender.send(Ok(value.clone()));
        }

        state.subscribers.entry(key).or_default().push(sender);
        Body::from_stream(UnboundedReceiverStream::new(receiver))
    }

    async fn set_value(
        State(state): State<Arc<AppState>>,
        Path(key): Path<String>,
        value: Bytes,
    ) -> impl IntoResponse {
        let key = Bytes::from(key);
        state.values.insert(key.clone(), value.clone());

        let _ = state.store();

        if let Some(mut subscribers) = state.subscribers.get_mut(&key) {
            subscribers.retain(|subscriber| subscriber.send(Ok(value.clone())).is_ok());
        }
    }

    async fn remove_value(
        State(state): State<Arc<AppState>>,
        Path(key): Path<String>,
    ) -> impl IntoResponse {
        let key = Bytes::from(key);
        state.values.remove(&key);
        state.subscribers.remove(&key);
    }

    Router::new().route(
        "/{key}",
        get(get_value).post(set_value).delete(remove_value),
    )
}
