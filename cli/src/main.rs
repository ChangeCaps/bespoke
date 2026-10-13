use clap::{Parser, Subcommand};

mod get;
mod remove;
mod serve;
mod set;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    Serve(serve::Args),
    Set(set::Args),
    Get(get::Args),
    Remove(remove::Args),
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let args = Args::parse();

    match args.cmd {
        Cmd::Serve(args) => serve::serve(args).await,
        Cmd::Get(args) => get::get(args).await,
        Cmd::Set(args) => set::set(args).await,
        Cmd::Remove(args) => remove::remove(args).await,
    }
}
