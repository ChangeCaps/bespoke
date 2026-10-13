use bespoke::Bespoke;
use clap::Parser;

#[derive(Parser, Debug)]
pub struct Args {
    url: String,
    key: String,
}

pub async fn remove(args: Args) -> eyre::Result<()> {
    Bespoke::new(args.url)?.remove(&args.key).await?;

    Ok(())
}
