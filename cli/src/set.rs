use bespoke::Bespoke;
use clap::Parser;

#[derive(Parser, Debug)]
pub struct Args {
    url: String,
    key: String,
    value: String,
}

pub async fn set(args: Args) -> eyre::Result<()> {
    Bespoke::new(args.url)?.set(&args.key, &args.value).await?;

    Ok(())
}
