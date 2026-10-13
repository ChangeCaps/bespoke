use bespoke::Bespoke;
use clap::Parser;
use futures::TryStreamExt;

#[derive(Parser, Debug)]
pub struct Args {
    url: String,
    key: String,
}

pub async fn get(args: Args) -> eyre::Result<()> {
    let mut bytes = Bespoke::new(args.url)?
        .stream::<String, String>(&args.key)
        .await?;

    while let Some(bytes) = bytes.try_next().await? {
        println!("{}", bytes);
    }

    Ok(())
}
