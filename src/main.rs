use clap::Parser;
use futures::StreamExt;
use near_lake_framework::LakeConfig;
use std::str::FromStr;

#[derive(Parser)]
#[clap(author = "Near Inc. <hello@nearprotocol.com")]
pub(crate) struct Opts {
    #[clap(long, short)]
    pub accounts: String,
    #[clap(long, short)]
    pub block_height: u64,
    #[clap(subcommand)]
    pub chain_id: ChainId,
}

#[derive(Parser)]
pub(crate) enum ChainId {
    Mainnet,
    Testnet,
}

#[tokio::main]
async fn main() -> Result<(), tokio::io::Error> {
    // Read the args passed to the application from commmand-line
    let opts: Opts = Opts::parse();

    // Store the block height for later use
    let start_block_height = opts.block_height;

    // Create a list of account names string to watch
    // We parse them with near-account in order to
    // throw an error if account name is invalid
    let watching_list: Vec<String> = opts
        .accounts
        .split(',')
        .map(|elem| {
            near_lake_framework::near_indexer_primitives::types::AccountId::from_str(elem)
            .expect("AccountId is invalid")
            .to_string()
        })
        .collect();

    // Inform about indexer is being started and what accounts we're watching for
    eprintln!(
        "Starting indexer transaction watcher for accounts: \n {:#?}",
        &opts.accounts
    );

    // NEAR Lake Framework boilerplate
    // Instantiate the config
    let config = LakeConfig {
        s3_endpoint: None, // means default AWS S3 endpoint
        // Passing the S3 bucket name based on the chain_id from passed args
        s3_bucket_name: match opts.chain_id {
            ChainId::Mainnet => "near-lake-data-mainnet",
            ChainId::Testnet => "near-lake-data-testnet",
        }.to_string(),
        // Passing the S3 bucket region name
        s3_region_name: "eu-central-1".to_string(),
        // And saying from which block height to start indexing
        start_block_height,
    };

    // Instantiating the stream
    let stream = near_lake_framework::streamer(config);

    // Defining how the stream will be handled
    let mut handlers = tokio_stream::wrappers::ReceiverStream::new(stream)
        .map(|streamer_message| handle_streamer_message(streamer_message, watching_list.clone()))
        .buffer_unordered(1usize);

    // Finishing the boilerplate with a busy loop to actually handle the stream
    while let Some(_handle_message) = handlers.next().await {};

    Ok(())
}

/// Function that receives the StreamerMessage from
/// the NEAR Lake Framework and our list of watched
/// account names so we know what we are looking for
/// in each block.
async fn handle_streamer_message(
    streamer_message: near_lake_framework::near_indexer_primitives::StreamerMessage,
    watching_list: Vec<String>,
) {
    // StateChanges we are looking for can be found in each shard, so we iterate over available shards
    for shard in streamer_message.shards {
        for state_change in shard.state_changes {
            // We want to print the block height and
            // change type if the StateChange affects one of the accounts we are watching for
            if is_change_watched(&state_change, &watching_list) {
                // We convert it to JSON in order to show it is possible
                // also, it is easier to read the printed version for this tutorial
                // but we don't encourage you to do the same in your indexer. It's up to you
                let changes_json = serde_json::to_value(state_change)
                    .expect("Failed to serialize StateChange to JSON");
                println!(
                    "#{}. {}",
                    streamer_message.block.header.height,
                    changes_json["type"]
                );
                println!("{:#?}", changes_json);
            }
        }
    }
}

fn is_change_watched(
    state_change: &near_lake_framework::near_indexer_primitives::views::StateChangeWithCauseView,
    watching_list: &[String],
) -> bool {
    // StateChangeWithCauseView.value field hold the Enum
    // Every enum variant holds the `account_id` field
    // We do need only the value of `account_id` here
    // and we want to avoid enum matching, so that's why we convert the `state_change` to serde_json::Value
    let value = serde_json::to_value(state_change)
        .expect("Failed to serialize StateChange to JSON");
    // A little bit tricky way to get the String value withour escaping quotes
    let account_id: String = value["change"]["account_id"].as_str().unwrap().to_string();

    // check the watching_list has the affected account_id from the state_change
    watching_list.contains(&account_id)
}
