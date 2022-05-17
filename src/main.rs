use clap::Parser;
use near_lake_framework::near_indexer_primitives::types::AccountId;
use near_lake_framework::near_indexer_primitives::views::{
    StateChangeValueView, StateChangeWithCauseView,
};
use near_lake_framework::LakeConfigBuilder;

#[derive(Parser)]
#[clap(author = "Near Inc. <hello@nearprotocol.com")]
pub(crate) struct Opts {
    #[clap(long, short)]
    pub accounts: Vec<AccountId>,
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

    // Inform about indexer is being started and what accounts we're watching for
    eprintln!(
        "Starting indexer transaction watcher for accounts: \n {:#?}",
        &opts.accounts
    );

    // NEAR Lake Framework boilerplate
    // Instantiate the config
    let mut config = LakeConfigBuilder::default().start_block_height(opts.block_height);

    match opts.chain_id {
        ChainId::Mainnet => config = config.mainnet(),
        ChainId::Testnet => config = config.testnet(),
    };

    // Instantiating the stream
    let (_, mut stream) =
        near_lake_framework::streamer(config.build().expect("Failed to build LakeConfig"));

    // Finishing the boilerplate with a busy loop to actually handle the stream
    while let Some(streamer_message) = stream.recv().await {
        handle_streamer_message(streamer_message, &opts.accounts).await;
    }

    Ok(())
}

/// Function that receives the StreamerMessage from
/// the NEAR Lake Framework and our list of watched
/// account names so we know what we are looking for
/// in each block.
async fn handle_streamer_message(
    streamer_message: near_lake_framework::near_indexer_primitives::StreamerMessage,
    watching_list: &[AccountId],
) {
    // StateChanges we are looking for can be found in each shard, so we iterate over available shards
    for shard in streamer_message.shards {
        for state_change in shard.state_changes {
            // We want to print the block height and
            // change type if the StateChange affects one of the accounts we are watching for
            if is_change_watched(&state_change, watching_list) {
                // We convert it to JSON in order to show it is possible
                // also, it is easier to read the printed version for this tutorial
                // but we don't encourage you to do the same in your indexer. It's up to you
                let changes_json = serde_json::to_value(state_change)
                    .expect("Failed to serialize StateChange to JSON");
                println!(
                    "#{}. {}",
                    streamer_message.block.header.height, changes_json["type"]
                );
                println!("{:#?}", changes_json);
            }
        }
    }
}

fn is_change_watched(state_change: &StateChangeWithCauseView, watching_list: &[AccountId]) -> bool {
    // get the affected account_id from state_change.value
    // ref https://docs.rs/near-primitives/0.12.0/near_primitives/views/enum.StateChangeValueView.html
    let account_id = match &state_change.value {
        StateChangeValueView::AccountUpdate { account_id, .. } => account_id,
        StateChangeValueView::AccountDeletion { account_id } => account_id,
        StateChangeValueView::AccessKeyUpdate { account_id, .. } => account_id,
        StateChangeValueView::AccessKeyDeletion { account_id, .. } => account_id,
        StateChangeValueView::DataUpdate { account_id, .. } => account_id,
        StateChangeValueView::DataDeletion { account_id, .. } => account_id,
        StateChangeValueView::ContractCodeUpdate { account_id, .. } => account_id,
        StateChangeValueView::ContractCodeDeletion { account_id, .. } => account_id,
    };

    // check the watching_list has the affected account_id from the state_change
    watching_list.contains(account_id)
}
