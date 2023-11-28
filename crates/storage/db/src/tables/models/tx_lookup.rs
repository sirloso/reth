//! Transaction lookup related models and types.

use reth_codecs::{main_codec, Compact};
use reth_primitives::{TxHash, TxNumber};

/// Transaction number lookup by hash as it is saved in the database.
#[main_codec]
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct TxNumberLookup {
    /// Address for the account. Acts as `DupSort::SubKey`.
    pub hash: TxHash,
    /// Account state before the transaction.
    pub number: TxNumber,
}
