//! Transaction lookup related models and types.

use reth_codecs::{derive_arbitrary, Compact};
use reth_primitives::{TxHash, TxNumber, B256};
use serde::{Deserialize, Serialize};

/// Transaction number lookup by hash as it is saved in the database.
#[derive_arbitrary(compact)]
#[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize, PartialOrd, Ord)]
pub struct TxNumberLookup {
    /// Address for the account. Acts as `DupSort::SubKey`.
    pub hash: TxHash,
    /// Account state before the transaction.
    pub number: TxNumber,
}

// NOTE: Removing main_codec and manually encode subkey
// and compress second part of the value. If we have compression
// over whole value (Even SubKey) that would mess up fetching of values with `seek_by_key_subkey``
impl Compact for TxNumberLookup {
    fn to_compact<B>(self, buf: &mut B) -> usize
    where
        B: bytes::BufMut + AsMut<[u8]>,
    {
        // for now put full bytes and later compress it.
        buf.put_slice(&self.hash[..]);
        self.number.to_compact(buf) + 32
    }

    fn from_compact(buf: &[u8], len: usize) -> (Self, &[u8]) {
        let hash = B256::from_slice(&buf[..32]);
        let (number, out) = u64::from_compact(&buf[32..], len - 32);
        (Self { hash, number }, out)
    }
}
