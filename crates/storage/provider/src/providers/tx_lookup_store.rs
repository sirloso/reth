use itertools::Itertools;
use reth_primitives::{hex::FromHexError, BlockNumber, TxHash, TxNumber};
use std::{
    collections::{BTreeSet, HashSet},
    fs::{self, File},
    io::{BufRead, BufReader, Lines},
    num::ParseIntError,
    ops::RangeInclusive,
    path::PathBuf,
    str::FromStr,
};
use thiserror::Error;

/// Transaction lookup result type.
pub type TxLookupResult<Ok> = Result<Ok, TxLookupError>;

// TODO: move to interfaces.
#[derive(Error, Debug)]
pub enum TxLookupError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    ParseHash(#[from] FromHexError),
    #[error(transparent)]
    ParseInt(#[from] ParseIntError),
    #[error("failed to split the line")]
    LineSplit,
}

/// The temporary storage for transaction hash to lookup index.
#[derive(Debug)]
pub struct TxLookupStore {
    path: PathBuf,
}

impl TxLookupStore {
    /// Create new txlookup store at given path.
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Write a sorted lookup index for a given block range to a temporary file.
    pub fn store(
        &self,
        range: RangeInclusive<BlockNumber>,
        index: Vec<(TxHash, TxNumber)>,
    ) -> TxLookupResult<()> {
        let filename = format!("{}-{}.tmp", *range.start(), *range.end());
        fs::write(
            self.path.join(filename),
            index.into_iter().map(|(hash, number)| format!("{hash} {number}")).join("\n"),
        )?;
        Ok(())
    }

    /// Create an iterator over all temporary index files that returns entries in a sorted order.
    pub fn read_iter(&self) -> TxLookupResult<TxLookupIter> {
        let mut iter = TxLookupIter::default();
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            // TODO: better file checks?
            if entry.file_type()?.is_file() {
                if let Some(cursor) = TxLookupCursor::new(entry.path())? {
                    iter.add_cursor(cursor);
                }
            }
        }
        Ok(iter)
    }
}

#[derive(Default, Debug)]
pub struct TxLookupIter {
    cursors: BTreeSet<TxLookupCursor>,
    drained: HashSet<PathBuf>,
}

impl TxLookupIter {
    fn add_cursor(&mut self, cursor: TxLookupCursor) {
        self.cursors.insert(cursor);
    }
}

impl Iterator for TxLookupIter {
    type Item = TxLookupResult<(TxHash, TxNumber)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut next_cursor = self.cursors.pop_first()?;
        let item = next_cursor.current.take().expect("must be present");
        let next_cursor_path = next_cursor.path.clone();
        match next_cursor.advance() {
            Ok(Some(cursor)) => {
                // Re-insert the back cursor if it still has any value.
                self.cursors.insert(cursor);
            }
            Ok(None) => {
                // Add filepath to the drained list
                self.drained.insert(next_cursor_path);
            }
            Err(error) => return Some(Err(error)),
        };
        Some(Ok(item))
    }
}

#[derive(Debug)]
struct TxLookupCursor {
    path: PathBuf,
    lines: Lines<BufReader<File>>,
    current: Option<(TxHash, TxNumber)>,
}

impl TxLookupCursor {
    fn new(path: PathBuf) -> TxLookupResult<Option<Self>> {
        let lines = BufReader::new(fs::File::open(&path)?).lines();
        Self { path, lines, current: None }.advance()
    }

    fn advance(mut self) -> TxLookupResult<Option<Self>> {
        match self.lines.next() {
            Some(result) => {
                let line = result?;
                let (hash, num) =
                    line.split(' ').collect_tuple().ok_or(TxLookupError::LineSplit)?;
                self.current = Some((TxHash::from_str(&hash)?, TxNumber::from_str(num)?));
                Ok(Some(self))
            }
            None => Ok(None),
        }
    }

    fn tx_hash(&self) -> Option<TxHash> {
        self.current.map(|(hash, _)| hash)
    }
}

impl PartialEq for TxLookupCursor {
    fn eq(&self, other: &Self) -> bool {
        self.tx_hash().eq(&other.tx_hash())
    }
}

impl Eq for TxLookupCursor {}

impl PartialOrd for TxLookupCursor {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.tx_hash().partial_cmp(&other.tx_hash())
    }
}

impl Ord for TxLookupCursor {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.tx_hash().cmp(&other.tx_hash())
    }
}
