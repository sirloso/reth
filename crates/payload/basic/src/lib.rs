//! A basic payload generator for reth.

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/paradigmxyz/reth/main/assets/reth-docs.png",
    html_favicon_url = "https://avatars0.githubusercontent.com/u/97369466?s=256",
    issue_tracker_base_url = "https://github.com/paradigmxyz/reth/issues/"
)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

use alloy_rlp::Encodable;
use futures_core::ready;
use futures_util::FutureExt;
use revm::{
    db::states::bundle_state::BundleRetention,
    primitives::{BlockEnv, CfgEnv, Env},
    Database, DatabaseCommit, State,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{atomic::AtomicBool, Arc},
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{oneshot, Semaphore},
    time::{Interval, Sleep},
};
use tracing::{debug, trace, warn};

use reth_interfaces::RethResult;
use reth_payload_builder::{
    database::CachedReads, error::PayloadBuilderError, BuiltPayload, KeepPayloadJobAlive,
    PayloadBuilderAttributes, PayloadId, PayloadJob, PayloadJobGenerator,
};
use reth_primitives::{
    bytes::BytesMut,
    constants::{
        BEACON_NONCE, EMPTY_RECEIPTS, EMPTY_TRANSACTIONS, EMPTY_WITHDRAWALS,
        ETHEREUM_BLOCK_GAS_LIMIT, RETH_CLIENT_VERSION, SLOT_DURATION,
    },
    proofs, Block, BlockNumberOrTag, Bytes, ChainSpec, Header, Receipts, SealedBlock, Withdrawal,
    B256, EMPTY_OMMER_ROOT_HASH, U256,
};
use reth_provider::{
    BlockReaderIdExt, BlockSource, BundleStateWithReceipts, ProviderError, StateProviderFactory,
};
use reth_revm::{
    database::StateProviderDatabase,
    state_change::{apply_beacon_root_contract_call, post_block_withdrawals_balance_increments},
};
use reth_tasks::TaskSpawner;
use reth_transaction_pool::TransactionPool;

use crate::metrics::PayloadBuilderMetrics;

mod metrics;

/// The [`PayloadJobGenerator`] that creates [`BasicPayloadJob`]s.
#[derive(Debug)]
pub struct BasicPayloadJobGenerator<Client, Pool, Tasks, Builder> {
    /// The client that can interact with the chain.
    client: Client,
    /// txpool
    pool: Pool,
    /// How to spawn building tasks
    executor: Tasks,
    /// The configuration for the job generator.
    config: BasicPayloadJobGeneratorConfig,
    /// Restricts how many generator tasks can be executed at once.
    payload_task_guard: PayloadTaskGuard,
    /// The chain spec.
    chain_spec: Arc<ChainSpec>,
    /// The type responsible for building payloads.
    ///
    /// See [PayloadBuilder]
    builder: Builder,
}

// === impl BasicPayloadJobGenerator ===

impl<Client, Pool, Tasks, Builder> BasicPayloadJobGenerator<Client, Pool, Tasks, Builder> {
    /// Creates a new [BasicPayloadJobGenerator] with the given config and custom [PayloadBuilder]
    pub fn with_builder(
        client: Client,
        pool: Pool,
        executor: Tasks,
        config: BasicPayloadJobGeneratorConfig,
        chain_spec: Arc<ChainSpec>,
        builder: Builder,
    ) -> Self {
        Self {
            client,
            pool,
            executor,
            payload_task_guard: PayloadTaskGuard::new(config.max_payload_tasks),
            config,
            chain_spec,
            builder,
        }
    }

    /// Returns the maximum duration a job should be allowed to run.
    ///
    /// This adheres to the following specification:
    // > Client software SHOULD stop the updating process when either a call to engine_getPayload
    // > with the build process's payloadId is made or SECONDS_PER_SLOT (12s in the Mainnet
    // > configuration) have passed since the point in time identified by the timestamp parameter.
    // See also <https://github.com/ethereum/execution-apis/blob/431cf72fd3403d946ca3e3afc36b973fc87e0e89/src/engine/paris.md?plain=1#L137>
    #[inline]
    fn max_job_duration(&self, unix_timestamp: u64) -> Duration {
        let duration_until_timestamp = duration_until(unix_timestamp);

        // safety in case clocks are bad
        let duration_until_timestamp = duration_until_timestamp.min(self.config.deadline * 3);

        self.config.deadline + duration_until_timestamp
    }

    /// Returns the [Instant](tokio::time::Instant) at which the job should be terminated because it
    /// is considered timed out.
    #[inline]
    fn job_deadline(&self, unix_timestamp: u64) -> tokio::time::Instant {
        tokio::time::Instant::now() + self.max_job_duration(unix_timestamp)
    }
}

// === impl BasicPayloadJobGenerator ===

impl<Client, Pool, Tasks, Builder> PayloadJobGenerator
    for BasicPayloadJobGenerator<Client, Pool, Tasks, Builder>
where
    Client: StateProviderFactory + BlockReaderIdExt + Clone + Unpin + 'static,
    Pool: TransactionPool + Unpin + 'static,
    Tasks: TaskSpawner + Clone + Unpin + 'static,
    Builder: PayloadBuilder<Pool, Client> + Unpin + 'static,
{
    type Job = BasicPayloadJob<Client, Pool, Tasks, Builder>;

    fn new_payload_job(
        &self,
        attributes: PayloadBuilderAttributes,
    ) -> Result<Self::Job, PayloadBuilderError> {
        let parent_block = if attributes.parent.is_zero() {
            // use latest block if parent is zero: genesis block
            self.client
                .block_by_number_or_tag(BlockNumberOrTag::Latest)?
                .ok_or_else(|| PayloadBuilderError::MissingParentBlock(attributes.parent))?
                .seal_slow()
        } else {
            let block = self
                .client
                .find_block_by_hash(attributes.parent, BlockSource::Any)?
                .ok_or_else(|| PayloadBuilderError::MissingParentBlock(attributes.parent))?;

            // we already know the hash, so we can seal it
            block.seal(attributes.parent)
        };

        let config = PayloadConfig::new(
            Arc::new(parent_block),
            self.config.extradata.clone(),
            attributes,
            Arc::clone(&self.chain_spec),
        );

        let until = self.job_deadline(config.attributes.timestamp);
        let deadline = Box::pin(tokio::time::sleep_until(until));

        Ok(BasicPayloadJob {
            config,
            client: self.client.clone(),
            pool: self.pool.clone(),
            executor: self.executor.clone(),
            deadline,
            interval: tokio::time::interval(self.config.interval),
            best_payload: None,
            pending_block: None,
            cached_reads: None,
            payload_task_guard: self.payload_task_guard.clone(),
            metrics: Default::default(),
            builder: self.builder.clone(),
        })
    }
}

/// Restricts how many generator tasks can be executed at once.
#[derive(Debug, Clone)]
struct PayloadTaskGuard(Arc<Semaphore>);

// === impl PayloadTaskGuard ===

impl PayloadTaskGuard {
    fn new(max_payload_tasks: usize) -> Self {
        Self(Arc::new(Semaphore::new(max_payload_tasks)))
    }
}

/// Settings for the [BasicPayloadJobGenerator].
#[derive(Debug, Clone)]
pub struct BasicPayloadJobGeneratorConfig {
    /// Data to include in the block's extra data field.
    extradata: Bytes,
    /// Target gas ceiling for built blocks, defaults to [ETHEREUM_BLOCK_GAS_LIMIT] gas.
    max_gas_limit: u64,
    /// The interval at which the job should build a new payload after the last.
    interval: Duration,
    /// The deadline for when the payload builder job should resolve.
    ///
    /// By default this is [SLOT_DURATION]: 12s
    deadline: Duration,
    /// Maximum number of tasks to spawn for building a payload.
    max_payload_tasks: usize,
}

// === impl BasicPayloadJobGeneratorConfig ===

impl BasicPayloadJobGeneratorConfig {
    /// Sets the interval at which the job should build a new payload after the last.
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Sets the deadline when this job should resolve.
    pub fn deadline(mut self, deadline: Duration) -> Self {
        self.deadline = deadline;
        self
    }

    /// Sets the maximum number of tasks to spawn for building a payload(s).
    ///
    /// # Panics
    ///
    /// If `max_payload_tasks` is 0.
    pub fn max_payload_tasks(mut self, max_payload_tasks: usize) -> Self {
        assert!(max_payload_tasks > 0, "max_payload_tasks must be greater than 0");
        self.max_payload_tasks = max_payload_tasks;
        self
    }

    /// Sets the data to include in the block's extra data field.
    ///
    /// Defaults to the current client version: `rlp(RETH_CLIENT_VERSION)`.
    pub fn extradata(mut self, extradata: Bytes) -> Self {
        self.extradata = extradata;
        self
    }

    /// Sets the target gas ceiling for mined blocks.
    ///
    /// Defaults to [ETHEREUM_BLOCK_GAS_LIMIT] gas.
    pub fn max_gas_limit(mut self, max_gas_limit: u64) -> Self {
        self.max_gas_limit = max_gas_limit;
        self
    }
}

impl Default for BasicPayloadJobGeneratorConfig {
    fn default() -> Self {
        let mut extradata = BytesMut::new();
        RETH_CLIENT_VERSION.as_bytes().encode(&mut extradata);
        Self {
            extradata: extradata.freeze().into(),
            max_gas_limit: ETHEREUM_BLOCK_GAS_LIMIT,
            interval: Duration::from_secs(1),
            // 12s slot time
            deadline: SLOT_DURATION,
            max_payload_tasks: 3,
        }
    }
}

/// A basic payload job that continuously builds a payload with the best transactions from the pool.
#[derive(Debug)]
pub struct BasicPayloadJob<Client, Pool, Tasks, Builder> {
    /// The configuration for how the payload will be created.
    config: PayloadConfig,
    /// The client that can interact with the chain.
    client: Client,
    /// The transaction pool.
    pool: Pool,
    /// How to spawn building tasks
    executor: Tasks,
    /// The deadline when this job should resolve.
    deadline: Pin<Box<Sleep>>,
    /// The interval at which the job should build a new payload after the last.
    interval: Interval,
    /// The best payload so far.
    best_payload: Option<Arc<BuiltPayload>>,
    /// Receiver for the block that is currently being built.
    pending_block: Option<PendingPayload>,
    /// Restricts how many generator tasks can be executed at once.
    payload_task_guard: PayloadTaskGuard,
    /// Caches all disk reads for the state the new payloads builds on
    ///
    /// This is used to avoid reading the same state over and over again when new attempts are
    /// triggered, because during the building process we'll repeatedly execute the transactions.
    cached_reads: Option<CachedReads>,
    /// metrics for this type
    metrics: PayloadBuilderMetrics,
    /// The type responsible for building payloads.
    ///
    /// See [PayloadBuilder]
    builder: Builder,
}

impl<Client, Pool, Tasks, Builder> Future for BasicPayloadJob<Client, Pool, Tasks, Builder>
where
    Client: StateProviderFactory + Clone + Unpin + 'static,
    Pool: TransactionPool + Unpin + 'static,
    Tasks: TaskSpawner + Clone + 'static,
    Builder: PayloadBuilder<Pool, Client> + Unpin + 'static,
{
    type Output = Result<(), PayloadBuilderError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // check if the deadline is reached
        if this.deadline.as_mut().poll(cx).is_ready() {
            trace!(target: "payload_builder", "payload building deadline reached");
            return Poll::Ready(Ok(()))
        }

        // check if the interval is reached
        while this.interval.poll_tick(cx).is_ready() {
            // start a new job if there is no pending block and we haven't reached the deadline
            if this.pending_block.is_none() {
                trace!(target: "payload_builder", "spawn new payload build task");
                let (tx, rx) = oneshot::channel();
                let client = this.client.clone();
                let pool = this.pool.clone();
                let cancel = Cancelled::default();
                let _cancel = cancel.clone();
                let guard = this.payload_task_guard.clone();
                let payload_config = this.config.clone();
                let best_payload = this.best_payload.clone();
                this.metrics.inc_initiated_payload_builds();
                let cached_reads = this.cached_reads.take().unwrap_or_default();
                let builder = this.builder.clone();
                this.executor.spawn_blocking(Box::pin(async move {
                    // acquire the permit for executing the task
                    let _permit = guard.0.acquire().await;
                    let args = BuildArguments {
                        client,
                        pool,
                        cached_reads,
                        config: payload_config,
                        cancel,
                        best_payload,
                    };
                    let result = builder.try_build(args);
                    let _ = tx.send(result);
                }));

                this.pending_block = Some(PendingPayload { _cancel, payload: rx });
            }
        }

        // poll the pending block
        if let Some(mut fut) = this.pending_block.take() {
            match fut.poll_unpin(cx) {
                Poll::Ready(Ok(outcome)) => {
                    this.interval.reset();
                    match outcome {
                        BuildOutcome::Better { payload, cached_reads } => {
                            this.cached_reads = Some(cached_reads);
                            debug!(target: "payload_builder", value = %payload.fees(), "built better payload");
                            let payload = Arc::new(payload);
                            this.best_payload = Some(payload);
                        }
                        BuildOutcome::Aborted { fees, cached_reads } => {
                            this.cached_reads = Some(cached_reads);
                            trace!(target: "payload_builder", worse_fees = %fees, "skipped payload build of worse block");
                        }
                        BuildOutcome::Cancelled => {
                            unreachable!("the cancel signal never fired")
                        }
                    }
                }
                Poll::Ready(Err(error)) => {
                    // job failed, but we simply try again next interval
                    debug!(target: "payload_builder", ?error, "payload build attempt failed");
                    this.metrics.inc_failed_payload_builds();
                }
                Poll::Pending => {
                    this.pending_block = Some(fut);
                }
            }
        }

        Poll::Pending
    }
}

impl<Client, Pool, Tasks, Builder> PayloadJob for BasicPayloadJob<Client, Pool, Tasks, Builder>
where
    Client: StateProviderFactory + Clone + Unpin + 'static,
    Pool: TransactionPool + Unpin + 'static,
    Tasks: TaskSpawner + Clone + 'static,
    Builder: PayloadBuilder<Pool, Client> + Unpin + 'static,
{
    type PayloadAttributes = PayloadBuilderAttributes;
    type ResolvePayloadFuture = ResolveBestPayload;

    fn best_payload(&self) -> Result<Arc<BuiltPayload>, PayloadBuilderError> {
        if let Some(ref payload) = self.best_payload {
            return Ok(payload.clone())
        }
        // No payload has been built yet, but we need to return something that the CL then can
        // deliver, so we need to return an empty payload.
        //
        // Note: it is assumed that this is unlikely to happen, as the payload job is started right
        // away and the first full block should have been built by the time CL is requesting the
        // payload.
        self.metrics.inc_requested_empty_payload();
        build_empty_payload(&self.client, self.config.clone()).map(Arc::new)
    }

    fn payload_attributes(&self) -> Result<PayloadBuilderAttributes, PayloadBuilderError> {
        Ok(self.config.attributes.clone())
    }

    fn resolve(&mut self) -> (Self::ResolvePayloadFuture, KeepPayloadJobAlive) {
        let best_payload = self.best_payload.take();
        let maybe_better = self.pending_block.take();
        let mut empty_payload = None;

        if best_payload.is_none() {
            debug!(target: "payload_builder", id=%self.config.payload_id(), "no best payload yet to resolve, building empty payload");

            let args = BuildArguments {
                client: self.client.clone(),
                pool: self.pool.clone(),
                cached_reads: self.cached_reads.take().unwrap_or_default(),
                config: self.config.clone(),
                cancel: Cancelled::default(),
                best_payload: None,
            };

            if let Some(payload) = self.builder.on_missing_payload(args) {
                return (
                    ResolveBestPayload { best_payload: Some(payload), maybe_better, empty_payload },
                    KeepPayloadJobAlive::Yes,
                )
            }

            // if no payload has been built yet
            self.metrics.inc_requested_empty_payload();
            // no payload built yet, so we need to return an empty payload
            let (tx, rx) = oneshot::channel();
            let client = self.client.clone();
            let config = self.config.clone();
            self.executor.spawn_blocking(Box::pin(async move {
                let res = build_empty_payload(&client, config);
                let _ = tx.send(res);
            }));

            empty_payload = Some(rx);
        }

        let fut = ResolveBestPayload { best_payload, maybe_better, empty_payload };

        (fut, KeepPayloadJobAlive::No)
    }
}

/// The future that returns the best payload to be served to the consensus layer.
///
/// This returns the payload that's supposed to be sent to the CL.
///
/// If payload has been built so far, it will return that, but it will check if there's a better
/// payload available from an in progress build job. If so it will return that.
///
/// If no payload has been built so far, it will either return an empty payload or the result of the
/// in progress build job, whatever finishes first.
#[derive(Debug)]
pub struct ResolveBestPayload {
    /// Best payload so far.
    best_payload: Option<Arc<BuiltPayload>>,
    /// Regular payload job that's currently running that might produce a better payload.
    maybe_better: Option<PendingPayload>,
    /// The empty payload building job in progress.
    empty_payload: Option<oneshot::Receiver<Result<BuiltPayload, PayloadBuilderError>>>,
}

impl Future for ResolveBestPayload {
    type Output = Result<Arc<BuiltPayload>, PayloadBuilderError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // check if there is a better payload before returning the best payload
        if let Some(fut) = Pin::new(&mut this.maybe_better).as_pin_mut() {
            if let Poll::Ready(res) = fut.poll(cx) {
                this.maybe_better = None;
                if let Ok(BuildOutcome::Better { payload, .. }) = res {
                    debug!(target: "payload_builder", "resolving better payload");
                    return Poll::Ready(Ok(Arc::new(payload)))
                }
            }
        }

        if let Some(best) = this.best_payload.take() {
            debug!(target: "payload_builder", "resolving best payload");
            return Poll::Ready(Ok(best))
        }

        let mut empty_payload = this.empty_payload.take().expect("polled after completion");
        match empty_payload.poll_unpin(cx) {
            Poll::Ready(Ok(res)) => {
                if let Err(err) = &res {
                    warn!(target: "payload_builder", ?err, "failed to resolve empty payload");
                } else {
                    debug!(target: "payload_builder", "resolving empty payload");
                }
                Poll::Ready(res.map(Arc::new))
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err.into())),
            Poll::Pending => {
                this.empty_payload = Some(empty_payload);
                Poll::Pending
            }
        }
    }
}

/// A future that resolves to the result of the block building job.
#[derive(Debug)]
struct PendingPayload {
    /// The marker to cancel the job on drop
    _cancel: Cancelled,
    /// The channel to send the result to.
    payload: oneshot::Receiver<Result<BuildOutcome, PayloadBuilderError>>,
}

impl Future for PendingPayload {
    type Output = Result<BuildOutcome, PayloadBuilderError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res = ready!(self.payload.poll_unpin(cx));
        Poll::Ready(res.map_err(Into::into).and_then(|res| res))
    }
}

/// A marker that can be used to cancel a job.
///
/// If dropped, it will set the `cancelled` flag to true.
#[derive(Default, Clone, Debug)]
pub struct Cancelled(Arc<AtomicBool>);

// === impl Cancelled ===

impl Cancelled {
    /// Returns true if the job was cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Drop for Cancelled {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Static config for how to build a payload.
#[derive(Clone, Debug)]
pub struct PayloadConfig {
    /// Pre-configured block environment.
    pub initialized_block_env: BlockEnv,
    /// Configuration for the environment.
    pub initialized_cfg: CfgEnv,
    /// The parent block.
    pub parent_block: Arc<SealedBlock>,
    /// Block extra data.
    pub extra_data: Bytes,
    /// Requested attributes for the payload.
    pub attributes: PayloadBuilderAttributes,
    /// The chain spec.
    pub chain_spec: Arc<ChainSpec>,
}

impl PayloadConfig {
    /// Returns an owned instance of the [PayloadConfig]'s extra_data bytes.
    pub fn extra_data(&self) -> Bytes {
        self.extra_data.clone()
    }

    /// Returns the payload id.
    pub fn payload_id(&self) -> PayloadId {
        self.attributes.id
    }
}

impl PayloadConfig {
    /// Create new payload config.
    pub fn new(
        parent_block: Arc<SealedBlock>,
        extra_data: Bytes,
        attributes: PayloadBuilderAttributes,
        chain_spec: Arc<ChainSpec>,
    ) -> Self {
        // configure evm env based on parent block
        let (initialized_cfg, initialized_block_env) =
            attributes.cfg_and_block_env(&chain_spec, &parent_block);

        Self {
            initialized_block_env,
            initialized_cfg,
            parent_block,
            extra_data,
            attributes,
            chain_spec,
        }
    }
}

/// The possible outcomes of a payload building attempt.
#[derive(Debug)]
pub enum BuildOutcome {
    /// Successfully built a better block.
    Better {
        /// The new payload that was built.
        payload: BuiltPayload,
        /// The cached reads that were used to build the payload.
        cached_reads: CachedReads,
    },
    /// Aborted payload building because resulted in worse block wrt. fees.
    Aborted {
        /// The total fees associated with the attempted payload.
        fees: U256,
        /// The cached reads that were used to build the payload.
        cached_reads: CachedReads,
    },
    /// Build job was cancelled
    Cancelled,
}

/// A collection of arguments used for building payloads.
///
/// This struct encapsulates the essential components and configuration required for the payload
/// building process. It holds references to the Ethereum client, transaction pool, cached reads,
/// payload configuration, cancellation status, and the best payload achieved so far.
#[derive(Debug)]
pub struct BuildArguments<Pool, Client> {
    /// How to interact with the chain.
    pub client: Client,
    /// The transaction pool.
    pub pool: Pool,
    /// Previously cached disk reads
    pub cached_reads: CachedReads,
    /// How to configure the payload.
    pub config: PayloadConfig,
    /// A marker that can be used to cancel the job.
    pub cancel: Cancelled,
    /// The best payload achieved so far.
    pub best_payload: Option<Arc<BuiltPayload>>,
}

impl<Pool, Client> BuildArguments<Pool, Client> {
    /// Create new build arguments.
    pub fn new(
        client: Client,
        pool: Pool,
        cached_reads: CachedReads,
        config: PayloadConfig,
        cancel: Cancelled,
        best_payload: Option<Arc<BuiltPayload>>,
    ) -> Self {
        Self { client, pool, cached_reads, config, cancel, best_payload }
    }
}

/// A trait for building payloads that encapsulate Ethereum transactions.
///
/// This trait provides the `try_build` method to construct a transaction payload
/// using `BuildArguments`. It returns a `Result` indicating success or a
/// `PayloadBuilderError` if building fails.
///
/// Generic parameters `Pool` and `Client` represent the transaction pool and
/// Ethereum client types.
pub trait PayloadBuilder<Pool, Client>: Send + Sync + Clone {
    /// Tries to build a transaction payload using provided arguments.
    ///
    /// Constructs a transaction payload based on the given arguments,
    /// returning a `Result` indicating success or an error if building fails.
    ///
    /// # Arguments
    ///
    /// - `args`: Build arguments containing necessary components.
    ///
    /// # Returns
    ///
    /// A `Result` indicating the build outcome or an error.
    fn try_build(
        &self,
        args: BuildArguments<Pool, Client>,
    ) -> Result<BuildOutcome, PayloadBuilderError>;

    /// Invoked when the payload job is being resolved and there is no payload yet.
    ///
    /// If this returns a payload, it will be used as the final payload for the job.
    ///
    /// TODO(mattsse): This needs to be refined a bit because this only exists for OP atm
    fn on_missing_payload(&self, args: BuildArguments<Pool, Client>) -> Option<Arc<BuiltPayload>> {
        let _args = args;
        None
    }
}

/// Builds an empty payload without any transactions.
fn build_empty_payload<Client>(
    client: &Client,
    config: PayloadConfig,
) -> Result<BuiltPayload, PayloadBuilderError>
where
    Client: StateProviderFactory,
{
    let extra_data = config.extra_data();
    let PayloadConfig {
        initialized_block_env,
        parent_block,
        attributes,
        chain_spec,
        initialized_cfg,
        ..
    } = config;

    debug!(target: "payload_builder", parent_hash = ?parent_block.hash, parent_number = parent_block.number, "building empty payload");

    let state = client.state_by_block_hash(parent_block.hash).map_err(|err| {
        warn!(target: "payload_builder", parent_hash=%parent_block.hash, ?err,  "failed to get state for empty payload");
        err
    })?;
    let mut db = State::builder()
        .with_database_boxed(Box::new(StateProviderDatabase::new(&state)))
        .with_bundle_update()
        .build();

    let base_fee = initialized_block_env.basefee.to::<u64>();
    let block_number = initialized_block_env.number.to::<u64>();
    let block_gas_limit: u64 = initialized_block_env.gas_limit.try_into().unwrap_or(u64::MAX);

    // apply eip-4788 pre block contract call
    pre_block_beacon_root_contract_call(
        &mut db,
        &chain_spec,
        block_number,
        &initialized_cfg,
        &initialized_block_env,
        &attributes,
    ).map_err(|err| {
        warn!(target: "payload_builder", parent_hash=%parent_block.hash, ?err,  "failed to apply beacon root contract call for empty payload");
        err
    })?;

    let WithdrawalsOutcome { withdrawals_root, withdrawals } =
        commit_withdrawals(&mut db, &chain_spec, attributes.timestamp, attributes.withdrawals).map_err(|err| {
            warn!(target: "payload_builder", parent_hash=%parent_block.hash,?err,  "failed to commit withdrawals for empty payload");
            err
        })?;

    // merge all transitions into bundle state, this would apply the withdrawal balance changes and
    // 4788 contract call
    db.merge_transitions(BundleRetention::PlainState);

    // calculate the state root
    let bundle_state =
        BundleStateWithReceipts::new(db.take_bundle(), Receipts::new(), block_number);
    let state_root = state.state_root(&bundle_state).map_err(|err| {
        warn!(target: "payload_builder", parent_hash=%parent_block.hash, ?err,  "failed to calculate state root for empty payload");
        err
    })?;

    let header = Header {
        parent_hash: parent_block.hash,
        ommers_hash: EMPTY_OMMER_ROOT_HASH,
        beneficiary: initialized_block_env.coinbase,
        state_root,
        transactions_root: EMPTY_TRANSACTIONS,
        withdrawals_root,
        receipts_root: EMPTY_RECEIPTS,
        logs_bloom: Default::default(),
        timestamp: attributes.timestamp,
        mix_hash: attributes.prev_randao,
        nonce: BEACON_NONCE,
        base_fee_per_gas: Some(base_fee),
        number: parent_block.number + 1,
        gas_limit: block_gas_limit,
        difficulty: U256::ZERO,
        gas_used: 0,
        extra_data,
        blob_gas_used: None,
        excess_blob_gas: None,
        parent_beacon_block_root: attributes.parent_beacon_block_root,
    };

    let block = Block { header, body: vec![], ommers: vec![], withdrawals };
    let sealed_block = block.seal_slow();

    Ok(BuiltPayload::new(attributes.id, sealed_block, U256::ZERO))
}

/// Represents the outcome of committing withdrawals to the runtime database and post state.
/// Pre-shanghai these are `None` values.
#[derive(Default, Debug)]
pub struct WithdrawalsOutcome {
    /// committed withdrawals, if any.
    pub withdrawals: Option<Vec<Withdrawal>>,
    /// withdrawals root if any.
    pub withdrawals_root: Option<B256>,
}

impl WithdrawalsOutcome {
    /// No withdrawals pre shanghai
    pub fn pre_shanghai() -> Self {
        Self { withdrawals: None, withdrawals_root: None }
    }

    /// No withdrawals
    pub fn empty() -> Self {
        Self { withdrawals: Some(vec![]), withdrawals_root: Some(EMPTY_WITHDRAWALS) }
    }
}

/// Executes the withdrawals and commits them to the _runtime_ Database and BundleState.
///
/// Returns the withdrawals root.
///
/// Returns `None` values pre shanghai
pub fn commit_withdrawals<DB: Database<Error = ProviderError>>(
    db: &mut State<DB>,
    chain_spec: &ChainSpec,
    timestamp: u64,
    withdrawals: Vec<Withdrawal>,
) -> RethResult<WithdrawalsOutcome> {
    if !chain_spec.is_shanghai_active_at_timestamp(timestamp) {
        return Ok(WithdrawalsOutcome::pre_shanghai())
    }

    if withdrawals.is_empty() {
        return Ok(WithdrawalsOutcome::empty())
    }

    let balance_increments =
        post_block_withdrawals_balance_increments(chain_spec, timestamp, &withdrawals);

    db.increment_balances(balance_increments)?;

    let withdrawals_root = proofs::calculate_withdrawals_root(&withdrawals);

    // calculate withdrawals root
    Ok(WithdrawalsOutcome {
        withdrawals: Some(withdrawals),
        withdrawals_root: Some(withdrawals_root),
    })
}

/// Apply the [EIP-4788](https://eips.ethereum.org/EIPS/eip-4788) pre block contract call.
///
/// This constructs a new [EVM](revm::EVM) with the given DB, and environment ([CfgEnv] and
/// [BlockEnv]) to execute the pre block contract call.
///
/// The parent beacon block root used for the call is gathered from the given
/// [PayloadBuilderAttributes].
///
/// This uses [apply_beacon_root_contract_call] to ultimately apply the beacon root contract state
/// change.
pub fn pre_block_beacon_root_contract_call<DB: Database + DatabaseCommit>(
    db: &mut DB,
    chain_spec: &ChainSpec,
    block_number: u64,
    initialized_cfg: &CfgEnv,
    initialized_block_env: &BlockEnv,
    attributes: &PayloadBuilderAttributes,
) -> Result<(), PayloadBuilderError>
where
    DB::Error: std::fmt::Display,
{
    // Configure the environment for the block.
    let env = Env {
        cfg: initialized_cfg.clone(),
        block: initialized_block_env.clone(),
        ..Default::default()
    };

    // apply pre-block EIP-4788 contract call
    let mut evm_pre_block = revm::EVM::with_env(env);
    evm_pre_block.database(db);

    // initialize a block from the env, because the pre block call needs the block itself
    apply_beacon_root_contract_call(
        chain_spec,
        attributes.timestamp,
        block_number,
        attributes.parent_beacon_block_root,
        &mut evm_pre_block,
    )
    .map_err(|err| PayloadBuilderError::Internal(err.into()))
}

/// Checks if the new payload is better than the current best.
///
/// This compares the total fees of the blocks, higher is better.
#[inline(always)]
pub fn is_better_payload(best_payload: Option<&BuiltPayload>, new_fees: U256) -> bool {
    if let Some(best_payload) = best_payload {
        new_fees > best_payload.fees()
    } else {
        true
    }
}

/// Returns the duration until the given unix timestamp in seconds.
///
/// Returns `Duration::ZERO` if the given timestamp is in the past.
fn duration_until(unix_timestamp_secs: u64) -> Duration {
    let unix_now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let timestamp = Duration::from_secs(unix_timestamp_secs);
    timestamp.saturating_sub(unix_now)
}
