import {
	User,
	ReferrerInfo,
	isOracleValid,
	DriftClient,
	PerpMarketAccount,
	calculateAskPrice,
	calculateBidPrice,
	MakerInfo,
	isFillableByVAMM,
	calculateBaseAssetAmountForAmmToFulfill,
	isVariant,
	DLOB,
	NodeToFill,
	UserMap,
	UserStatsMap,
	MarketType,
	isOrderExpired,
	getVariant,
	PRICE_PRECISION,
	convertToNumber,
	BASE_PRECISION,
	QUOTE_PRECISION,
	WrappedEvent,
	PerpMarkets,
	OrderActionRecord,
	BulkAccountLoader,
	SlotSubscriber,
	OrderRecord,
} from '@drift-labs/sdk';
import { TxSigAndSlot } from '@drift-labs/sdk/lib/tx/types';
import { Mutex, tryAcquire, withTimeout, E_ALREADY_LOCKED } from 'async-mutex';

import {
	SendTransactionError,
	Transaction,
	TransactionResponse,
	TransactionSignature,
	TransactionInstruction,
	ComputeBudgetProgram,
	GetVersionedTransactionConfig,
	AddressLookupTableAccount,
} from '@solana/web3.js';

import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import {
	ExplicitBucketHistogramAggregation,
	InstrumentType,
	MeterProvider,
	View,
} from '@opentelemetry/sdk-metrics-base';
import {
	Meter,
	ObservableGauge,
	Counter,
	BatchObservableResult,
	Histogram,
} from '@opentelemetry/api-metrics';

import { logger } from '../logger';
import { Bot } from '../types';
import { FillerConfig } from '../config';
import { RuntimeSpec, metricAttrFromUserAccount } from '../metrics';
import { webhookMessage } from '../webhook';
import {
	isEndIxLog,
	isErrFillingLog,
	isErrStaleOracle,
	isFillIxLog,
	isIxLog,
	isMakerBreachedMaintenanceMarginLog,
	isMakerFallbackLog,
	isMakerOrderDoesNotExistLog,
	isOrderDoesNotExistLog,
	isTakerBreachedMaintenanceMarginLog,
} from './common/txLogParse';
import { getErrorCode } from '../error';

const MAX_TX_PACK_SIZE = 900; //1232;
const CU_PER_FILL = 200_000; // CU cost for a successful fill
const BURST_CU_PER_FILL = 350_000; // CU cost for a successful fill
const MAX_CU_PER_TX = 1_400_000; // seems like this is all budget program gives us...on devnet
const TX_COUNT_COOLDOWN_ON_BURST = 10; // send this many tx before resetting burst mode
const FILL_ORDER_THROTTLE_BACKOFF = 10000; // the time to wait before trying to fill a throttled (error filling) node again
const FILL_ORDER_COOLDOWN_BACKOFF = 2000; // the time to wait before trying to a node in the filling map again
const USER_MAP_RESYNC_COOLDOWN_SLOTS = 50;
const dlobMutexError = new Error('dlobMutex timeout');

const errorCodesToSuppress = [
	6081, // 0x17c1 Error Number: 6081. Error Message: MarketWrongMutability.
];

enum METRIC_TYPES {
	sdk_call_duration_histogram = 'sdk_call_duration_histogram',
	try_fill_duration_histogram = 'try_fill_duration_histogram',
	runtime_specs = 'runtime_specs',
	total_collateral = 'total_collateral',
	last_try_fill_time = 'last_try_fill_time',
	unrealized_pnl = 'unrealized_pnl',
	mutex_busy = 'mutex_busy',
	attempted_fills = 'attempted_fills',
	successful_fills = 'successful_fills',
	observed_fills_count = 'observed_fills_count',
	tx_sim_error_count = 'tx_sim_error_count',
	user_map_user_account_keys = 'user_map_user_account_keys',
	user_stats_map_authority_keys = 'user_stats_map_authority_keys',
}

export class FillerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 6000;

	private slotSubscriber: SlotSubscriber;
	private bulkAccountLoader: BulkAccountLoader | undefined;
	private driftClient: DriftClient;
	private pollingIntervalMs: number;
	private transactionVersion: number | undefined;
	private lookupTableAccount: AddressLookupTableAccount;

	private dlobMutex = withTimeout(
		new Mutex(),
		2 * this.defaultIntervalMs,
		dlobMutexError
	);
	private dlob: DLOB;

	private userMapMutex = new Mutex();
	private userMap: UserMap;
	private userStatsMap: UserStatsMap;

	private periodicTaskMutex = new Mutex();

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	private lastSlotReyncUserMapsMutex = new Mutex();
	private lastSlotResyncUserMaps = 0;

	private intervalIds: Array<NodeJS.Timer> = [];
	private throttledNodes = new Map<string, number>();
	private fillingNodes = new Map<string, number>();
	private useBurstCULimit = false;
	private fillTxSinceBurstCU = 0;

	// metrics
	private metricsInitialized = false;
	private metricsPort: number | undefined;
	private meter: Meter;
	private exporter: PrometheusExporter;
	private bootTimeMs: number;

	private runtimeSpecsGauge: ObservableGauge;
	private runtimeSpec: RuntimeSpec;
	private sdkCallDurationHistogram: Histogram;
	private tryFillDurationHistogram: Histogram;
	private lastTryFillTimeGauge: ObservableGauge;
	private totalCollateralGauge: ObservableGauge;
	private unrealizedPnLGauge: ObservableGauge;
	private mutexBusyCounter: Counter;
	private attemptedFillsCounter: Counter;
	private successfulFillsCounter: Counter;
	private observedFillsCountCounter: Counter;
	private txSimErrorCounter: Counter;
	private userMapUserAccountKeysGauge: ObservableGauge;
	private userStatsMapAuthorityKeysGauge: ObservableGauge;

	constructor(
		slotSubscriber: SlotSubscriber,
		bulkAccountLoader: BulkAccountLoader | undefined,
		driftClient: DriftClient,
		runtimeSpec: RuntimeSpec,
		config: FillerConfig
	) {
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.slotSubscriber = slotSubscriber;
		this.bulkAccountLoader = bulkAccountLoader;
		this.driftClient = driftClient;
		this.runtimeSpec = runtimeSpec;
		this.pollingIntervalMs =
			config.fillerPollingInterval ?? this.defaultIntervalMs;

		this.metricsPort = config.metricsPort;
		if (this.metricsPort) {
			this.initializeMetrics();
		}

		this.transactionVersion = config.transactionVersion ?? undefined;
		logger.info(
			`${this.name}: using transactionVersion: ${this.transactionVersion}`
		);
	}

	private initializeMetrics() {
		if (this.metricsInitialized) {
			logger.error('Tried to initilaize metrics multiple times');
			return;
		}
		this.metricsInitialized = true;

		const { endpoint: defaultEndpoint } = PrometheusExporter.DEFAULT_OPTIONS;
		this.exporter = new PrometheusExporter(
			{
				port: this.metricsPort,
				endpoint: defaultEndpoint,
			},
			() => {
				logger.info(
					`prometheus scrape endpoint started: http://localhost:${this.metricsPort}${defaultEndpoint}`
				);
			}
		);
		const meterName = this.name;
		const meterProvider = new MeterProvider({
			views: [
				new View({
					instrumentName: METRIC_TYPES.sdk_call_duration_histogram,
					instrumentType: InstrumentType.HISTOGRAM,
					meterName: meterName,
					aggregation: new ExplicitBucketHistogramAggregation(
						Array.from(new Array(20), (_, i) => 0 + i * 100),
						true
					),
				}),
				new View({
					instrumentName: METRIC_TYPES.try_fill_duration_histogram,
					instrumentType: InstrumentType.HISTOGRAM,
					meterName: meterName,
					aggregation: new ExplicitBucketHistogramAggregation(
						Array.from(new Array(20), (_, i) => 0 + i * 5),
						true
					),
				}),
			],
		});

		meterProvider.addMetricReader(this.exporter);
		this.meter = meterProvider.getMeter(meterName);

		this.bootTimeMs = Date.now();

		this.runtimeSpecsGauge = this.meter.createObservableGauge(
			METRIC_TYPES.runtime_specs,
			{
				description: 'Runtime sepcification of this program',
			}
		);
		this.runtimeSpecsGauge.addCallback((obs) => {
			obs.observe(this.bootTimeMs, this.runtimeSpec);
		});
		this.totalCollateralGauge = this.meter.createObservableGauge(
			METRIC_TYPES.total_collateral,
			{
				description: 'Total collateral of the account',
			}
		);
		this.lastTryFillTimeGauge = this.meter.createObservableGauge(
			METRIC_TYPES.last_try_fill_time,
			{
				description: 'Last time that fill was attempted',
			}
		);
		this.unrealizedPnLGauge = this.meter.createObservableGauge(
			METRIC_TYPES.unrealized_pnl,
			{
				description: 'The account unrealized PnL',
			}
		);

		this.mutexBusyCounter = this.meter.createCounter(METRIC_TYPES.mutex_busy, {
			description: 'Count of times the mutex was busy',
		});
		this.successfulFillsCounter = this.meter.createCounter(
			METRIC_TYPES.successful_fills,
			{
				description: 'Count of fills that we successfully landed',
			}
		);
		this.attemptedFillsCounter = this.meter.createCounter(
			METRIC_TYPES.attempted_fills,
			{
				description: 'Count of fills we attempted',
			}
		);
		this.observedFillsCountCounter = this.meter.createCounter(
			METRIC_TYPES.observed_fills_count,
			{
				description: 'Count of fills observed in the market',
			}
		);
		this.txSimErrorCounter = this.meter.createCounter(
			METRIC_TYPES.tx_sim_error_count,
			{
				description: 'Count of errors from simulating transactions',
			}
		);
		this.userMapUserAccountKeysGauge = this.meter.createObservableGauge(
			METRIC_TYPES.user_map_user_account_keys,
			{
				description: 'number of user account keys in UserMap',
			}
		);
		this.userMapUserAccountKeysGauge.addCallback(async (obs) => {
			obs.observe(this.userMap.size());
		});

		this.userStatsMapAuthorityKeysGauge = this.meter.createObservableGauge(
			METRIC_TYPES.user_stats_map_authority_keys,
			{
				description: 'number of authority keys in UserStatsMap',
			}
		);
		this.userStatsMapAuthorityKeysGauge.addCallback(async (obs) => {
			obs.observe(this.userStatsMap.size());
		});

		this.sdkCallDurationHistogram = this.meter.createHistogram(
			METRIC_TYPES.sdk_call_duration_histogram,
			{
				description: 'Distribution of sdk method calls',
				unit: 'ms',
			}
		);
		this.tryFillDurationHistogram = this.meter.createHistogram(
			METRIC_TYPES.try_fill_duration_histogram,
			{
				description: 'Distribution of tryFills',
				unit: 'ms',
			}
		);

		this.lastTryFillTimeGauge.addCallback(async (obs) => {
			await this.watchdogTimerMutex.runExclusive(async () => {
				const user = this.driftClient.getUser();
				obs.observe(
					this.watchdogTimerLastPatTime,
					metricAttrFromUserAccount(
						user.userAccountPublicKey,
						user.getUserAccount()
					)
				);
			});
		});

		this.meter.addBatchObservableCallback(
			async (batchObservableResult: BatchObservableResult) => {
				for (const user of this.driftClient.getUsers()) {
					const userAccount = user.getUserAccount();

					batchObservableResult.observe(
						this.totalCollateralGauge,
						convertToNumber(user.getTotalCollateral(), QUOTE_PRECISION),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);

					batchObservableResult.observe(
						this.unrealizedPnLGauge,
						convertToNumber(user.getUnrealizedPNL(), QUOTE_PRECISION),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);
				}
			},
			[this.totalCollateralGauge, this.unrealizedPnLGauge]
		);
	}

	public async init() {
		logger.info(`${this.name} initing`);

		await this.userMapMutex.runExclusive(async () => {
			this.userMap = new UserMap(
				this.driftClient,
				this.driftClient.userAccountSubscriptionConfig
			);
			this.userStatsMap = new UserStatsMap(
				this.driftClient,
				this.driftClient.userAccountSubscriptionConfig
			);

			await this.userMap.fetchAllUsers();
			await this.userStatsMap.fetchAllUserStats();
		});

		this.lookupTableAccount =
			await this.driftClient.fetchMarketLookupTableAccount();

		await webhookMessage(`[${this.name}]: started`);
	}

	public async reset() {}

	public async startIntervalLoop(_intervalMs: number) {
		const intervalId = setInterval(
			this.tryFill.bind(this),
			this.pollingIntervalMs
		);
		this.intervalIds.push(intervalId);

		logger.info(`${this.name} Bot started!`);
	}

	public async healthCheck(): Promise<boolean> {
		let healthy = false;
		await this.watchdogTimerMutex.runExclusive(async () => {
			healthy =
				this.watchdogTimerLastPatTime > Date.now() - 5 * this.pollingIntervalMs;
			if (!healthy) {
				logger.warn(
					`watchdog timer last pat time ${this.watchdogTimerLastPatTime} is too old`
				);
			}
		});

		const stateAccount = this.driftClient.getStateAccount();
		const userMapResyncRequired =
			this.userMap.size() !== stateAccount.numberOfSubAccounts.toNumber() ||
			this.userStatsMap.size() !== stateAccount.numberOfAuthorities.toNumber();
		if (userMapResyncRequired) {
			logger.warn(
				`${
					this.name
				} user map resync required, userMap size: ${this.userMap.size()}, stateAccount.numberOfSubAccounts: ${stateAccount.numberOfSubAccounts.toNumber()}, userStatsMap size: ${this.userStatsMap.size()}, stateAccount.numberOfAuthorities: ${stateAccount.numberOfAuthorities.toNumber()}`
			);
		}

		healthy = healthy && !userMapResyncRequired;

		return healthy;
	}

	public async trigger(record: WrappedEvent<any>) {
		logger.debug(
			`filler seen record (slot: ${record.slot}): ${record.eventType}`
		);
		if (record.order) {
			logger.debug(` . ${record.user} - ${record.order.orderId}`);
		}
		// potentially a race here, but the lock is really slow :/
		// await this.userMapMutex.runExclusive(async () => {
		await this.userMap.updateWithEventRecord(record);
		await this.userStatsMap.updateWithEventRecord(record, this.userMap);
		// });

		if (record.eventType === 'OrderRecord') {
			const orderRecord = record as OrderRecord;
			const marketType = getVariant(orderRecord.order.marketType);
			if (marketType === 'perp') {
				await this.tryFill(orderRecord);
			}
		} else if (record.eventType === 'OrderActionRecord') {
			const actionRecord = record as OrderActionRecord;
			if (getVariant(actionRecord.action) === 'fill') {
				const marketType = getVariant(actionRecord.marketType);
				if (marketType === 'perp') {
					if (PerpMarkets[this.runtimeSpec.driftEnv][actionRecord.marketIndex].symbol) {
						this.observedFillsCountCounter.add(1, {
							market:
							PerpMarkets[this.runtimeSpec.driftEnv][actionRecord.marketIndex]
								.symbol,
						});
					}
				}
			}
		}
	}

	public viewDlob(): DLOB {
		return this.dlob;
	}

	/**
	 * Checks that userMap and userStatsMap are up in sync with , if not, signal that we should update them next block.
	 */
	private async resyncUserMapsIfRequired() {
		const stateAccount = this.driftClient.getStateAccount();
		const resyncRequired =
			this.userMap.size() !== stateAccount.numberOfSubAccounts.toNumber() ||
			this.userStatsMap.size() !== stateAccount.numberOfAuthorities.toNumber();

		if (resyncRequired) {
			await this.lastSlotReyncUserMapsMutex.runExclusive(async () => {
				let doResync = false;
				const start = Date.now();
				if (!this.bulkAccountLoader) {
					logger.info(`Resyncing UserMaps immediately (no BulkAccountLoader)`);
					doResync = true;
				} else {
					const nextResyncSlot =
						this.lastSlotResyncUserMaps + USER_MAP_RESYNC_COOLDOWN_SLOTS;
					if (nextResyncSlot >= this.bulkAccountLoader.mostRecentSlot) {
						const slotsRemaining =
							nextResyncSlot - this.bulkAccountLoader.mostRecentSlot;
						if (slotsRemaining % 10 === 0) {
							logger.info(
								`Resyncing UserMaps in cooldown, ${slotsRemaining} more slots to go`
							);
						}
						return;
					} else {
						doResync = true;
						this.lastSlotResyncUserMaps = this.bulkAccountLoader.mostRecentSlot;
					}
				}

				if (doResync) {
					logger.info(`Resyncing UserMap`);
					const newUserMap = new UserMap(
						this.driftClient,
						this.driftClient.userAccountSubscriptionConfig
					);
					const newUserStatsMap = new UserStatsMap(
						this.driftClient,
						this.driftClient.userAccountSubscriptionConfig
					);
					newUserMap.fetchAllUsers().then(() => {
						newUserStatsMap
							.fetchAllUserStats()
							.then(async () => {
								await this.userMapMutex.runExclusive(async () => {
									for (const user of this.userMap.values()) {
										await user.unsubscribe();
									}
									for (const user of this.userStatsMap.values()) {
										await user.unsubscribe();
									}
									delete this.userMap;
									delete this.userStatsMap;

									this.userMap = newUserMap;
									this.userStatsMap = newUserStatsMap;
								});
							})
							.finally(() => {
								logger.info(`UserMaps resynced in ${Date.now() - start}ms`);
							});
					});
				}
			});
		}
	}

	private async getPerpFillableNodesForMarket(
		market: PerpMarketAccount
	): Promise<Array<NodeToFill>> {
		const marketIndex = market.marketIndex;

		const oraclePriceData =
			this.driftClient.getOracleDataForPerpMarket(marketIndex);

		const vAsk = calculateAskPrice(market, oraclePriceData);
		const vBid = calculateBidPrice(market, oraclePriceData);

		let nodes: Array<NodeToFill> = [];
		await this.dlobMutex.runExclusive(async () => {
			nodes = this.dlob.findNodesToFill(
				marketIndex,
				vBid,
				vAsk,
				this.slotSubscriber.currentSlot,
				Date.now() / 1000,
				MarketType.PERP,
				oraclePriceData,
				this.driftClient.getStateAccount(),
				this.driftClient.getPerpMarketAccount(marketIndex)
			);
		});

		return nodes;
	}

	private getNodeToFillSignature(node: NodeToFill): string {
		if (!node.node.userAccount) {
			return '~';
		}
		return this.getFillSignatureFromUserAccountAndOrderId(
			node.node.userAccount.toString(),
			node.node.order.orderId.toString()
		);
	}

	private getFillSignatureFromUserAccountAndOrderId(
		userAccount: string,
		orderId: string
	): string {
		return `${userAccount}-${orderId}`;
	}

	private filterFillableNodes(nodeToFill: NodeToFill): boolean {
		if (nodeToFill.node.isVammNode()) {
			logger.warn(
				`filtered out a vAMM node on market ${nodeToFill.node.order.marketIndex} for user ${nodeToFill.node.userAccount}-${nodeToFill.node.order.orderId}`
			);
			return false;
		}

		if (nodeToFill.node.haveFilled) {
			logger.warn(
				`filtered out filled node on market ${nodeToFill.node.order.marketIndex} for user ${nodeToFill.node.userAccount}-${nodeToFill.node.order.orderId}`
			);
			return false;
		}

		const now = Date.now();
		const nodeToFillSignature = this.getNodeToFillSignature(nodeToFill);
		if (this.fillingNodes.has(nodeToFillSignature)) {
			const timeStartedToFillNode = this.fillingNodes.get(nodeToFillSignature);
			if (timeStartedToFillNode + FILL_ORDER_COOLDOWN_BACKOFF > now) {
				// still cooling down on this node, filter it out
				return false;
			}
		}

		if (this.throttledNodes.has(nodeToFillSignature)) {
			const lastFillAttempt = this.throttledNodes.get(nodeToFillSignature);
			if (lastFillAttempt + FILL_ORDER_THROTTLE_BACKOFF > now) {
				logger.warn(
					`skipping node (throttled, retry in ${
						lastFillAttempt + FILL_ORDER_THROTTLE_BACKOFF - now
					}ms) on market ${nodeToFill.node.order.marketIndex} for user ${
						nodeToFill.node.userAccount
					}-${nodeToFill.node.order.orderId}`
				);
				return false;
			} else {
				this.throttledNodes.delete(nodeToFillSignature);
			}
		}

		const marketIndex = nodeToFill.node.order.marketIndex;
		const oraclePriceData =
			this.driftClient.getOracleDataForPerpMarket(marketIndex);

		// return early to fill if order is expired
		if (isOrderExpired(nodeToFill.node.order, Date.now() / 1000)) {
			logger.warn(
				`order is expired on market ${nodeToFill.node.order.marketIndex} for user ${nodeToFill.node.userAccount}-${nodeToFill.node.order.orderId}`
			);
			return true;
		}

		if (
			!nodeToFill.makerNode &&
			isVariant(nodeToFill.node.order.marketType, 'perp') &&
			!isFillableByVAMM(
				nodeToFill.node.order,
				this.driftClient.getPerpMarketAccount(
					nodeToFill.node.order.marketIndex
				),
				oraclePriceData,
				this.slotSubscriber.currentSlot,
				Date.now() / 1000
			)
		) {
			logger.warn(
				`filtered out unfillable node on market ${nodeToFill.node.order.marketIndex} for user ${nodeToFill.node.userAccount}-${nodeToFill.node.order.orderId}`
			);
			logger.warn(` . no maker node: ${!nodeToFill.makerNode}`);
			logger.warn(
				` . is perp: ${isVariant(nodeToFill.node.order.marketType, 'perp')}`
			);
			logger.warn(
				` . is not fillable by vamm: ${!isFillableByVAMM(
					nodeToFill.node.order,
					this.driftClient.getPerpMarketAccount(
						nodeToFill.node.order.marketIndex
					),
					oraclePriceData,
					this.slotSubscriber.currentSlot,
					Date.now() / 1000
				)}`
			);
			logger.warn(
				` .     calculateBaseAssetAmountForAmmToFulfill: ${calculateBaseAssetAmountForAmmToFulfill(
					nodeToFill.node.order,
					this.driftClient.getPerpMarketAccount(
						nodeToFill.node.order.marketIndex
					),
					oraclePriceData,
					this.slotSubscriber.currentSlot
				).toString()}`
			);
			return false;
		}

		// if making with vAMM, ensure valid oracle
		if (!nodeToFill.makerNode) {
			const oracleIsValid = isOracleValid(
				this.driftClient.getPerpMarketAccount(nodeToFill.node.order.marketIndex)
					.amm,
				oraclePriceData,
				this.driftClient.getStateAccount().oracleGuardRails,
				this.slotSubscriber.currentSlot
			);
			if (!oracleIsValid) {
				logger.error(`Oracle is not valid for market ${marketIndex}`);
				return false;
			}
		}

		return true;
	}

	private async getNodeFillInfo(nodeToFill: NodeToFill): Promise<{
		makerInfo: MakerInfo | undefined;
		chUser: User;
		referrerInfo: ReferrerInfo;
		marketType: MarketType;
	}> {
		let makerInfo: MakerInfo | undefined;
		let chUser: User;
		let referrerInfo: ReferrerInfo;
		await tryAcquire(this.userMapMutex).runExclusive(async () => {
			if (nodeToFill.makerNode) {
				const makerUserAccount = (
					await this.userMap.mustGet(
						nodeToFill.makerNode.userAccount.toString()
					)
				).getUserAccount();
				const makerAuthority = makerUserAccount.authority;
				const makerUserStats = (
					await this.userStatsMap.mustGet(makerAuthority.toString())
				).userStatsAccountPublicKey;
				makerInfo = {
					maker: nodeToFill.makerNode.userAccount,
					makerUserAccount: makerUserAccount,
					order: nodeToFill.makerNode.order,
					makerStats: makerUserStats,
				};
			}

			chUser = await this.userMap.mustGet(
				nodeToFill.node.userAccount.toString()
			);
			referrerInfo = (
				await this.userStatsMap.mustGet(
					chUser.getUserAccount().authority.toString()
				)
			).getReferrerInfo();
		});

		return Promise.resolve({
			makerInfo,
			chUser,
			referrerInfo,
			marketType: nodeToFill.node.order.marketType,
		});
	}

	/**
	 * Returns the number of bytes occupied by this array if it were serialized in compact-u16-format.
	 * NOTE: assumes each element of the array is 1 byte (not sure if this holds?)
	 *
	 * https://docs.solana.com/developing/programming-model/transactions#compact-u16-format
	 *
	 * https://stackoverflow.com/a/69951832
	 *  hex     |  compact-u16
	 *  --------+------------
	 *  0x0000  |  [0x00]
	 *  0x0001  |  [0x01]
	 *  0x007f  |  [0x7f]
	 *  0x0080  |  [0x80 0x01]
	 *  0x3fff  |  [0xff 0x7f]
	 *  0x4000  |  [0x80 0x80 0x01]
	 *  0xc000  |  [0x80 0x80 0x03]
	 *  0xffff  |  [0xff 0xff 0x03])
	 */
	private calcCompactU16EncodedSize(array: any[], elemSize = 1): number {
		if (array.length > 0x3fff) {
			return 3 + array.length * elemSize;
		} else if (array.length > 0x7f) {
			return 2 + array.length * elemSize;
		} else {
			return 1 + (array.length * elemSize || 1);
		}
	}

	/**
	 * Instruction are made of 3 parts:
	 * - index of accounts where programId resides (1 byte)
	 * - affected accounts    (compact-u16-format byte array)
	 * - raw instruction data (compact-u16-format byte array)
	 * @param ix The instruction to calculate size for.
	 */
	private calcIxEncodedSize(ix: TransactionInstruction): number {
		return (
			1 +
			this.calcCompactU16EncodedSize(new Array(ix.keys.length), 1) +
			this.calcCompactU16EncodedSize(new Array(ix.data.byteLength), 1)
		);
	}

	private async sleep(ms: number) {
		return new Promise((resolve) => setTimeout(resolve, ms));
	}

	/**
	 * Iterates through a tx's logs and handles it appropriately 3e.g. throttling users, updating metrics, etc.)
	 *
	 * @param nodesFilled nodes that we sent a transaction to fill
	 * @param logs logs from tx.meta.logMessages or this.clearingHouse.program._events._eventParser.parseLogs
	 *
	 * @returns number of nodes successfully filled
	 */
	private async handleTransactionLogs(
		nodesFilled: Array<NodeToFill>,
		logs: string[]
	): Promise<number> {
		let inFillIx = false;
		let errorThisFillIx = false;
		let ixIdx = -1; // skip ComputeBudgetProgram
		let successCount = 0;
		let burstedCU = false;
		for (const log of logs) {
			if (log === null) {
				logger.error(`log is null`);
				continue;
			}

			if (log.includes('exceeded maximum number of instructions allowed')) {
				// temporary burst CU limit
				logger.warn(`Using bursted CU limit`);
				this.useBurstCULimit = true;
				this.fillTxSinceBurstCU = 0;
				burstedCU = true;
				continue;
			}

			if (isEndIxLog(this.driftClient.program.programId.toBase58(), log)) {
				if (!errorThisFillIx) {
					successCount++;
				}

				inFillIx = false;
				errorThisFillIx = false;
				continue;
			}

			if (isIxLog(log)) {
				if (isFillIxLog(log)) {
					inFillIx = true;
					errorThisFillIx = false;
					ixIdx++;

					// can also print this from parsing the log record in upcoming
					const nodeFilled = nodesFilled[ixIdx];
					if (nodeFilled.makerNode) {
						logger.info(
							`Processing tx log for assoc node ${ixIdx}:\ntaker: ${nodeFilled.node.userAccount.toBase58()}-${
								nodeFilled.node.order.orderId
							} ${convertToNumber(
								nodeFilled.node.order.baseAssetAmountFilled,
								BASE_PRECISION
							)}/${convertToNumber(
								nodeFilled.node.order.baseAssetAmount,
								BASE_PRECISION
							)} @ ${convertToNumber(
								nodeFilled.node.order.price,
								PRICE_PRECISION
							)}\nmaker: ${nodeFilled.makerNode.userAccount.toBase58()}-${
								nodeFilled.makerNode.order.orderId
							} ${convertToNumber(
								nodeFilled.makerNode.order.baseAssetAmountFilled,
								BASE_PRECISION
							)}/${convertToNumber(
								nodeFilled.makerNode.order.baseAssetAmount,
								BASE_PRECISION
							)} @ ${convertToNumber(
								nodeFilled.makerNode.order.price,
								PRICE_PRECISION
							)}`
						);
					} else {
						logger.info(
							`Processing tx log for assoc node ${ixIdx}:\ntaker: ${nodeFilled.node.userAccount.toBase58()}-${
								nodeFilled.node.order.orderId
							} ${convertToNumber(
								nodeFilled.node.order.baseAssetAmountFilled,
								BASE_PRECISION
							)}/${convertToNumber(
								nodeFilled.node.order.baseAssetAmount,
								BASE_PRECISION
							)} @ ${convertToNumber(
								nodeFilled.node.order.price,
								PRICE_PRECISION
							)}\nmaker: vAMM`
						);
					}
				} else {
					inFillIx = false;
				}
				continue;
			}

			if (!inFillIx) {
				// this is not a log for a fill instruction
				continue;
			}

			// try to handle the log line
			const orderIdDoesNotExist = isOrderDoesNotExistLog(log);
			if (orderIdDoesNotExist) {
				const filledNode = nodesFilled[ixIdx];
				logger.error(
					`assoc node (ixIdx: ${ixIdx}): ${filledNode.node.userAccount.toString()}, ${
						filledNode.node.order.orderId
					}; does not exist (filled by someone else); ${log}`
				);
				this.throttledNodes.delete(this.getNodeToFillSignature(filledNode));
				errorThisFillIx = true;
				continue;
			}

			const makerOrderIdDoesNotExist = isMakerOrderDoesNotExistLog(log);
			if (makerOrderIdDoesNotExist) {
				const filledNode = nodesFilled[ixIdx];
				if (!filledNode.makerNode) {
					logger.error(
						`Got maker DNE error, but don't have a maker node: ${log}, ${ixIdx}\n${JSON.stringify(
							filledNode,
							null,
							2
						)}`
					);
					continue;
				}
				const makerNodeSignature =
					this.getFillSignatureFromUserAccountAndOrderId(
						filledNode.makerNode.userAccount.toString(),
						filledNode.makerNode.order.orderId.toString()
					);
				logger.error(
					`maker assoc node (ixIdx: ${ixIdx}): ${filledNode.makerNode.userAccount.toString()}, ${
						filledNode.makerNode.order.orderId
					}; does not exist; throttling: ${makerNodeSignature}; ${log}`
				);
				this.throttledNodes.set(makerNodeSignature, Date.now());
				continue;
			}

			const makerFallbackOrderId = isMakerFallbackLog(log);
			if (makerFallbackOrderId) {
				const filledNode = nodesFilled[ixIdx];
				if (!filledNode.makerNode) {
					logger.error(
						`Got maker fallback log, but don't have a maker node: ${log}, ${ixIdx}\n${JSON.stringify(
							filledNode,
							null,
							2
						)}`
					);
					continue;
				}
				const makerNodeSignature =
					this.getFillSignatureFromUserAccountAndOrderId(
						filledNode.makerNode.userAccount.toString(),
						makerFallbackOrderId.toString()
					);
				logger.error(
					`maker fallback order assoc node (ixIdx: ${ixIdx}): ${filledNode.makerNode.userAccount.toString()}, ${
						filledNode.makerNode.order.orderId
					}; throttling ${makerNodeSignature}; ${log}`
				);
				this.throttledNodes.set(makerNodeSignature, Date.now());
				continue;
			}

			const makerBreachedMaintenanceMargin =
				isMakerBreachedMaintenanceMarginLog(log);
			if (makerBreachedMaintenanceMargin) {
				const filledNode = nodesFilled[ixIdx];
				if (!filledNode.makerNode) {
					logger.error(
						`Got maker breached maint. margin log, but don't have a maker node: ${log}, ${ixIdx}\n${JSON.stringify(
							filledNode,
							null,
							2
						)}`
					);
					continue;
				}
				const makerNodeSignature =
					this.getFillSignatureFromUserAccountAndOrderId(
						filledNode.makerNode.userAccount.toString(),
						filledNode.makerNode.order.orderId.toString()
					);
				logger.error(
					`maker breach maint. margin, assoc node (ixIdx: ${ixIdx}): ${filledNode.makerNode.userAccount.toString()}, ${
						filledNode.makerNode.order.orderId
					}; (throttling ${makerNodeSignature}); ${log}`
				);
				this.throttledNodes.set(makerNodeSignature, Date.now());
				errorThisFillIx = true;

				const tx = new Transaction();
				tx.add(
					ComputeBudgetProgram.requestUnits({
						units: 1_000_000,
						additionalFee: 0,
					})
				);
				tx.add(
					await this.driftClient.getForceCancelOrdersIx(
						filledNode.makerNode.userAccount,
						(
							await this.userMap.mustGet(
								filledNode.makerNode.userAccount.toString()
							)
						).getUserAccount()
					)
				);
				this.driftClient.txSender
					.send(tx, [], this.driftClient.opts)
					.then((txSig) => {
						logger.info(
							`Force cancelled orders for maker ${filledNode.makerNode.userAccount.toBase58()} due to breach of maintenance margin. Tx: ${txSig}`
						);
					})
					.catch((e) => {
						console.error(e);
						logger.error(`Failed to send ForceCancelOrder Ixs (error above):`);
						webhookMessage(
							`[${this.name}]: :x: error processing fill tx logs:\n${
								e.stack ? e.stack : e.message
							}`
						);
					});

				continue;
			}

			const takerBreachedMaintenanceMargin =
				isTakerBreachedMaintenanceMarginLog(log);
			if (takerBreachedMaintenanceMargin) {
				const filledNode = nodesFilled[ixIdx];
				const takerNodeSignature =
					this.getFillSignatureFromUserAccountAndOrderId(
						filledNode.node.userAccount.toString(),
						filledNode.node.order.orderId.toString()
					);
				logger.error(
					`taker breach maint. margin, assoc node (ixIdx: ${ixIdx}): ${filledNode.node.userAccount.toString()}, ${
						filledNode.node.order.orderId
					}; (throttling ${takerNodeSignature} and force cancelling orders); ${log}`
				);
				this.throttledNodes.set(takerNodeSignature, Date.now());
				errorThisFillIx = true;

				const tx = new Transaction();
				tx.add(
					ComputeBudgetProgram.requestUnits({
						units: 1_000_000,
						additionalFee: 0,
					})
				);
				tx.add(
					await this.driftClient.getForceCancelOrdersIx(
						filledNode.node.userAccount,
						(
							await this.userMap.mustGet(filledNode.node.userAccount.toString())
						).getUserAccount()
					)
				);

				this.driftClient.txSender
					.send(tx, [], this.driftClient.opts)
					.then((txSig) => {
						logger.info(
							`Force cancelled orders for user ${filledNode.node.userAccount.toBase58()} due to breach of maintenance margin. Tx: ${txSig}`
						);
					})
					.catch((e) => {
						console.error(e);
						logger.error(`Failed to send ForceCancelOrder Ixs (error above):`);
						webhookMessage(
							`[${this.name}]: :x: error processing fill tx logs:\n${
								e.stack ? e.stack : e.message
							}`
						);
					});

				continue;
			}

			const errFillingLog = isErrFillingLog(log);
			if (errFillingLog) {
				const orderId = errFillingLog[0];
				const userAcc = errFillingLog[1];
				const extractedSig = this.getFillSignatureFromUserAccountAndOrderId(
					userAcc,
					orderId
				);
				this.throttledNodes.set(extractedSig, Date.now());

				const filledNode = nodesFilled[ixIdx];
				const assocNodeSig = this.getNodeToFillSignature(filledNode);
				logger.warn(
					`Throttling node due to fill error. extractedSig: ${extractedSig}, assocNodeSig: ${assocNodeSig}, assocNodeIdx: ${ixIdx}`
				);
				errorThisFillIx = true;
				continue;
			}

			if (isErrStaleOracle(log)) {
				logger.error(`Stale oracle error: ${log}`);
				errorThisFillIx = true;
				continue;
			}
		}

		if (!burstedCU) {
			if (this.fillTxSinceBurstCU > TX_COUNT_COOLDOWN_ON_BURST) {
				this.useBurstCULimit = false;
			}
			this.fillTxSinceBurstCU += 1;
		}

		return successCount;
	}

	private async processBulkFillTxLogs(
		nodesFilled: Array<NodeToFill>,
		txSig: TransactionSignature
	): Promise<number> {
		let tx: TransactionResponse | null = null;
		let attempts = 0;
		const config: GetVersionedTransactionConfig = {
			commitment: 'confirmed',
			maxSupportedTransactionVersion: 0,
		};
		while (tx === null && attempts < 10) {
			logger.info(`waiting for ${txSig} to be confirmed`);
			tx = await this.driftClient.connection.getTransaction(txSig, config);
			attempts++;
			await this.sleep(1000);
		}

		if (tx === null) {
			logger.error(`tx ${txSig} not found`);
			return 0;
		}

		return this.handleTransactionLogs(nodesFilled, tx.meta.logMessages);
	}

	private removeFillingNodes(nodes: Array<NodeToFill>) {
		for (const node of nodes) {
			this.fillingNodes.delete(this.getNodeToFillSignature(node));
		}
	}

	private async tryBulkFillPerpNodes(
		nodesToFill: Array<NodeToFill>
	): Promise<[TransactionSignature, number]> {
		const ixs: Array<TransactionInstruction> = [];
		let txSig = '';

		/**
		 * At all times, the running Tx size is:
		 * - signatures (compact-u16 array, 64 bytes per elem)
		 * - message header (3 bytes)
		 * - affected accounts (compact-u16 array, 32 bytes per elem)
		 * - previous block hash (32 bytes)
		 * - message instructions (
		 * 		- progamIdIdx (1 byte)
		 * 		- accountsIdx (compact-u16, 1 byte per elem)
		 *		- instruction data (compact-u16, 1 byte per elem)
		 */
		let runningTxSize = 0;
		let runningCUUsed = 0;

		const uniqueAccounts = new Set<string>();
		uniqueAccounts.add(this.driftClient.provider.wallet.publicKey.toString()); // fee payer goes first

		// first ix is compute budget
		const computeBudgetIx = ComputeBudgetProgram.requestUnits({
			units: 10_000_000,
			additionalFee: 0,
		});
		computeBudgetIx.keys.forEach((key) =>
			uniqueAccounts.add(key.pubkey.toString())
		);
		uniqueAccounts.add(computeBudgetIx.programId.toString());
		ixs.push(computeBudgetIx);

		// initialize the barebones transaction
		// signatures
		runningTxSize += this.calcCompactU16EncodedSize(new Array(1), 64);
		// message header
		runningTxSize += 3;
		// accounts
		runningTxSize += this.calcCompactU16EncodedSize(
			new Array(uniqueAccounts.size),
			32
		);
		// block hash
		runningTxSize += 32;
		runningTxSize += this.calcIxEncodedSize(computeBudgetIx);

		const txPackerStart = Date.now();
		const nodesSent: Array<NodeToFill> = [];
		let idxUsed = 0;
		for (const [idx, nodeToFill] of nodesToFill.entries()) {
			logger.info(
				`filling perp node ${idx}, marketIdx: ${
					nodeToFill.node.order.marketIndex
				}: ${nodeToFill.node.userAccount.toString()}, ${
					nodeToFill.node.order.orderId
				}, orderType: ${getVariant(nodeToFill.node.order.orderType)}`
			);
			if (nodeToFill.makerNode) {
				logger.info(
					`filling\ntaker: ${nodeToFill.node.userAccount.toBase58()}-${
						nodeToFill.node.order.orderId
					} ${convertToNumber(
						nodeToFill.node.order.baseAssetAmountFilled,
						BASE_PRECISION
					)}/${convertToNumber(
						nodeToFill.node.order.baseAssetAmount,
						BASE_PRECISION
					)} @ ${convertToNumber(
						nodeToFill.node.order.price,
						PRICE_PRECISION
					)}\nmaker: ${nodeToFill.makerNode.userAccount.toBase58()}-${
						nodeToFill.makerNode.order.orderId
					} ${convertToNumber(
						nodeToFill.makerNode.order.baseAssetAmountFilled,
						BASE_PRECISION
					)}/${convertToNumber(
						nodeToFill.makerNode.order.baseAssetAmount,
						BASE_PRECISION
					)} @ ${convertToNumber(
						nodeToFill.makerNode.order.price,
						PRICE_PRECISION
					)}`
				);
			} else {
				logger.info(
					`filling\ntaker: ${nodeToFill.node.userAccount.toBase58()}-${
						nodeToFill.node.order.orderId
					} ${convertToNumber(
						nodeToFill.node.order.baseAssetAmountFilled,
						BASE_PRECISION
					)}/${convertToNumber(
						nodeToFill.node.order.baseAssetAmount,
						BASE_PRECISION
					)} @ ${convertToNumber(
						nodeToFill.node.order.price,
						PRICE_PRECISION
					)}\nmaker: vAMM`
				);
			}

			const { makerInfo, chUser, referrerInfo, marketType } =
				await this.getNodeFillInfo(nodeToFill);

			if (!isVariant(marketType, 'perp')) {
				throw new Error('expected perp market type');
			}

			const ix = await this.driftClient.getFillPerpOrderIx(
				chUser.getUserAccountPublicKey(),
				chUser.getUserAccount(),
				nodeToFill.node.order,
				makerInfo,
				referrerInfo
			);

			if (!ix) {
				logger.error(`failed to generate an ix`);
				break;
			}

			this.fillingNodes.set(
				this.getNodeToFillSignature(nodeToFill),
				Date.now()
			);

			// first estimate new tx size with this additional ix and new accounts
			const ixKeys = ix.keys.map((key) => key.pubkey);
			const newAccounts = ixKeys
				.concat(ix.programId)
				.filter((key) => !uniqueAccounts.has(key.toString()));
			const newIxCost = this.calcIxEncodedSize(ix);
			const additionalAccountsCost =
				newAccounts.length > 0
					? this.calcCompactU16EncodedSize(newAccounts, 32) - 1
					: 0;

			// We have to use MAX_TX_PACK_SIZE because it appears we cannot send tx with a size of exactly 1232 bytes.
			// Also, some logs may get truncated near the end of the tx, so we need to leave some room for that.
			const cuToUsePerFill = this.useBurstCULimit
				? BURST_CU_PER_FILL
				: CU_PER_FILL;
			if (
				runningTxSize + newIxCost + additionalAccountsCost >=
					MAX_TX_PACK_SIZE ||
				runningCUUsed + cuToUsePerFill >= MAX_CU_PER_TX
			) {
				logger.info(
					`Fully packed fill tx: est. tx size ${
						runningTxSize + newIxCost + additionalAccountsCost
					}, max: ${MAX_TX_PACK_SIZE}, est. CU used: expected ${
						runningCUUsed + cuToUsePerFill
					}, max: ${MAX_CU_PER_TX}`
				);
				break;
			}

			// add to tx
			logger.info(
				`including tx ${chUser
					.getUserAccountPublicKey()
					.toString()}-${nodeToFill.node.order.orderId.toString()}`
			);
			ixs.push(ix);
			runningTxSize += newIxCost + additionalAccountsCost;
			runningCUUsed += cuToUsePerFill;
			newAccounts.forEach((key) => uniqueAccounts.add(key.toString()));
			idxUsed++;
			nodesSent.push(nodeToFill);
		}

		logger.debug(`txPacker took ${Date.now() - txPackerStart}ms`);

		if (nodesSent.length === 0) {
			return [txSig, 0];
		}

		logger.info(
			`sending tx, ${
				uniqueAccounts.size
			} unique accounts, total ix: ${idxUsed}, calcd tx size: ${runningTxSize}, took ${
				Date.now() - txPackerStart
			}ms`
		);

		let txResp: Promise<TxSigAndSlot>;
		const txStart = Date.now();
		if (isNaN(this.transactionVersion)) {
			const tx = new Transaction();
			for (const ix of ixs) {
				tx.add(ix);
			}
			txResp = this.driftClient.txSender.send(tx, [], this.driftClient.opts);
		} else if (this.transactionVersion === 0) {
			txResp = this.driftClient.txSender.sendVersionedTransaction(
				ixs,
				[this.lookupTableAccount],
				[],
				this.driftClient.opts
			);
		} else {
			throw new Error(
				`unsupported transaction version ${this.transactionVersion}`
			);
		}
		txResp
			.then((resp: TxSigAndSlot) => {
				txSig = resp.txSig;
				const duration = Date.now() - txStart;
				logger.info(`sent tx: ${txSig}, took: ${duration}ms`);

				const user = this.driftClient.getUser();
				this.sdkCallDurationHistogram.record(duration, {
					...metricAttrFromUserAccount(
						user.getUserAccountPublicKey(),
						user.getUserAccount()
					),
					method: 'sendTx',
				});

				const parseLogsStart = Date.now();
				this.processBulkFillTxLogs(nodesSent, txSig)
					.then((successfulFills) => {
						const processBulkFillLogsDuration = Date.now() - parseLogsStart;
						logger.info(
							`parse logs took ${processBulkFillLogsDuration}ms, filled ${successfulFills}`
						);

						// record successful fills
						const user = this.driftClient.getUser();
						this.successfulFillsCounter.add(
							successfulFills,
							metricAttrFromUserAccount(
								user.userAccountPublicKey,
								user.getUserAccount()
							)
						);
					})
					.catch((e) => {
						console.error(e);
						logger.error(`Failed to process fill tx logs (error above):`);
						webhookMessage(
							`[${this.name}]: :x: error processing fill tx logs:\n${
								e.stack ? e.stack : e.message
							}`
						);
					});
			})
			.catch(async (e) => {
				console.error(e);
				logger.error(`Failed to send packed tx (error above):`);
				const simError = e as SendTransactionError;

				if (simError.logs && simError.logs.length > 0) {
					const start = Date.now();
					await this.handleTransactionLogs(nodesSent, simError.logs);
					logger.error(
						`Failed to send tx, sim error tx logs took: ${Date.now() - start}ms`
					);

					const errorCode = getErrorCode(e);

					if (
						!errorCodesToSuppress.includes(errorCode) &&
						!(e as Error).message.includes('Transaction was not confirmed')
					) {
						if (errorCode) {
							this.txSimErrorCounter.add(1, {
								errorCode: errorCode.toString(),
							});
						}
						webhookMessage(
							`[${this.name}]: :x: error simulating tx:\n${
								simError.logs ? simError.logs.join('\n') : ''
							}\n${e.stack || e}`
						);
					}
				}
			})
			.finally(() => {
				this.removeFillingNodes(nodesToFill);
			});

		return [txSig, nodesSent.length];
	}

	private async tryFill(orderRecord?: OrderRecord) {
		const startTime = Date.now();
		let ran = false;
		try {
			await tryAcquire(this.periodicTaskMutex).runExclusive(async () => {
				await this.dlobMutex.runExclusive(async () => {
					if (this.dlob) {
						this.dlob.clear();
						delete this.dlob;
					}
					this.dlob = new DLOB();
					await tryAcquire(this.userMapMutex).runExclusive(async () => {
						await this.dlob.initFromUserMap(
							this.userMap,
							this.slotSubscriber.getSlot()
						);
					});
					if (orderRecord) {
						this.dlob.insertOrder(
							orderRecord.order,
							orderRecord.user,
							this.slotSubscriber.getSlot()
						);
					}
				});

				await this.resyncUserMapsIfRequired();

				// 1) get all fillable nodes
				let fillableNodes: Array<NodeToFill> = [];
				for (const market of this.driftClient.getPerpMarketAccounts()) {
					try {
						fillableNodes = fillableNodes.concat(
							await this.getPerpFillableNodesForMarket(market)
						);
						logger.debug(
							`got ${fillableNodes.length} fillable nodes on market ${market.marketIndex}`
						);
					} catch (e) {
						console.error(e);
						webhookMessage(
							`[${this.name}]: :x: Failed to get fillable nodes for market ${
								market.marketIndex
							}:\n${e.stack ? e.stack : e.message}`
						);
						continue;
					}
				}

				// filter out nodes that we know cannot be filled
				const seenNodes = new Set<string>();
				const filteredNodes = fillableNodes.filter((node) => {
					const sig = this.getNodeToFillSignature(node);
					if (seenNodes.has(sig)) {
						return false;
					}
					seenNodes.add(sig);
					return this.filterFillableNodes(node);
				});
				logger.debug(
					`filtered ${fillableNodes.length} to ${filteredNodes.length}`
				);

				// fill the perp nodes
				let filledNodeCount = 0;
				while (filledNodeCount < filteredNodes.length) {
					const [_tx, attemptedFills] = await this.tryBulkFillPerpNodes(
						filteredNodes.slice(filledNodeCount)
					);
					filledNodeCount += attemptedFills;

					// record fill attempts
					const user = this.driftClient.getUser();
					this.attemptedFillsCounter.add(
						attemptedFills,
						metricAttrFromUserAccount(
							user.userAccountPublicKey,
							user.getUserAccount()
						)
					);
				}

				ran = true;
			});
		} catch (e) {
			if (e === E_ALREADY_LOCKED) {
				const user = this.driftClient.getUser();
				this.mutexBusyCounter.add(
					1,
					metricAttrFromUserAccount(
						user.getUserAccountPublicKey(),
						user.getUserAccount()
					)
				);
			} else if (e === dlobMutexError) {
				logger.error(`${this.name} dlobMutexError timeout`);
			} else {
				webhookMessage(
					`[${this.name}]: :x: uncaught error:\n${
						e.stack ? e.stack : e.message
					}`
				);
				throw e;
			}
		} finally {
			if (ran) {
				const duration = Date.now() - startTime;
				const user = this.driftClient.getUser();
				this.tryFillDurationHistogram.record(
					duration,
					metricAttrFromUserAccount(
						user.getUserAccountPublicKey(),
						user.getUserAccount()
					)
				);
				logger.debug(`tryFill done, took ${duration}ms`);

				await this.watchdogTimerMutex.runExclusive(async () => {
					this.watchdogTimerLastPatTime = Date.now();
				});
			}
		}
	}
}
