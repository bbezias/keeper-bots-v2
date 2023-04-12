import {
  calculateAskPrice,
  calculateBidPrice,
  BN,
  isVariant,
  DriftClient,
  PerpMarketAccount,
  SlotSubscriber,
  PositionDirection,
  OrderType,
  BASE_PRECISION,
  convertToNumber,
  PRICE_PRECISION,
  Order,
  PerpPosition,
  PerpMarkets, MarketType, DLOB, UserMap, UserStatsMap, OrderRecord, NewUserRecord, PostOnlyParams, QUOTE_PRECISION, TEN_THOUSAND, ZERO,
} from '@drift-labs/sdk';
import { Mutex, tryAcquire, E_ALREADY_LOCKED, withTimeout } from 'async-mutex';

import { logger } from '../logger';
import { Bot } from '../types';
import { RuntimeSpec, metricAttrFromUserAccount } from '../metrics';
import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import { Counter, Histogram, Meter, ObservableGauge } from '@opentelemetry/api';
import fs from 'fs';
import {
  ExplicitBucketHistogramAggregation,
  InstrumentType,
  MeterProvider,
  View,
} from '@opentelemetry/sdk-metrics-base';
import { FloatingMakerConfig } from '../config';
import { BatchObservableResult } from '@opentelemetry/api-metrics';
import { INDEX_TO_LETTERS, INDEX_TO_NAME } from './utils';

// State enum
enum StateType {
  /** Flat there is no open position */
  NEUTRAL = 'neutral',

  /** Long position on this market */
  LONG = 'long',

  /** Short position on market */
  SHORT = 'short',

  /** Current closing a long position (shorts only) */
  CLOSING_LONG = 'closing-long',

  /** Current closing a short position (long only) */
  CLOSING_SHORT = 'closing-short',
}

type State = {
  marketPosition: Map<number, PerpPosition>;
  openOrders: Map<number, Array<Order>>;
  stateType: Map<number, StateType>;
  longNotional: Map<number, number>;
  shortNotional: Map<number, number>;
};

type LevelParams = { [marketIndex: number]: { bid: number, ask: number, minSpread: number, amount: number } }
type Config = { maxExposure: number, timeoutDelay: number, levels: LevelParams };

const dlobMutexError = new Error('dlobMutex timeout');

const MARKET_UPDATE_COOLDOWN_SLOTS = 30; // wait slots before updating market position
const driftEnv = process.env.DRIFT_ENV || 'devnet';

enum METRIC_TYPES {
  sdk_call_duration_histogram = 'sdk_call_duration_histogram',
  try_make_duration_histogram = 'try_make_duration_histogram',
  runtime_specs = 'runtime_specs',
  mutex_busy = 'mutex_busy',
  errors = 'errors',
  unrealized_pnl = 'unrealized_pnl',
  unrealized_funding_pnl = 'unrealized_funding_pnl',
  levels = 'levels',
  max_exposure = 'max_exposure',
  initial_margin_requirement = 'initial_margin_requirement',
  maintenance_margin_requirement = 'maintenance_margin_requirement',
  total_collateral = 'total_collateral',
  free_collateral = 'free_collateral',
  perp_position_value = 'perp_position_value',
  perp_position_base = 'perp_position_base',
  perp_position_quote = 'perp_position_quote',
  total_leverage = 'total_leverage',
  total_asset_value = 'total_asset_value'
}

/**
 *
 * This bot is responsible for placing limit orders that rest on the DLOB.
 * limit price offsets are used to automatically shift the orders with the
 * oracle price, making order updating automatic.
 *
 */
export class FloatingPerpMakerBot implements Bot {
  public readonly name: string;
  public readonly dryRun: boolean;
  public readonly defaultIntervalMs: number = 5000;

  private driftClient: DriftClient;
  private slotSubscriber: SlotSubscriber;
  private periodicTaskMutex = new Mutex();
  private longMutex: { [marketId: number]: Mutex };
  private shortMutex: { [marketId: number]: Mutex };
  private lastSlotMarketUpdated: Map<number, number> = new Map();

  private intervalIds: Array<NodeJS.Timer> = [];

  private dlobMutex = withTimeout(
    new Mutex(),
    10 * this.defaultIntervalMs,
    dlobMutexError
  );
  private dlob: DLOB;
  private userMap: UserMap;
  private userStatsMap: UserStatsMap;
  // metrics
  private metricsInitialized = false;
  private metricsPort: number | undefined;
  private exporter: PrometheusExporter;
  private meter: Meter;
  private bootTimeMs = Date.now();
  private runtimeSpecsGauge: ObservableGauge;
  private totalCollateral: ObservableGauge;
  private freeCollateral: ObservableGauge;
  private initialMarginRequirement: ObservableGauge;
  private maintenanceMarginRequirement: ObservableGauge;
  private totalLeverage: ObservableGauge;
  private perpPositionValue: ObservableGauge;
  private totalAssetValueGauge: ObservableGauge;
  private perpPositionBase: ObservableGauge;
  private perpPositionQuote: ObservableGauge;
  private unrealizedPnL: ObservableGauge;
  private levelGauge: ObservableGauge;
  private maxExposureGauge: ObservableGauge;
  private unrealizedFundingPnL: ObservableGauge;
  private runtimeSpec: RuntimeSpec;
  private mutexBusyCounter: Counter;
  private errorCounter: Counter;
  private tryMakeDurationHistogram: Histogram;
  private levels: LevelParams;
  private timeoutDelay: number
  private lastPlaced: { [marketId: number]: number } = {};
  private needOrderUpdate: { [marketId: number]: boolean } = {};

  private agentState: State;
  private marketEnabled: number[] = [];

  /**
   * Set true to enforce max position size
   */
  private RESTRICT_POSITION_SIZE = true;

  /**
   * if a position's notional value passes this percentage of account
   * collateral, the position enters a CLOSING_* state.
   */
  private MAX_POSITION_EXPOSURE = 0.1;

  private watchdogTimerMutex = new Mutex();
  private watchdogTimerLastPatTime = Date.now();

  constructor(
    clearingHouse: DriftClient,
    slotSubscriber: SlotSubscriber,
    runtimeSpec: RuntimeSpec,
    config: FloatingMakerConfig
  ) {
    this.name = config.botId;
    this.dryRun = config.dryRun;
    this.driftClient = clearingHouse;
    this.slotSubscriber = slotSubscriber;
    this.longMutex = {};
    this.shortMutex = {};
    this.needOrderUpdate = {};
    for (const i of [0, 1, 2, 3, 4, 5, 6, 7]) {
      const x = INDEX_TO_LETTERS[i];
      const name = INDEX_TO_NAME[i];
      if (!config.marketEnabled[x]) continue;
      this.longMutex[i] = new Mutex();
      this.shortMutex[i] = new Mutex();
      this.needOrderUpdate[i] = true;
      this.marketEnabled.push(i);
      logger.info(`${name} enabled`);
    }

    fs.readFile('./config.json', (err, data) => {
      if (err) throw err;
      const params: Config = JSON.parse(data.toString());
      this.levels = params.levels;
      this.timeoutDelay = params.timeoutDelay;
      this.MAX_POSITION_EXPOSURE = params.maxExposure;
      console.log(this.levels);
    });

    this.metricsPort = config.metricsPort;
    if (this.metricsPort) {
      this.initializeMetrics();
    }
  }

  private initializeMetrics() {
    if (this.metricsInitialized) {
      logger.error('Tried to initialize metrics multiple times');
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
          instrumentName: METRIC_TYPES.try_make_duration_histogram,
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
    this.mutexBusyCounter = this.meter.createCounter(METRIC_TYPES.mutex_busy, {
      description: 'Count of times the mutex was busy',
    });
    this.errorCounter = this.meter.createCounter(METRIC_TYPES.errors, {
      description: 'Count of errors',
    });
    this.totalCollateral = this.meter.createObservableGauge(
      METRIC_TYPES.total_collateral,
      {
        description: 'Total collateral of the account',
      }
    );
    this.freeCollateral = this.meter.createObservableGauge(
      METRIC_TYPES.free_collateral,
      {
        description: 'Free collateral of the account',
      }
    );
    this.tryMakeDurationHistogram = this.meter.createHistogram(
      METRIC_TYPES.try_make_duration_histogram,
      {
        description: 'Distribution of tryTrigger',
        unit: 'ms',
      }
    );

    this.initialMarginRequirement = this.meter.createObservableGauge(
      METRIC_TYPES.initial_margin_requirement,
      {
        description: 'The account initial margin requirement',
      }
    );
    this.maintenanceMarginRequirement = this.meter.createObservableGauge(
      METRIC_TYPES.maintenance_margin_requirement,
      {
        description: 'The account maintenance margin requirement',
      }
    );
    this.unrealizedPnL = this.meter.createObservableGauge(
      METRIC_TYPES.unrealized_pnl,
      {
        description: 'The account unrealized PnL',
      }
    );
    this.unrealizedFundingPnL = this.meter.createObservableGauge(
      METRIC_TYPES.unrealized_funding_pnl,
      {
        description: 'The account unrealized funding PnL',
      }
    );
    this.levelGauge = this.meter.createObservableGauge(
      METRIC_TYPES.levels,
      {
        description: 'Level settings',
      }
    );
    this.maxExposureGauge = this.meter.createObservableGauge(
      METRIC_TYPES.max_exposure,
      {
        description: 'Max exposure settings',
      }
    );
    this.perpPositionValue = this.meter.createObservableGauge(
      METRIC_TYPES.perp_position_value,
      {
        description: 'Value of account perp positions',
      }
    );
    this.perpPositionBase = this.meter.createObservableGauge(
      METRIC_TYPES.perp_position_base,
      {
        description: 'Base asset value of account perp positions',
      }
    );
    this.totalAssetValueGauge = this.meter.createObservableGauge(
      METRIC_TYPES.total_asset_value,
      {
        description: 'Total Asset Value',
      }
    );
    this.perpPositionQuote = this.meter.createObservableGauge(
      METRIC_TYPES.perp_position_quote,
      {
        description: 'Quote asset value of account perp positions',
      }
    );

    this.totalLeverage = this.meter.createObservableGauge(
      METRIC_TYPES.total_leverage,
      {
        description: 'Total leverage of the account',
      }
    );

    this.meter.addBatchObservableCallback(
      async (batchObservableResult: BatchObservableResult) => {
        // each subaccount is responsible for a market
        // record account specific metrics
        for (const [idx, user] of this.driftClient.getUsers().entries()) {
          const accMarketIdx = idx;
          const userAccount = user.getUserAccount();
          const oracle =
            this.driftClient.getOracleDataForPerpMarket(accMarketIdx);
          const labels = metricAttrFromUserAccount(user.userAccountPublicKey, userAccount);

          batchObservableResult.observe(
            this.totalLeverage,
            convertToNumber(user.getLeverage(), TEN_THOUSAND),
            labels
          );
          batchObservableResult.observe(
            this.totalCollateral,
            convertToNumber(user.getTotalCollateral(), QUOTE_PRECISION),
            labels
          );
          batchObservableResult.observe(
            this.freeCollateral,
            convertToNumber(user.getFreeCollateral(), QUOTE_PRECISION),
            labels
          );

          batchObservableResult.observe(
            this.totalAssetValueGauge,
            convertToNumber(
              user.getTotalAssetValue(),
              QUOTE_PRECISION
            ),
            labels
          );

          batchObservableResult.observe(
            this.initialMarginRequirement,
            convertToNumber(
              user.getInitialMarginRequirement(),
              QUOTE_PRECISION
            ),
            labels
          );
          batchObservableResult.observe(
            this.maintenanceMarginRequirement,
            convertToNumber(
              user.getMaintenanceMarginRequirement(),
              QUOTE_PRECISION
            ),
            labels
          );

          batchObservableResult.observe(
            this.maxExposureGauge,
            this.MAX_POSITION_EXPOSURE,
            labels
          );

          for (const marketIndex of this.marketEnabled) {
            console.log('market index', marketIndex);
            const labelWithMarket = { ...labels };
            labelWithMarket.market = INDEX_TO_NAME[marketIndex];
            batchObservableResult.observe(
              this.perpPositionValue,
              convertToNumber(
                user.getPerpPositionValue(marketIndex, oracle),
                QUOTE_PRECISION
              ),
              labelWithMarket
            );

            const perpPosition = user.getPerpPosition(marketIndex);
            batchObservableResult.observe(
              this.perpPositionBase,
              convertToNumber(perpPosition.baseAssetAmount, BASE_PRECISION),
              labelWithMarket
            );
            batchObservableResult.observe(
              this.perpPositionQuote,
              convertToNumber(perpPosition.quoteAssetAmount, QUOTE_PRECISION),
              labelWithMarket
            );

            batchObservableResult.observe(
              this.unrealizedPnL,
              convertToNumber(user.getUnrealizedPNL(false, marketIndex), QUOTE_PRECISION),
              labelWithMarket
            );

            batchObservableResult.observe(
              this.unrealizedFundingPnL,
              convertToNumber(user.getUnrealizedFundingPNL(marketIndex), QUOTE_PRECISION),
              labelWithMarket
            );

            const bidLabels = { ...labelWithMarket };
            bidLabels.side = 'bid';
            batchObservableResult.observe(
              this.levelGauge,
              this.levels[marketIndex].bid,
              bidLabels
            );

            const askLabels = { ...labelWithMarket };
            askLabels.side = 'bid';
            batchObservableResult.observe(
              this.levelGauge,
              this.levels[marketIndex].bid,
              askLabels
            );

            const spreadLabels = { ...labelWithMarket };
            spreadLabels.side = 'spread';
            batchObservableResult.observe(
              this.levelGauge,
              this.levels[marketIndex].bid,
              spreadLabels
            );
          }
        }
      },
      [
        this.totalLeverage,
        this.totalCollateral,
        this.freeCollateral,
        this.perpPositionValue,
        this.perpPositionBase,
        this.perpPositionQuote,
        this.initialMarginRequirement,
        this.maintenanceMarginRequirement,
        this.unrealizedPnL,
        this.unrealizedFundingPnL,
        this.levelGauge,
        this.maxExposureGauge,
        this.totalAssetValueGauge
      ]
    );

  }

  public async init() {
    logger.info(`${this.name} initiating`);
    this.agentState = {
      marketPosition: new Map<number, PerpPosition>(),
      openOrders: new Map<number, Array<Order>>(),
      stateType: new Map<number, StateType>(),
      shortNotional: new Map<number, number>(),
      longNotional: new Map<number, number>(),
    };

    const initPromises: Array<Promise<any>> = [];

    this.userMap = new UserMap(
      this.driftClient,
      this.driftClient.userAccountSubscriptionConfig
    );
    initPromises.push(this.userMap.fetchAllUsers());

    this.userStatsMap = new UserStatsMap(
      this.driftClient,
      this.driftClient.userAccountSubscriptionConfig
    );
    initPromises.push(this.userStatsMap.fetchAllUserStats());

    this.dlob = new DLOB();
    initPromises.push(
      this.dlob.initFromUserMap(this.userMap, this.slotSubscriber.getSlot())
    );

    await Promise.all(initPromises);

    this.updateAgentState();
  }

  public async reset() {
    for (const intervalId of this.intervalIds) {
      clearInterval(intervalId);
    }
    this.intervalIds = [];
    if (this.dlob) {
      this.dlob.clear();
      delete this.dlob;
    }
    delete this.userMap;
    delete this.userStatsMap;
  }

  public async startIntervalLoop(intervalMs: number) {
    await this.updateOpenOrders();
    const intervalId = setInterval(
      this.updateOpenOrders.bind(this),
      intervalMs
    );
    this.intervalIds.push(intervalId);

    logger.info(`${this.name} Bot started!`);
  }

  public async healthCheck(): Promise<boolean> {
    let healthy = false;
    await this.watchdogTimerMutex.runExclusive(async () => {
      healthy =
        this.watchdogTimerLastPatTime > Date.now() - 2 * this.defaultIntervalMs;
    });
    return healthy;
  }

  public async trigger(record: any): Promise<void> {
    if (record.eventType === 'OrderRecord') {
      await this.userMap.updateWithOrderRecord(record as OrderRecord);
      await this.userStatsMap.updateWithOrderRecord(
        record as OrderRecord,
        this.userMap
      );
    } else if (record.eventType === 'NewUserRecord') {
      await this.userMap.mustGet((record as NewUserRecord).user.toString());
      await this.userStatsMap.mustGet(
        (record as NewUserRecord).user.toString()
      );
    }
  }

  public viewDlob(): undefined {
    return undefined;
  }

  /**
   * Updates the agent state based on its current market positions.
   *
   * We want to maintain a two-sided market while being conscious of the positions
   * taken on by the account.
   *
   * As open positions approach MAX_POSITION_EXPOSURE, limit orders are skewed such
   * that the position that decreases risk will be closer to the oracle price, and the
   * position that increases risk will be further from the oracle price.
   *
   * @returns {Promise<void>}
   */
  private updateAgentState(): void {

    const perpPositions: { [marketIndex: number]: PerpPosition } = {};

    for (const p of this.marketEnabled) {
      perpPositions[p] = undefined;
    }

    this.driftClient.getUserAccount().perpPositions.map((p) => {
      if (this.marketEnabled.includes(p.marketIndex)) {
        if (!perpPositions[p.marketIndex] || !p.baseAssetAmount.eq(ZERO)) {
          perpPositions[p.marketIndex] = p;
        }
      }
    });

    for (const marketIndex of this.marketEnabled) {

      const p = perpPositions[marketIndex];

      if (!p) continue;

      this.agentState.marketPosition.set(p.marketIndex, p);

      // update state
      let currentState = this.agentState.stateType.get(p.marketIndex);
      if (!currentState) {
        this.agentState.stateType.set(p.marketIndex, StateType.NEUTRAL);
        currentState = StateType.NEUTRAL;
      }

      // check if you need to enter a closing state
      const accountCollateral = convertToNumber(
        this.driftClient.getUser().getTotalCollateral(),
        QUOTE_PRECISION
      );
      const positionValue = convertToNumber(
        p.quoteAssetAmount,
        QUOTE_PRECISION
      );
      const exposure = Math.abs(positionValue / accountCollateral);

      const oracle = this.driftClient.getOracleDataForPerpMarket(marketIndex);
      const marketAccount = this.driftClient.getPerpMarketAccount(marketIndex);
      const vAsk = convertToNumber(calculateAskPrice(marketAccount, oracle), PRICE_PRECISION);
      const vBid = convertToNumber(calculateBidPrice(marketAccount, oracle), PRICE_PRECISION);
      const mid = (vBid + vAsk) / 2;

      const positionMax = this.levels[marketIndex].amount / mid;
      const baseAmount = convertToNumber(p.baseAssetAmount, BASE_PRECISION);

      let longNotional = Math.max(positionMax - baseAmount, 0);
      let shortNotional = Math.max(positionMax + baseAmount, 0);

      if (longNotional < 0.1 * positionMax) longNotional = 0;
      if (shortNotional < 0.1 * positionMax) shortNotional = 0;

      this.agentState.shortNotional.set(marketIndex, shortNotional);
      this.agentState.longNotional.set(marketIndex, longNotional);

      if (exposure >= this.MAX_POSITION_EXPOSURE) {
        // state becomes closing only
        if (p.baseAssetAmount.gt(new BN(0))) {
          this.agentState.stateType.set(
            p.marketIndex,
            StateType.CLOSING_LONG
          );
        } else {
          this.agentState.stateType.set(
            p.marketIndex,
            StateType.CLOSING_SHORT
          );
        }
      } else {
        // update state to be whatever our current position is
        if (p.baseAssetAmount.gt(new BN(0))) {
          this.agentState.stateType.set(p.marketIndex, StateType.LONG);
        } else if (p.baseAssetAmount.lt(new BN(0))) {
          this.agentState.stateType.set(p.marketIndex, StateType.SHORT);
        } else {
          this.agentState.stateType.set(p.marketIndex, StateType.NEUTRAL);
        }
      }
    }

    // zero out the open orders
    for (const market of PerpMarkets[driftEnv]) {
      this.agentState.openOrders.set(market.marketIndex, []);
    }

    this.driftClient.getUserAccount().orders.map((o) => {
      if (isVariant(o.status, 'init')) {
        return;
      }
      const marketIndex = o.marketIndex;
      this.agentState.openOrders.set(marketIndex, [
        ...this.agentState.openOrders.get(marketIndex),
        o,
      ]);
    });
  }

  private async updateOpenOrdersForMarket(marketAccount: PerpMarketAccount) {
    const currSlot = this.slotSubscriber.currentSlot;
    const marketIndex = marketAccount.marketIndex;
    const nextUpdateSlot =
      this.lastSlotMarketUpdated.get(marketIndex) +
      MARKET_UPDATE_COOLDOWN_SLOTS;

    if (nextUpdateSlot > currSlot) {
      return;
    }

    const name = INDEX_TO_NAME[marketIndex];

    const openOrders = this.agentState.openOrders.get(marketIndex);
    const oracle = this.driftClient.getOracleDataForPerpMarket(marketIndex);
    const vAsk = calculateAskPrice(marketAccount, oracle);
    const vBid = calculateBidPrice(marketAccount, oracle);

    const bestAsk = this.dlob.getBestAsk(marketIndex, vAsk, this.slotSubscriber.getSlot(), MarketType.PERP, oracle);
    const bestBid = this.dlob.getBestBid(marketIndex, vBid, this.slotSubscriber.getSlot(), MarketType.PERP, oracle);

    // cancel orders if not quoting both sides of the market
    const isOpenTradeTimedOut = !this.lastPlaced[marketIndex] || this.lastPlaced[marketIndex] < new Date().getTime() - this.timeoutDelay * 1000;

    let longAmountOpen = 0;
    let shortAmountOpen = 0;
    let longCountOpen = 0;
    let shortCountOpen = 0;

    const longNotional = this.agentState.longNotional.get(marketIndex);
    const shortNotional = this.agentState.shortNotional.get(marketIndex);

    const bestAskNumber = convertToNumber(bestAsk, PRICE_PRECISION);
    const bestBidNumber = convertToNumber(bestBid, PRICE_PRECISION);

    for (const o of openOrders) {
      const baseAmount = Math.abs(convertToNumber(o.baseAssetAmount, BASE_PRECISION));
      if ((isVariant(o.direction, 'long'))) {
        longCountOpen += 1;
        longAmountOpen = baseAmount;
      } else {
        shortCountOpen += 1;
        shortAmountOpen = baseAmount;
      }

      console.log(`${o.direction} ${name} ${convertToNumber(new BN(oracle.price.toNumber() + o.oraclePriceOffset), PRICE_PRECISION)} ${isVariant(o.direction, 'long') ? bestAskNumber : bestBidNumber}`);
    }

    let needCancel = false;
    if (longCountOpen > 1 || shortCountOpen > 1) {
      console.log(name, 'cancel due to too many open', longCountOpen, shortCountOpen);
      needCancel = true;
    } else if (Math.abs(longAmountOpen - longNotional) > 0.2 * longNotional) {
      console.log(name, 'cancel due to delta in long notional', longAmountOpen, longNotional);
      needCancel = true;
    } else if (Math.abs(shortAmountOpen - shortNotional) > 0.2 * shortNotional) {
      console.log(name, 'cancel due to delta in short notional', shortAmountOpen, shortNotional);
      needCancel = true;
    }

    if (openOrders.length > 0 && (needCancel || isOpenTradeTimedOut || this.needOrderUpdate[marketIndex])) {
      logger.info(`${name} Reason for cancelling OpenOrder/Timeout/config change ${needCancel} ${isOpenTradeTimedOut} ${this.needOrderUpdate[marketIndex]}`);
      // cancel orders
      for (const o of openOrders) {
        this.driftClient.cancelOrder(o.orderId).then(tx => console.log(
          `${this.name} cancelling order ${this.driftClient
          .getUserAccount()
          .authority.toBase58()}-${o.orderId}: ${tx}`
        )).catch(e => logger.error(`Order cancel error ${e}`));
      }
    }

    this.needOrderUpdate[marketIndex] = false;

    if (openOrders.length === 0) {
      // let biasNum = new BN(90);
      // let biasDenom = new BN(100);

      const bestBidNb = convertToNumber(bestBid, PRICE_PRECISION);
      const bestAskNb = convertToNumber(bestAsk, PRICE_PRECISION);
      const bestMid = (bestBidNb + bestAskNb) / 2;
      const oraclePrice = convertToNumber(oracle.price, PRICE_PRECISION);
      this.lastPlaced[marketIndex] = new Date().getTime();

      let bidWanted = bestBidNb - this.levels[marketIndex].bid;
      let askWanted = bestAskNb + this.levels[marketIndex].ask;

      console.log("Bid/Ask wanted", bidWanted, askWanted);

      if (askWanted - bidWanted < this.levels[marketIndex].minSpread) {
        bidWanted = bestMid - this.levels[marketIndex].minSpread / 2;
        askWanted = bestMid + this.levels[marketIndex].minSpread / 2;
        console.log("Bid/Ask spread too small so we change ->", bidWanted, askWanted);
      }

      const bidOffset = new BN((bidWanted - oraclePrice) * PRICE_PRECISION.toNumber()).toNumber();
      const askOffset = new BN((askWanted - oraclePrice) * PRICE_PRECISION.toNumber()).toNumber();

      if (longNotional && longNotional > 0) {
        const release = await this.longMutex[marketIndex].acquire();
        // const oracleBidSpread = oracle.price.sub(bestBid);
        console.log(marketIndex, longNotional);
        this.driftClient.placePerpOrder({
          marketIndex: marketIndex,
          orderType: OrderType.LIMIT,
          direction: PositionDirection.LONG,
          baseAssetAmount: new BN(BASE_PRECISION.toNumber() * longNotional),
          postOnly: PostOnlyParams.MUST_POST_ONLY,
          oraclePriceOffset: bidOffset, // limit bid below oracle,
        }).then(tx0 => console.log(`${this.name} placing long: ${tx0}`)).catch(
          e => logger.error(`Error on place long order: ${e}`)
        ).finally(() => {
          release();
        });
      }

      if (shortNotional && shortNotional > 0) {
        // const oracleAskSpread = bestAsk.sub(oracle.price);
        const release = await this.shortMutex[marketIndex].acquire();
        console.log(marketIndex, shortNotional);
        this.driftClient.placePerpOrder({
          marketIndex: marketIndex,
          orderType: OrderType.LIMIT,
          direction: PositionDirection.SHORT,
          baseAssetAmount: new BN(BASE_PRECISION.toNumber() * shortNotional),
          postOnly: PostOnlyParams.MUST_POST_ONLY,
          oraclePriceOffset: askOffset, // limit ask above oracle
        }).then(tx1 => console.log(`${this.name} placing short: ${tx1}`)).catch(
          e => logger.error(`Error on place long order: ${e}`)
        ).finally(() => {
          release();
        });
      }
    }

    // enforce cooldown on market
    this.lastSlotMarketUpdated.set(marketIndex, currSlot);
  }

  public updateParams(config: Config) {

    fs.writeFile('./config.json', JSON.stringify(config), (err) => {
      if (err) throw err;
      console.log('The file has been saved!');
    });

    this.levels = config.levels;
    this.MAX_POSITION_EXPOSURE = config.maxExposure;
    for (const marketIndex of this.marketEnabled) {
      this.needOrderUpdate[marketIndex] = true;
    }

    console.log('Config updated');
    console.log(this.levels);
    console.log(this.MAX_POSITION_EXPOSURE);
  }

  private async updateOpenOrders() {
    const start = Date.now();
    let ran = false;
    try {
      await tryAcquire(this.periodicTaskMutex).runExclusive(async () => {
        await this.dlobMutex.runExclusive(async () => {
          if (this.dlob) {
            this.dlob.clear();
            delete this.dlob;
          }
          this.dlob = new DLOB();
          await this.dlob.initFromUserMap(
            this.userMap,
            this.slotSubscriber.getSlot()
          );
        });

        this.updateAgentState();
        await Promise.all(
          this.driftClient.getPerpMarketAccounts().map((marketAccount) => {

            if (!this.marketEnabled.includes(marketAccount.marketIndex)) return;

            if (this.longMutex[marketAccount.marketIndex].isLocked() || this.shortMutex[marketAccount.marketIndex].isLocked()) return;

            console.log(
              `${this.name} updating open orders for market ${marketAccount.marketIndex}`
            );
            this.updateOpenOrdersForMarket(marketAccount);
          })
        );

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
        throw e;
      }
    } finally {
      if (ran) {
        const duration = Date.now() - start;
        const user = this.driftClient.getUser();
        this.tryMakeDurationHistogram.record(
          duration,
          metricAttrFromUserAccount(
            user.getUserAccountPublicKey(),
            user.getUserAccount()
          )
        );
        logger.debug(`${this.name} Bot took ${Date.now() - start}ms to run`);

        await this.watchdogTimerMutex.runExclusive(async () => {
          this.watchdogTimerLastPatTime = Date.now();
        });
      }
    }
  }
}
