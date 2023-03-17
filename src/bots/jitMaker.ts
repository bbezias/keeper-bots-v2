import {
  BN,
  isVariant,
  DriftClient,
  PerpMarketAccount,
  SlotSubscriber,
  PositionDirection,
  OrderType,
  OrderRecord,
  NewUserRecord,
  BASE_PRECISION,
  QUOTE_PRECISION,
  convertToNumber,
  PRICE_PRECISION,
  PerpPosition,
  SpotPosition,
  DLOB,
  DLOBNode,
  UserMap,
  UserStatsMap,
  getOrderSignature,
  MarketType, PostOnlyParams, calculateBidAskPrice,
} from '@drift-labs/sdk';
import { Mutex, tryAcquire, withTimeout, E_ALREADY_LOCKED } from 'async-mutex';

import { TransactionSignature, PublicKey } from '@solana/web3.js';

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { Bot } from '../types';

import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import { Counter, Histogram, Meter, ObservableGauge } from '@opentelemetry/api';
import {
  ExplicitBucketHistogramAggregation,
  InstrumentType,
  MeterProvider,
  View,
} from '@opentelemetry/sdk-metrics-base';
import { RuntimeSpec, metricAttrFromUserAccount } from '../metrics';
import { JitMakerConfig } from '../config';
import { KucoinController } from '../kucoin-api/kucoin';
import { Order, OrderBook } from '../kucoin-api/models';
import { PositionChangeOperationResponse } from '../kucoin-api/ws';
import { Contract } from '../kucoin-api/market';

type Action = {
  baseAssetAmount: BN;
  marketIndex: number;
  direction: PositionDirection;
  price: BN;
  node: DLOBNode;
};

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
  stateType: Map<number, StateType>;
  spotMarketPosition: Map<number, SpotPosition>;
  perpMarketPosition: Map<number, PerpPosition>;
};

const dlobMutexError = new Error('dlobMutex timeout');

enum METRIC_TYPES {
  sdk_call_duration_histogram = 'sdk_call_duration_histogram',
  try_jit_duration_histogram = 'try_jit_duration_histogram',
  runtime_specs = 'runtime_specs',
  mutex_busy = 'mutex_busy',
  errors = 'errors',
}

enum KUCOIN_CONTRACTS {
  sol = 'SOLUSDTM',
  btc = 'XBTUSDTM',
  eth = 'ETHUSDTM',
  apt = 'APTUSDTM',
  matic = 'MATICUSDTM'
}

const INDEX_TO_NAME = {
  0: 'Solana',
  1: 'Bitcoin',
  2: 'Ethereum',
  3: 'Aptos',
  5: 'Polygon'
};

const SYMBOL_TO_INDEX = {
  SOLUSDTM: 0,
  XBTUSDTM: 1,
  ETHUSDTM: 2,
  APTUSDTM: 3,
  MATICUSDTM: 5
};

const INDEX_TO_SYMBOL = {
  0: KUCOIN_CONTRACTS.sol,
  1: KUCOIN_CONTRACTS.btc,
  2: KUCOIN_CONTRACTS.eth,
  3: KUCOIN_CONTRACTS.apt,
  5: KUCOIN_CONTRACTS.matic
};

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

class MaxSizeList {
  private readonly maxSize: number;
  private readonly list: number[];

  public getAverage(): number {
    if (this.list.length === 0) {
      return 0;
    }
    const sum = this.list.reduce((accumulator, currentValue) => accumulator + currentValue);
    return sum / this.list.length;
  }

  constructor(maxSize: number) {
    this.maxSize = maxSize;
    this.list = [];
  }

  public add(item: number) {
    if (this.list.length === this.maxSize) {
      this.list.shift(); // Remove first element
    }
    this.list.push(item);
  }

  public getItems() {
    return this.list;
  }
}


/**
 *
 * This bot is responsible for placing small trades during an order's JIT auction
 * in order to partially fill orders and collect maker fees. The bot also tracks
 * its position on all available markets in order to limit the size of open positions.
 *
 */
export class JitMakerBot implements Bot {
  public readonly name: string;
  public readonly dryRun: boolean;
  public readonly defaultIntervalMs: number = 1000;

  private driftClient: DriftClient;
  private slotSubscriber: SlotSubscriber;
  private dlobMutex = withTimeout(
    new Mutex(),
    10 * this.defaultIntervalMs,
    dlobMutexError
  );
  private dlob: DLOB;
  private periodicTaskMutex = new Mutex();
  private userMap: UserMap;
  private userStatsMap: UserStatsMap;
  private orderLastSeenBaseAmount: Map<string, BN> = new Map(); // need some way to trim this down over time

  private intervalIds: Array<NodeJS.Timer> = [];

  private agentState: State;

  // metrics
  private metricsInitialized = false;
  private metricsPort: number | undefined;
  private exporter: PrometheusExporter;
  private meter: Meter;
  private bootTimeMs = Date.now();
  private runtimeSpecsGauge: ObservableGauge;
  private runtimeSpec: RuntimeSpec;
  private mutexBusyCounter: Counter;
  private errorCounter: Counter;
  private tryJitDurationHistogram: Histogram;

  private watchdogTimerMutex = new Mutex();
  private watchdogTimerLastPatTime = Date.now();

  /**
   * Set true to enforce max position size
   */
  private RESTRICT_POSITION_SIZE = true;

  /**
   * if a position's notional value passes this percentage of account
   * collateral, the position enters a CLOSING_* state.
   */
  private readonly MAX_POSITION_EXPOSURE;

  private kucoin: KucoinController;
  private exchangeDelta: { [id: number]: number } = {
    0: 0,
    1: 0,
    2: 0,
    3: 0,
    5: 0
  };
  private readonly profitThreshold: number;
  private readonly profitThresholdIfReduce: number;
  private readonly kucoinTakerFee: number;

  private bookKucoin: { [id: number]: OrderBook } = {};
  private positionKucoin: { [id: number]: number } = {};
  private maxTradeSize: { [id: number]: number } = {
    0: 0,
    1: 0,
    2: 0,
    3: 0,
    5: 0
  };
  private maxTradeSizeInLot: { [id: number]: number } = {
    0: 0,
    1: 0,
    2: 0,
    3: 0,
    5: 0
  };
  private futureKucoinContract: { [id: number]: Contract } = {};
  private coolingPeriod: { [id: number]: number } = {
    0: 0,
    1: 0,
    2: 0,
    3: 0,
    5: 0
  };

  private deltaKucoinDrift: { [id: number]: MaxSizeList } = {
    0: new MaxSizeList(20),
    1: new MaxSizeList(20),
    2: new MaxSizeList(20),
    3: new MaxSizeList(20),
    5: new MaxSizeList(20)
  };

  private exchangeDeltaTime: { [id: number]: number } = {
    0: 0,
    1: 0,
    2: 0,
    3: 0,
    5: 0
  };

  private isActive = false;

  constructor(
    clearingHouse: DriftClient,
    slotSubscriber: SlotSubscriber,
    runtimeSpec: RuntimeSpec,
    config: JitMakerConfig
  ) {
    this.name = config.botId;
    this.dryRun = config.dryRun;
    this.driftClient = clearingHouse;
    this.slotSubscriber = slotSubscriber;
    this.runtimeSpec = runtimeSpec;

    this.kucoinTakerFee = config.kucoinTakerFee;
    this.profitThreshold = config.profitThreshold;
    this.profitThresholdIfReduce = config.profitThresholdIfReduce;
    this.MAX_POSITION_EXPOSURE = config.maxPositionExposure;
    this.maxTradeSize[0] = config.maxTradeSize.sol;
    this.maxTradeSize[1] = config.maxTradeSize.btc;
    this.maxTradeSize[2] = config.maxTradeSize.eth;
    this.maxTradeSize[3] = config.maxTradeSize.apt;
    this.maxTradeSize[5] = config.maxTradeSize.matic;

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
          instrumentName: METRIC_TYPES.try_jit_duration_histogram,
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
        description: 'Runtime specification of this program',
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
    this.tryJitDurationHistogram = this.meter.createHistogram(
      METRIC_TYPES.try_jit_duration_histogram,
      {
        description: 'Distribution of tryTrigger',
        unit: 'ms',
      }
    );
  }

  private async refreshDelta(): Promise<void> {

    Object.entries(this.bookKucoin).forEach(([key, book]) => {
      const now = (new Date()).getTime();
      const marketIndex: number = parseInt(key);
      if (now <= this.exchangeDeltaTime[marketIndex] && this.exchangeDelta[marketIndex] !== 0) return;
      if (!book) {
        this.exchangeDelta[marketIndex] = 0;
        this.exchangeDeltaTime[marketIndex] = 0;
        logger.error(`Issue getting delta for ${marketIndex} - missing kucoin`);
      }
      const kucoinMid = (book._asks[0][0] + book._bids[0][0]) / 2;
      const [bid, ask] = calculateBidAskPrice(
        this.driftClient.getPerpMarketAccount(marketIndex).amm,
        this.driftClient.getOracleDataForPerpMarket(marketIndex)
      );
      const formattedBidPrice = convertToNumber(bid, PRICE_PRECISION);
      const formattedAskPrice = convertToNumber(ask, PRICE_PRECISION);
      if (!formattedAskPrice || !formattedBidPrice) {
        this.exchangeDelta[marketIndex] = 0;
        this.exchangeDeltaTime[marketIndex] = 0;
        logger.error(`Issue getting delta for ${marketIndex} - missing drift`);
      }

      const driftMid = (formattedAskPrice + formattedBidPrice) / 2;
      this.deltaKucoinDrift[marketIndex].add(kucoinMid / driftMid);
      this.exchangeDelta[marketIndex] = this.deltaKucoinDrift[marketIndex].getAverage();
      this.exchangeDeltaTime[marketIndex] = now + 30000;
      const name = INDEX_TO_NAME[marketIndex];
      logger.debug(`Ratio update for ${name}: ${this.exchangeDelta[marketIndex]}`);
    });
  }

  private async getKucoinPositionAndLog(symbol: string, index: number): Promise<boolean> {
    try {
      const p = await this.kucoin.api.position({ symbol });
      this.positionKucoin[index] = p.data.currentQty;
      logger.info(`Initial ${symbol} Position: ${this.positionKucoin[index] * this.futureKucoinContract[index].multiplier}`);
      return true;
    } catch (error) {
      console.error(`Error fetching position for ${symbol}`);
      return false;
    }
  }

  private async retryGetKucoinPositionAndLog(symbol: string, index: number): Promise<void> {
    let success = false;
    let retries = 0;
    const maxRetries = 5;

    while (!success && retries < maxRetries) {
      success = await this.getKucoinPositionAndLog(symbol, index);

      if (!success) {
        retries++;
        if (retries < maxRetries) {
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
      }
    }

    if (!success) {
      throw new Error(`Failed to get ${symbol} position after ${maxRetries} retries.`);
    }
  }

  private async initKucoin(): Promise<void> {
    logger.info("Kucoin initiating");
    this.kucoin = new KucoinController();
    await this.kucoin.initialise();
    this.kucoin.subscribe(KUCOIN_CONTRACTS.sol).then(() => {
      logger.info(`✅ Websocket SOLUSDTM subscribed with kucoin`);
    });
    this.kucoin.subscribe(KUCOIN_CONTRACTS.btc).then(() => {
      logger.info(`✅ Websocket BTCUSDTM subscribed with kucoin`);
    });

    this.kucoin.subscribe(KUCOIN_CONTRACTS.eth).then(() => {
      logger.info(`✅ Websocket ETHUSDTM subscribed with kucoin`);
    });

    this.kucoin.subscribe(KUCOIN_CONTRACTS.apt).then(() => {
      logger.info(`✅ Websocket APTUSDTM subscribed with kucoin`);
    });

    this.kucoin.subscribe(KUCOIN_CONTRACTS.matic).then(() => {
      logger.info(`✅ Websocket MATICUSDTM subscribed with kucoin`);
    });

    // Confirm balance
    const resp = await this.kucoin.api.accountOverview({ currency: 'USDT' });
    logger.info(`Kucoin account Balance + unrealised Pnl: ${resp.data.accountEquity}`);

    // Lot size
    const contractList = await this.kucoin.api.contractList();
    for (const x of contractList.data) {

      if (x.symbol === KUCOIN_CONTRACTS.sol || x.symbol === KUCOIN_CONTRACTS.btc || x.symbol === KUCOIN_CONTRACTS.eth || x.symbol === KUCOIN_CONTRACTS.apt || x.symbol === KUCOIN_CONTRACTS.matic) {
        const marketIndex = SYMBOL_TO_INDEX[x.symbol];
        logger.info(`Multiplier for ${x.symbol}: ${x.multiplier}`);
        this.futureKucoinContract[marketIndex] = x;
        this.maxTradeSizeInLot[marketIndex] = Math.trunc(this.maxTradeSize[marketIndex] / x.multiplier);
        if (this.maxTradeSizeInLot[marketIndex] < 1) {
          throw Error(`The max trade size in lot is smaller than one: ${this.maxTradeSizeInLot[marketIndex]}, maxSize: ${this.maxTradeSize[marketIndex]} mult: ${x.multiplier}`);
        }
      }
    }

    await this.retryGetKucoinPositionAndLog(KUCOIN_CONTRACTS.sol, 0);
    await sleep(1000);
    await this.retryGetKucoinPositionAndLog(KUCOIN_CONTRACTS.btc, 1);
    await sleep(1000);
    await this.retryGetKucoinPositionAndLog(KUCOIN_CONTRACTS.eth, 2);
    await sleep(1000);
    await this.retryGetKucoinPositionAndLog(KUCOIN_CONTRACTS.apt, 3);
    await sleep(1000);
    await this.retryGetKucoinPositionAndLog(KUCOIN_CONTRACTS.matic, 5);

    this.kucoin.on('book', r => {
      const marketIndex = SYMBOL_TO_INDEX[r.data.symbol];
      this.bookKucoin[marketIndex] = r.data;
    });

    this.kucoin.on('positionOperationChange', (r: PositionChangeOperationResponse) => {

      const marketIndex = SYMBOL_TO_INDEX[r.symbol];
      if (marketIndex === undefined) return;
      this.positionKucoin[marketIndex] = r.currentQty;
      logger.info(`currentQty ${r.symbol}: ${r.currentQty * this.futureKucoinContract[marketIndex].multiplier}`);
    });

    this.kucoin.on('order', (r: Order) => {
      logger.debug(r);
    });
  }

  public async init(): Promise<void> {

    const intervalId = setInterval(this.refreshDelta.bind(this), 2000);
    this.intervalIds.push(intervalId);

    logger.info(`${this.name} initiating`);
    const initPromises: Array<Promise<any>> = [];

    initPromises.push(this.initKucoin());

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

    this.agentState = {
      stateType: new Map<number, StateType>(),
      spotMarketPosition: new Map<number, SpotPosition>(),
      perpMarketPosition: new Map<number, PerpPosition>(),
    };
    initPromises.push(this.updateAgentState());

    await Promise.all(initPromises);
    logger.info(`All init done`);
    this.isActive = true;

    await this.userMap.fetchAllUsers();
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
    await this.tryMake();
    const intervalId = setInterval(this.tryMake.bind(this), intervalMs);
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
      await this.tryMake();
    } else if (record.eventType === 'NewUserRecord') {
      await this.userMap.mustGet((record as NewUserRecord).user.toString());
      await this.userStatsMap.mustGet(
        (record as NewUserRecord).user.toString()
      );
    }
  }

  public viewDlob(): DLOB {
    return this.dlob;
  }

  /**
   * This function creates a distribution of the values in array based on the
   * weights array. The returned array should be used in randomIndex to make
   * a random draw from the distribution.
   *
   */
  private createDistribution(
    array: Array<any>,
    weights: Array<number>,
    size: number
  ): Array<number> {
    const distribution = [];
    const sum = weights.reduce((a: number, b: number) => a + b);
    const quant = size / sum;
    for (let i = 0; i < array.length; ++i) {
      const limit = quant * weights[i];
      for (let j = 0; j < limit; ++j) {
        distribution.push(i);
      }
    }
    return distribution;
  }

  /**
   * Make a random choice from distribution
   * @param distribution array of values that can be drawn from
   * @returns
   */
  private randomIndex(distribution: Array<number>): number {
    const index = Math.floor(distribution.length * Math.random()); // random index
    return distribution[index];
  }

  /**
   * Generates a random number between [min, max]
   * @param min minimum value to generate random number from
   * @param max maximum value to generate random number from
   * @returns the random number
   */
  private randomIntFromInterval(min: number, max: number): number {
    return Math.floor(Math.random() * (max - min + 1) + min);
  }

  /**
   * Updates the agent state based on its current market positions.
   *
   * Our goal is to participate in JIT auctions while limiting the delta
   * exposure of the bot.
   *
   * We achieve this by allowing deltas to increase until MAX_POSITION_EXPOSURE
   * is hit, after which orders will only reduce risk until the position is
   * closed.
   *
   * @returns {Promise<void>}
   */
  private async updateAgentState(): Promise<void> {
    try {

      const x = this.driftClient.getUserAccount().perpPositions;
      const positionOnDrift: { [id: number]: number } = { 0: 0, 1: 0, 2: 0, 3: 0, 5: 0 };

      for await (const p of x) {

        if (this.isActive && (p.marketIndex === 0 || p.marketIndex === 1 || p.marketIndex === 2 || p.marketIndex === 3 || p.marketIndex === 5)) {
          const baseValue = convertToNumber(p.baseAssetAmount, BASE_PRECISION);
          const multiplier = this.futureKucoinContract[p.marketIndex].multiplier;
          positionOnDrift[p.marketIndex] += Math.round(baseValue / multiplier);
        }

        if (p.baseAssetAmount.isZero()) {
          continue;
        }

        // update current position based on market position
        this.agentState.perpMarketPosition.set(p.marketIndex, p);

        // update state
        let currentState = this.agentState.stateType.get(p.marketIndex);
        if (!currentState) {
          this.agentState.stateType.set(p.marketIndex, StateType.NEUTRAL);
          currentState = StateType.NEUTRAL;
        }

        let canUpdateStateBasedOnPosition = true;
        if (
          (currentState === StateType.CLOSING_LONG &&
            p.baseAssetAmount.gt(new BN(0))) ||
          (currentState === StateType.CLOSING_SHORT &&
            p.baseAssetAmount.lt(new BN(0)))
        ) {
          canUpdateStateBasedOnPosition = false;
        }

        if (canUpdateStateBasedOnPosition) {
          // check if need to enter a closing state
          const accountCollateral = convertToNumber(
            this.driftClient.getUser().getTotalCollateral(),
            QUOTE_PRECISION
          );
          const positionValue = convertToNumber(
            p.quoteAssetAmount,
            QUOTE_PRECISION
          );

          const exposure = positionValue / accountCollateral;

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
      }

      if (this.isActive) {

        const allowedMarketIndices = [0, 1, 2, 3, 5];
        for (const i of allowedMarketIndices) {
          const driftPosition = positionOnDrift[i];
          const kucoinPosition = this.positionKucoin[i];
          const symbol = INDEX_TO_SYMBOL[i];
          const delta = driftPosition + kucoinPosition;
          const now = (new Date()).getTime();
          if (delta !== 0 && this.coolingPeriod[i] < now) {
            const clientOid = `${symbol}-${(new Date()).getTime()}`;
            const side = delta > 0 ? 'sell' : 'buy';
            this.coolingPeriod[i] = now + 5000;
            this.kucoin.api.placeMarketOrder({
              symbol,
              size: Math.abs(delta),
              side: side,
              clientOid,
              leverage: 10
            }).then(r => {
              if (r.code === "200000") {
                logger.info(`✅ Hedge placed successfully ${r.data.orderId}`);
              } else {
                logger.error("Error hedging the position");
                console.log(r);
              }
            }).catch(e => {
              logger.error(`Error buying hedge on kucoin: ${e}`);
            });
            logger.info(`Hedging ${symbol} Open position, taking order ${side} ${Math.abs(delta)}`);
          }
        }
      }

    } catch (e) {
      logger.error(`Uncaught error in Update agent state`);
      console.log(e);
    }
  }

  private nodeCanBeFilled(
    node: DLOBNode,
    userAccountPublicKey: PublicKey
  ): boolean {
    if (node.haveFilled) {
      logger.error(
        `already made the JIT auction for ${node.userAccount} - ${node.order.orderId}`
      );
      return false;
    }

    // jitter can't fill its own orders
    if (node.userAccount.equals(userAccountPublicKey)) {
      return false;
    }

    const orderSignature = getOrderSignature(
      node.order.orderId,
      node.userAccount
    );
    const lastBaseAmountFilledSeen =
      this.orderLastSeenBaseAmount.get(orderSignature);
    return !lastBaseAmountFilledSeen?.eq(node.order.baseAssetAmountFilled);


  }

  /**
   *
   */
  private determineJitAuctionBaseFillAmount(
    orderBaseAmountAvailable: BN,
    maxSizeInLot: number,
    kucoinLotSize: number
  ): BN {

    const amountAvailable = convertToNumber(orderBaseAmountAvailable, BASE_PRECISION);

    if (amountAvailable < kucoinLotSize) return new BN(0);

    const amountAvailableInLot = Math.trunc(amountAvailable / kucoinLotSize);
    const amountToTakeInLot = Math.min(maxSizeInLot, amountAvailableInLot);
    const baseFillAmountNumber = amountToTakeInLot * kucoinLotSize;
    return new BN(
      baseFillAmountNumber * BASE_PRECISION.toNumber()
    );
  }

  /**
   * Draws an action based on the current state of the bot.
   *
   */
  private async drawAndExecuteAction(market: PerpMarketAccount) {

    try {
      // get nodes available to fill in the jit auction
      const nodesToFill = this.dlob.findJitAuctionNodesToFill(
        market.marketIndex,
        this.slotSubscriber.getSlot(),
        this.driftClient.getOracleDataForPerpMarket(market.marketIndex),
        MarketType.PERP
      );


      for (const nodeToFill of nodesToFill) {
        if (
          !this.nodeCanBeFilled(
            nodeToFill.node,
            await this.driftClient.getUserAccountPublicKey()
          )
        ) {
          continue;
        }

        // calculate jit maker order params
        const orderDirection = nodeToFill.node.order.direction;
        const jitMakerDirection = isVariant(orderDirection, 'long')
          ? PositionDirection.SHORT
          : PositionDirection.LONG;

        const startPrice = convertToNumber(
          nodeToFill.node.order.auctionStartPrice,
          PRICE_PRECISION
        );
        const endPrice = convertToNumber(
          nodeToFill.node.order.auctionEndPrice,
          PRICE_PRECISION
        );

        const kucoinBook = this.bookKucoin[market.marketIndex];

        if (!kucoinBook) {
          logger.error(
            `Kucoin price not found for market ${market.marketIndex}`
          );
          continue;
        } else if (kucoinBook.datetime.getTime() < new Date().getTime() - 10000) {
          logger.error(
            `Kucoin price not updated for market ${market.marketIndex} since ${(new Date().getTime() - kucoinBook.datetime.getTime()) / 1000} seconds`
          );
          continue;
        }

        const jitMakerBaseAssetAmount = this.determineJitAuctionBaseFillAmount(
          nodeToFill.node.order.baseAssetAmount.sub(
            nodeToFill.node.order.baseAssetAmountFilled
          ),
          this.maxTradeSizeInLot[market.marketIndex],
          this.futureKucoinContract[market.marketIndex].multiplier
        );

        const amount = convertToNumber(jitMakerBaseAssetAmount, BASE_PRECISION);
        if (amount === 0) {
          continue;
        }
        const amountToFill = convertToNumber(nodeToFill.node.order.baseAssetAmount, BASE_PRECISION);
        const amountFilled = convertToNumber(nodeToFill.node.order.baseAssetAmountFilled, BASE_PRECISION);

        const bestAsk = kucoinBook.bestAsk(amount);
        const bestBid = kucoinBook.bestBid(amount);

        if (this.exchangeDelta[market.marketIndex] === 0) {
          continue;
        }

        const factor = this.exchangeDelta[market.marketIndex];

        const bestAskUsdc = bestAsk / factor;
        const bestBidUsdc = bestBid / factor;

        if (endPrice < 0 || startPrice < 0) continue;
        if (bestAskUsdc * 2 < endPrice || bestBidUsdc / 2 > endPrice) continue;

        logger.info(
          `node slot: ${
            nodeToFill.node.order.slot
          }, cur slot: ${this.slotSubscriber.getSlot()}`
        );
        this.orderLastSeenBaseAmount.set(
          getOrderSignature(
            nodeToFill.node.order.orderId,
            nodeToFill.node.userAccount
          ),
          nodeToFill.node.order.baseAssetAmountFilled
        );

        logger.info(
          `${
            this.name
          } quoting order for node: ${nodeToFill.node.userAccount.toBase58()} - ${nodeToFill.node.order.orderId.toString()}, orderBaseFilled: ${convertToNumber(
            nodeToFill.node.order.baseAssetAmountFilled,
            BASE_PRECISION
          )}/${convertToNumber(
            nodeToFill.node.order.baseAssetAmount,
            BASE_PRECISION
          )}`
        );

        const orderSlot = nodeToFill.node.order.slot.toNumber();
        const currSlot = this.slotSubscriber.getSlot();
        const aucDur = nodeToFill.node.order.auctionDuration;
        const aucEnd = orderSlot + aucDur;
        const isTestingAccount = nodeToFill.node.userAccount.toBase58() === 'kmLtQsL8cJqmopn2j8GWRHJg9jeQuzdRAf7BJEJ3kDX';
        let pt = this.profitThreshold;
        let takePosition = false;
        let offeredPrice = 0;
        let virtualPnLRel = 0;
        let positionOnDrift = '';
        let bestKucoinValue;
        let acceptablePrice = 0;
        const currentState = this.agentState.stateType.get(market.marketIndex);
        if (jitMakerDirection === PositionDirection.LONG) {
          if (currentState === StateType.CLOSING_SHORT) pt = this.profitThresholdIfReduce;
          acceptablePrice = bestAskUsdc / (1 + this.kucoinTakerFee + pt);
          offeredPrice = Math.min(startPrice, acceptablePrice);
          virtualPnLRel = bestAskUsdc / offeredPrice - 1 - this.kucoinTakerFee;
          bestKucoinValue = bestAskUsdc;
          positionOnDrift = 'long';
          if (acceptablePrice >= endPrice) {
            takePosition = true;
          } else if (isTestingAccount) {
            takePosition = true;
            offeredPrice = startPrice;
          } else {
            virtualPnLRel = bestAskUsdc / endPrice - 1 - this.kucoinTakerFee;
          }
        } else {
          if (currentState === StateType.CLOSING_LONG) pt = this.profitThresholdIfReduce;
          acceptablePrice = (1 + this.kucoinTakerFee + pt) * bestBidUsdc;
          offeredPrice = Math.max(startPrice, acceptablePrice);
          bestKucoinValue = bestBidUsdc;
          virtualPnLRel = offeredPrice / bestBidUsdc - 1 - this.kucoinTakerFee;
          positionOnDrift = 'short';
          if (acceptablePrice <= endPrice) {
            takePosition = true;
          } else if (isTestingAccount) {
            takePosition = true;
            offeredPrice = startPrice;
          } else {
            virtualPnLRel = endPrice / bestBidUsdc - 1 - this.kucoinTakerFee;
          }
        }

        logger.info("===========================================");
        logger.info(`New Auction found on ${INDEX_TO_NAME[market.marketIndex]} : Taker is ${positionOnDrift === "long" ? "⬇️" : "⬆️"}️ / Maker is ${positionOnDrift === "long" ? "⬆️️" : "⬇️"}️ start price: ${startPrice.toFixed(4)} slot: ${currSlot} / end price: ${endPrice.toFixed(4)} slot: ${aucEnd}`);
        logger.info(`Start price: ${startPrice.toFixed(4)} slot: ${currSlot} / End price: ${endPrice.toFixed(4)} slot: ${aucEnd}`);
        logger.info(`Amount: ${amount} / Total to fill: ${amountToFill} / Already filled: ${amountFilled}`);
        logger.info(`Kucoin Adjusted: Bid ${bestBidUsdc.toFixed(4)} Ask ${bestAskUsdc.toFixed(4)}, factor: ${factor}`);
        logger.info(`Kucoin: Bid ${bestBid.toFixed(4)} Ask ${bestAsk.toFixed(4)}`);
        const virtualPnL = amount * virtualPnLRel * offeredPrice;

        if (takePosition || isTestingAccount) {
          logger.info(`Bidding this auction ${positionOnDrift} at: ${offeredPrice.toFixed(4)}, hedge on Kucoin for ${bestKucoinValue}`);
          logger.info(`Virtual PnL: $ ${virtualPnL.toFixed(2)} / ${(virtualPnLRel * 100).toFixed(2)}%`);

          const offeredPriceBn = new BN(offeredPrice * PRICE_PRECISION.toNumber());
          try {

            const txSig = await this.executeAction({
              baseAssetAmount: jitMakerBaseAssetAmount,
              marketIndex: nodeToFill.node.order.marketIndex,
              direction: jitMakerDirection,
              price: offeredPriceBn,
              node: nodeToFill.node,
            });

            if (txSig) {
              logger.info(`☑️ JIT auction filled (account: ${nodeToFill.node.userAccount.toString()} - ${nodeToFill.node.order.orderId.toString()}), Tx: ${txSig}`);
            } else {
              logger.error(`⛔ Skip offering`);
            }
            return txSig;
          } catch (e) {
            nodeToFill.node.haveFilled = false;

            // If we get an error that order does not exist, assume its been filled by somebody else and we
            // have received the history record yet
            const errorCode = getErrorCode(e);

            if (errorCode) {
              this.errorCounter.add(1, { errorCode: errorCode.toString() });

              if (errorCode === 6061) {
                logger.warn(`JIT auction (account: ${nodeToFill.node.userAccount.toString()} - ${nodeToFill.node.order.orderId.toString()}): too late, offer dont exists anymore`);
              } else {
                logger.error(
                  `Error (${errorCode}) filling JIT auction (account: ${nodeToFill.node.userAccount.toString()} - ${nodeToFill.node.order.orderId.toString()})`
                );
              }
            } else {
              console.log(e);
            }

            /* todo remove this, fix error handling
            if (errorCode === 6042) {
              this.dlob.remove(
                nodeToFill.node.order,
                nodeToFill.node.userAccount,
                () => {
                  logger.error(
                    `Order ${nodeToFill.node.order.orderId.toString()} not found when trying to fill. Removing from order list`
                  );
                }
              );
            }
            */

            // console.error(error);
          }
        } else {
          logger.info(`⛔ Skip offering, acceptable price ${acceptablePrice} - Pnl would be ${(virtualPnLRel * 100).toFixed(2)}% below the limit ${(pt * 100).toFixed(2)}%`);
        }
      }
    } catch (e) {
      logger.error("Uncaught error in drawAndExecuteAction");
      console.log(e);
    }
  }

  private async executeAction(action: Action): Promise<TransactionSignature> {
    const currentState = this.agentState.stateType.get(action.marketIndex);

    if (this.RESTRICT_POSITION_SIZE) {
      if (
        currentState === StateType.CLOSING_LONG &&
        action.direction === PositionDirection.LONG
      ) {
        logger.info(
          `${this.name}: Skipping long action on market ${action.marketIndex}, since currently CLOSING_LONG`
        );
        return;
      }
      if (
        currentState === StateType.CLOSING_SHORT &&
        action.direction === PositionDirection.SHORT
      ) {
        logger.info(
          `${this.name}: Skipping short action on market ${action.marketIndex}, since currently CLOSING_SHORT`
        );
        return;
      }
    }

    const takerUserAccount = (
      await this.userMap.mustGet(action.node.userAccount.toString())
    ).getUserAccount();
    const takerAuthority = takerUserAccount.authority;

    const takerUserStats = await this.userStatsMap.mustGet(
      takerAuthority.toString()
    );
    const takerUserStatsPublicKey = takerUserStats.userStatsAccountPublicKey;
    const referrerInfo = takerUserStats.getReferrerInfo();

    return await this.driftClient.placeAndMakePerpOrder(
      {
        orderType: OrderType.LIMIT,
        marketIndex: action.marketIndex,
        baseAssetAmount: action.baseAssetAmount,
        direction: action.direction,
        price: action.price,
        postOnly: PostOnlyParams.MUST_POST_ONLY,
        immediateOrCancel: true,
      },
      {
        taker: action.node.userAccount,
        order: action.node.order,
        takerStats: takerUserStatsPublicKey,
        takerUserAccount: takerUserAccount,
      },
      referrerInfo
    );
  }

  private async tryMakeJitAuctionForMarket(market: PerpMarketAccount) {
    await this.updateAgentState();
    await this.drawAndExecuteAction(market);
  }

  private async tryMake() {
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

        await Promise.all(
          // TODO: spot
          this.driftClient.getPerpMarketAccounts().map((marketAccount) => {
            if (marketAccount.marketIndex === 0 || marketAccount.marketIndex === 1 || marketAccount.marketIndex === 2 || marketAccount.marketIndex === 3 || marketAccount.marketIndex === 5) {
              this.tryMakeJitAuctionForMarket(marketAccount);
            }
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
        this.tryJitDurationHistogram.record(
          duration,
          metricAttrFromUserAccount(
            user.getUserAccountPublicKey(),
            user.getUserAccount()
          )
        );
        // logger.debug(`${this.name} Bot took ${Date.now() - start}ms to run`);
        await this.watchdogTimerMutex.runExclusive(async () => {
          this.watchdogTimerLastPatTime = Date.now();
        });
      }
    }
  }
}
