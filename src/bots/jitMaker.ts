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
  Order as DriftOrder,
  getOrderSignature,
  MarketType,
  PostOnlyParams,
  calculateBidAskPrice,
  Wallet,
  ZERO,
  getMarketOrderParams,
  OptionalOrderParams,
  OraclePriceData,
  WrappedEvent,
  EventMap,
} from '@drift-labs/sdk';
import { Mutex, tryAcquire, withTimeout, E_ALREADY_LOCKED } from 'async-mutex';

import { TransactionSignature, PublicKey, TransactionMessage, VersionedTransaction, ComputeBudgetProgram } from '@solana/web3.js';

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { Bot } from '../types';

import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import { Counter, Histogram, Meter, ObservableGauge, ObservableResult, Attributes } from '@opentelemetry/api';
import { MeterProvider } from '@opentelemetry/sdk-metrics';
import { RuntimeSpec, metricAttrFromUserAccount } from '../metrics';
import { JitMakerConfig } from '../config';
import { KucoinController } from '../kucoin-api/kucoin';
import { Order as KucoinOrder, OrderBook } from '../kucoin-api/models';
import { PositionChangeOperationResponse } from '../kucoin-api/ws';
import { Contract } from '../kucoin-api/market';
import { INDEX_TO_LETTERS, INDEX_TO_NAME, INDEX_TO_SYMBOL, MaxSizeList, StateType, SYMBOL_TO_INDEX } from './utils';

type Action = {
  baseAssetAmount: BN;
  marketIndex: number;
  direction: PositionDirection;
  price: BN;
  node: DLOBNode;
};

type ActionTake = {
  baseAssetAmount: BN;
  marketIndex: number;
  direction: PositionDirection;
};

export type KucoinMarketData = {
  book: OrderBook,
  vwap: { bid: number, ask: number },
  bestPrice: { bid: number, ask: number }
}

type State = {
  stateType: Map<number, StateType>;
  spotMarketPosition: Map<number, SpotPosition>;
  perpMarketPosition: Map<number, PerpPosition>;
  kucoinMarketData: Map<number, KucoinMarketData>;
  kucoinContract: Map<number, Contract>;
  positions: Map<number, { kucoin: number, drift: number }>;
  exchangeDeltaTime: Map<number, number>;
  deltaKucoinDrift: Map<number, MaxSizeList>;
  openOrders: Map<number, { kucoin: Map<string, KucoinOrder>, drift: Map<string, DriftOrder> }>;
  expectingOrderDataFrom: Map<number, string | undefined>;
  kucoinOpenOrderExposureList: Map<number, number>;
};

const dlobMutexError = new Error('dlobMutex timeout');

enum METRIC_TYPES {
  try_jit_duration_histogram = 'try_jit_duration_histogram',
  runtime_specs = 'runtime_specs',
  mutex_busy = 'mutex_busy',
  errors = 'errors',
}

const COMPUTE_UNITS = 600_000;

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
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

  private readonly driftClient: DriftClient;
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
  private readonly metricsPort?: number;
  private exporter: PrometheusExporter;
  private meter: Meter;
  private bootTimeMs = Date.now();
  private runtimeSpecsGauge: ObservableGauge;
  private marketDataGauge: ObservableGauge;
  private readonly runtimeSpec: RuntimeSpec;
  private mutexBusyCounter: Counter;
  private errorCounter: Counter;
  private tryJitDurationHistogram: Histogram;

  private watchdogTimerMutex = new Mutex();
  private watchdogTimerLastPatTime = Date.now();

  private readonly MAX_POSITION_EXPOSURE;
  private kucoin: KucoinController;

  private mutexes: { [id: number]: { kucoin: Mutex, long: Mutex, short: Mutex, jit: Mutex } };
  private exchangeDelta: { [id: number]: number };
  private readonly profitThreshold: { [id: number]: number };
  private readonly kucoinMakerFee: number;
  private maxTradeSize: { [id: number]: number } = {};
  private maxTradeSizeInLot: { [id: number]: number } = {};
  private coolingPeriod: { [id: number]: { jit: number, kucoin: number, long: number, short: number } } = {};
  private priceDistanceBeforeUpdateLimit: { [id: number]: number } = {};
  private kucoinOpenOrderExposureList: { [id: number]: number } = {};
  private marketEnabled: number[] = [];
  private contractEnabled: string[] = [];

  private isActive = false;
  private allInitDone = false;
  private tradeIndex = 0;

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

    this.kucoinMakerFee = config.kucoinMakerFee;
    this.profitThreshold = config.profitThreshold;
    this.MAX_POSITION_EXPOSURE = config.maxPositionExposure;

    this.agentState = {
      stateType: new Map<number, StateType>(),
      spotMarketPosition: new Map<number, SpotPosition>(),
      perpMarketPosition: new Map<number, PerpPosition>(),
      openOrders: new Map<number, { kucoin: Map<string, KucoinOrder>, drift: Map<string, DriftOrder> }>(),
      expectingOrderDataFrom: new Map<number, string>(),
      kucoinOpenOrderExposureList: new Map<number, number>(),
      kucoinMarketData: new Map<number, KucoinMarketData>(),
      positions: new Map<number, { kucoin: number, drift: number }>(),
      kucoinContract: new Map<number, Contract>(),
      exchangeDeltaTime: new Map<number, number>(),
      deltaKucoinDrift: new Map<number, MaxSizeList>(),
    };

    for (const i of [0, 1, 2, 3, 4, 5]) {
      const x = INDEX_TO_LETTERS[i];
      const name = INDEX_TO_NAME[i];
      if (!config.marketEnabled[x]) continue;
      this.marketEnabled.push(i);
      this.contractEnabled.push(INDEX_TO_SYMBOL[i]);
      this.profitThreshold[i] = config.profitThreshold[x];
      this.priceDistanceBeforeUpdateLimit[i] = config.priceDistanceBeforeUpdateLimit[x];
      this.maxTradeSize[i] = config.maxTradeSize[x];
      this.kucoinOpenOrderExposureList[i] = 0;
      this.coolingPeriod[i] = { jit: 0, kucoin: 0, long: 0, short: 0 };
      this.exchangeDelta[i] = 0;
      this.mutexes[i] = { jit: new Mutex(), short: new Mutex(), long: new Mutex(), kucoin: new Mutex() };
      this.agentState.deltaKucoinDrift.set(i, new MaxSizeList(20));
      this.agentState.exchangeDeltaTime.set(i, 0);
      this.agentState.positions.set(i, { kucoin: 0, drift: 0 });
      this.agentState.stateType.set(i, StateType.NOT_STARTED);
      this.agentState.openOrders.set(i, { kucoin: new Map(), drift: new Map() });
      this.agentState.expectingOrderDataFrom.set(i, undefined);
      this.agentState.kucoinOpenOrderExposureList.set(i, 0);
      logger.info(`${name} enabled`);
    }

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
    const meterProvider = new MeterProvider();

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

    this.marketDataGauge = this.meter.createObservableGauge(
      'market_data',
      {
        description: "All the market data"
      }
    );

    this.marketDataGauge.addCallback(obs => this.marketDataCallback(obs));

    this.runtimeSpecsGauge.addCallback((obs) => {
      obs.observe(this.bootTimeMs, this.runtimeSpec);
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

    this.agentState.kucoinMarketData.forEach((marketData, marketIndex) => {
      const now = (new Date()).getTime();
      const book = marketData.book;
      const exchangeDeltaTime = this.agentState.exchangeDeltaTime.get(marketIndex);
      const deltaKucoinDrift = this.agentState.deltaKucoinDrift.get(marketIndex);
      if (now <= exchangeDeltaTime[marketIndex] && this.exchangeDelta[marketIndex] !== 0) return;
      if (!book) {
        this.exchangeDelta[marketIndex] = 0;
        this.agentState.exchangeDeltaTime.set(marketIndex, 0);
        logger.error(`Issue getting delta for ${marketIndex} - missing kucoin`);
      }
      const kucoinMid = (book._asks[0][0] + book._bids[0][0]) / 2;
      const oraclePrice = this.driftClient.getOracleDataForPerpMarket(marketIndex);
      const [bid, ask] = calculateBidAskPrice(
        this.driftClient.getPerpMarketAccount(marketIndex).amm,
        this.driftClient.getOracleDataForPerpMarket(marketIndex)
      );
      const bestAsk = this.dlob.getBestAsk(marketIndex, ask, this.slotSubscriber.getSlot(), MarketType.PERP, oraclePrice);
      const bestBid = this.dlob.getBestBid(marketIndex, bid, this.slotSubscriber.getSlot(), MarketType.PERP, oraclePrice);
      const formattedBestBidPrice = convertToNumber(bestBid, PRICE_PRECISION);
      const formattedBestAskPrice = convertToNumber(bestAsk, PRICE_PRECISION);
      if (formattedBestAskPrice && formattedBestBidPrice && formattedBestAskPrice > formattedBestBidPrice) {
        const driftMid = (formattedBestAskPrice + formattedBestBidPrice) / 2;
        const value = kucoinMid / driftMid;
        deltaKucoinDrift.add(value);
        this.exchangeDelta[marketIndex] = deltaKucoinDrift.getAverage();
      }
      this.agentState.exchangeDeltaTime.set(marketIndex, now + 30000);
    });
  }

  private async getKucoinPositionAndLog(symbol: string, marketIndex: number): Promise<boolean> {
    try {
      const p = await this.kucoin.api.position({ symbol });
      this.agentState.positions.get(marketIndex).kucoin = p.data.currentQty;
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

    let retries = 0;
    while (!this.kucoin.api.ws.openingTime) {
      retries += 1;

      if (retries >= 30) {
        throw Error("Could not connect to Kucoin websocket");
      }

      logger.info("Waiting for websocket isConnected signal...");

      await sleep(2000);
    }

    for (const contract of this.contractEnabled) {
      this.kucoin.subscribe(contract).then(() => {
        logger.info(`✅ Websocket ${contract} subscribed with kucoin`);
      });
    }

    // Confirm balance
    const resp = await this.kucoin.api.accountOverview({ currency: 'USDT' });
    logger.info(`Kucoin account Balance + unrealised Pnl: ${resp.data.accountEquity}`);

    // Lot size
    const contractList = await this.kucoin.api.contractList();
    for (const x of contractList.data) {

      if (this.contractEnabled.includes(x.symbol)) {
        const marketIndex = SYMBOL_TO_INDEX[x.symbol];
        logger.info(`Multiplier for ${x.symbol}: ${x.multiplier}`);
        this.agentState.kucoinContract.set(marketIndex, x);
        this.maxTradeSizeInLot[marketIndex] = Math.trunc(this.maxTradeSize[marketIndex] / x.multiplier);
      }
    }

    for (const contract of this.contractEnabled) {
      const marketIndex = SYMBOL_TO_INDEX[contract];
      await this.retryGetKucoinPositionAndLog(contract, marketIndex);
      await sleep(1000);
    }

    this.kucoin.on('book', this.updateKucoinMarketData.bind(this));
    this.kucoin.on('positionOperationChange', this.updateKucoinPosition.bind(this));
    this.kucoin.on('order', this.updateKucoinOrder.bind(this));
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

    initPromises.push(this.userMap.fetchAllUsers());

    await Promise.all(initPromises);
    logger.info(`All init done`);
    this.allInitDone = true;
  }

  public async reset(): Promise<void> {
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

  public async startIntervalLoop(intervalMs: number): Promise<void> {
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

  public async trigger(record: WrappedEvent<keyof EventMap>): Promise<void> {
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

  private updateKucoinMarketData(r: any) {
    const marketIndex = SYMBOL_TO_INDEX[r.data.symbol];
    if (this.marketEnabled.includes(marketIndex)) {
      const newData: KucoinMarketData = {
        book: r.data,
        vwap: { bid: r.data.vwap('bids', 5), ask: r.data.vwap('asks', 5) },
        bestPrice: { bid: r.data.bestBid(10), ask: r.data.bestAsk(10) }
      };
      this.agentState.kucoinMarketData.set(marketIndex, newData);
    }
  }

  private updateKucoinPosition(r: PositionChangeOperationResponse) {
    const marketIndex = SYMBOL_TO_INDEX[r.symbol];
    if (marketIndex === undefined) return;
    this.agentState.positions.get(marketIndex).kucoin = r.currentQty;
    const name = INDEX_TO_NAME[marketIndex];
    logger.info(`${name}: kucoin currentQty ${r.currentQty}`);
  }

  /**
   * Called when an order update is received from the websocket feed
   * Maintain the Kucoin Open order list and update the open exposure
   */
  private updateKucoinOrder(r: KucoinOrder) {
    const marketIndex = SYMBOL_TO_INDEX[r.symbol];
    const name = INDEX_TO_NAME[marketIndex];
    const kucoinOrderList = this.agentState.openOrders.get(marketIndex).kucoin;

    if (r.status === 'open' || r.status === 'match') {
      if (kucoinOrderList.has(r.orderId)) {
        logger.info(`${name} ${r.orderId} Open limit order changed on Kucoin ${r.side === 'sell' ? "⬇️" : "⬆️"} ${r.matchSize}/${r.size} @ $${r.price}`);
      } else {
        logger.info(`${name} ${r.orderId} New Open limit order found on Kucoin ${r.side === 'sell' ? "⬇️" : "⬆️"} ${r.matchSize}/${r.size} @ $${r.price}`);
      }
      this.agentState.openOrders.get(marketIndex).kucoin.set(r.orderId, r);

    } else if (r.status === 'done') {
      if (kucoinOrderList.get(r.orderId)) {
        kucoinOrderList.delete(r.orderId);
        logger.info(`${name} ${r.orderId} Removal - reason: ${r.type} ${r.side === 'sell' ? "⬇️" : "⬆️"} ${r.matchSize}/${r.size} @ $${r.price}`);
      }
    }
    let exposure = 0;
    kucoinOrderList.forEach((o) => {
      const x = o.side === 'sell' ? -1 : 1;
      exposure += x * (+o.size - +o.matchSize);
    });
    if (exposure !== this.kucoinOpenOrderExposureList[marketIndex]) {
      logger.info(`${name} - Open order exposure ${this.kucoinOpenOrderExposureList[marketIndex]} -> ${exposure}`);
    }
    this.kucoinOpenOrderExposureList[marketIndex] = exposure;

    if (this.agentState.expectingOrderDataFrom.get(marketIndex) === r.orderId) {
      this.agentState.expectingOrderDataFrom.set(marketIndex, undefined);
      this.coolingPeriod[marketIndex].kucoin = 0;
    }
  }

  public viewDlob(): DLOB {
    return this.dlob;
  }

  private nodeCanBeFilled(node: DLOBNode, userAccountPublicKey: PublicKey):
    boolean {
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
      const perpPositions: { [id: number]: undefined | PerpPosition } = {
        0: undefined,
        1: undefined,
        2: undefined,
        3: undefined,
        5: undefined
      };

      for await (const p of x) {

        if (this.marketEnabled.includes(p.marketIndex)) {
          if (!perpPositions[p.marketIndex] || !p.baseAssetAmount.eq(ZERO)) {
            perpPositions[p.marketIndex] = p;
          }
        }
      }
      for (const i of this.marketEnabled) {
        const p = perpPositions[i];
        if (!p) continue;
        const perpPosition = this.agentState.perpMarketPosition.get(p.marketIndex);

        if (perpPosition && p.baseAssetAmount.eq(perpPosition.baseAssetAmount)) continue;

        const baseValue = convertToNumber(p.baseAssetAmount, BASE_PRECISION);

        const multiplier = this.agentState.kucoinContract.get(p.marketIndex).multiplier;
        const previousBaseValue = perpPosition ? convertToNumber(perpPosition.baseAssetAmount, BASE_PRECISION) : 0;
        const name = INDEX_TO_NAME[p.marketIndex];

        // check if we need to enter a closing state
        const accountCollateral = convertToNumber(
          this.driftClient.getUser().getTotalCollateral(),
          QUOTE_PRECISION
        );
        const positionValue = convertToNumber(
          p.quoteAssetAmount,
          QUOTE_PRECISION
        );

        this.agentState.positions.get(p.marketIndex).drift = baseValue / multiplier;

        const exposure = Math.abs(positionValue / accountCollateral);
        logger.info(`${name} - Drift Position has changed from ${previousBaseValue.toPrecision(4)} to ${baseValue.toPrecision(4)} - New Exposure ${(exposure * 100).toPrecision(2)}`);

        // update current position based on market position
        this.agentState.perpMarketPosition.set(p.marketIndex, p);

        // update state
        let currentState = this.agentState.stateType.get(p.marketIndex);
        if (!currentState) {
          this.agentState.stateType.set(p.marketIndex, StateType.NEUTRAL);
          currentState = StateType.NOT_STARTED;
        }

        if (currentState === StateType.CLOSING_LONG && p.baseAssetAmount.gt(new BN(0))) {
          logger.info(`${name} - Drift state remains "closing_long"`);
        } else if (currentState === StateType.CLOSING_SHORT && p.baseAssetAmount.lt(new BN(0))) {
          logger.info(`${name} - Drift state remains "closing_short"`);
        } else if (exposure >= this.MAX_POSITION_EXPOSURE && p.baseAssetAmount.lt(new BN(0))) {
          logger.info(`${name} - Drift state changed from "${currentState.toLowerCase()}" to "closing_short"`);
          this.agentState.stateType.set(
            p.marketIndex,
            StateType.CLOSING_SHORT
          );
        } else if (exposure >= this.MAX_POSITION_EXPOSURE && p.baseAssetAmount.gt(new BN(0))) {
          logger.info(`${name} - Drift state changed from "${currentState.toLowerCase()}" to "closing_long"`);
          this.agentState.stateType.set(
            p.marketIndex,
            StateType.CLOSING_LONG
          );
        } else if (p.baseAssetAmount.gt(new BN(0))) {
          logger.info(`${name} - Drift state changed from "${currentState.toLowerCase()}" to "long"`);
          this.agentState.stateType.set(p.marketIndex, StateType.LONG);
        } else if (p.baseAssetAmount.lt(new BN(0))) {
          logger.info(`${name} - Drift state changed from "${currentState.toLowerCase()}" to "short"`);
          this.agentState.stateType.set(p.marketIndex, StateType.SHORT);
        } else {
          logger.info(`${name} - Drift state changed from "${currentState.toLowerCase()}" to "neutral"`);
          this.agentState.stateType.set(p.marketIndex, StateType.NEUTRAL);
        }
      }

      const existingOrders = new Set<number>();
      for (const marketIndex of this.marketEnabled) {
        const openOrders = this.agentState.openOrders.get(marketIndex).drift;
        if (openOrders) {
          for (const o of openOrders.values()) {
            existingOrders.add(o.orderId);
          }
        }
        this.agentState.openOrders.get(marketIndex).drift.clear();
      }

      // Update open order list
      this.driftClient.getUserAccount().orders.map((o) => {
        if (isVariant(o.status, 'init')) {
          return;
        }

        if (!isVariant(o.orderType, 'limit')) return;

        const marketIndex = o.marketIndex;
        const name = INDEX_TO_NAME[marketIndex];

        if (existingOrders.has(o.orderId)) {
          existingOrders.delete(o.orderId);
        } else {
          const amount = convertToNumber(o.baseAssetAmount, BASE_PRECISION);
          logger.info(`${o.orderId} - New limit order found on ${name}: ${amount.toFixed(4)} @ $${o.price} ${isVariant(o.direction, 'long') ? "⬆️" : "⬇️"}`);
        }

        this.agentState.openOrders.get(marketIndex).drift.set(o.orderId.toString(), o);
      });

      for (const orderId of existingOrders) {
        logger.info(`${orderId} - Order not open anymore`);
      }

    } catch
      (e) {
      logger.error(`Uncaught error in Update agent state`);
      console.log(e);
    }
  }

  /**
   * Provide the Kucoin price target to place limit order
   */
  private getKucoinPriceTarget(marketIndex: number, side: 'buy' | 'sell'): number {
    // TODO: UPDATE
    const marketData = this.agentState.kucoinMarketData.get(marketIndex);
    // return side === "buy" ? this.bookKucoin[i]._bids[0][0] : this.bookKucoin[i]._asks[0][0];
    return side === "buy" ? marketData.vwap.bid : marketData.vwap.ask;
  }

  /**
   * Provide the Kucoin price target to place limit order
   */
  private isPriceCloseToTargetKucoin(marketIndex: number, order: KucoinOrder): boolean {
    // TODO: UPDATE
    return Math.trunc(this.getKucoinPriceTarget(marketIndex, order.side) - +order.price) < 0.1;
  }

  /**
   * Cancel Order
   */
  private cancelKucoinOrder(marketIndex: number, orderId: string): void {
    const now = (new Date()).getTime();
    this.coolingPeriod[marketIndex].kucoin = now + 3000;
    if (!this.dryRun) {
      this.kucoin.api.cancelOrder({ orderId }).then(r => {
        if (r.code === "200000") {
          logger.info(`Kucoin order ${orderId} cancelled`);
        } else {
          logger.error("Error Cancelling position");
          console.log(r);
        }
      }).catch(e => {
        logger.error(`Error cancelling hedge on kucoin: ${e}`);
      });
    } else {
      logger.info(`✅ Dry run - Hedge placed successfully`);
    }
  }

  /**
   * Place Order
   */
  private placeKucoinOrder(marketIndex: number, side: 'buy' | 'sell', symbol: string, price: number, size: number): void {
    const now = (new Date()).getTime();
    const clientOid = `${symbol}-${(new Date()).getTime()}`;
    this.coolingPeriod[marketIndex].kucoin = now + 3000;
    if (!this.dryRun) {
      this.kucoin.api.placeLimitOrder({
        symbol,
        size,
        price: price.toString(),
        side: side,
        clientOid,
        leverage: 10,
        type: 'limit',
        postOnly: true
      }).then(r => {
        if (r.code === "200000") {
          logger.info(`✅ Hedge placed successfully ${r.data.orderId}`);
          this.agentState.expectingOrderDataFrom.set(marketIndex, r.data.orderId);
        } else if (r.code === '300008') {
          logger.info(`Limit order could not be placed (most likely because the market moved`);
        } else {
          logger.error("Error hedging the position");
          console.log(r);
        }
      }).catch(e => {
        logger.error(`Error buying hedge on kucoin: ${e}`);
      });
    } else {
      logger.info(`✅ Dry run - Hedge placed successfully`);
    }
  }

  /**
   * Verify the exposure on Drift, place a limit order on Kucoin if needed.
   * If a limit order already exists, verify that the price is still in line with the target otherwise cancel
   */
  private async hedgeWithKucoin(marketIndex: number) {

    const symbol = INDEX_TO_SYMBOL[marketIndex];

    const mutex: Mutex = this.mutexes[marketIndex].kucoin;
    if (mutex.isLocked()) {
      return;
    }
    const release = await mutex.acquire();
    try {
      const driftPosition = Math.round(this.agentState.positions.get(marketIndex).drift);
      const kucoinPosition = this.agentState.positions.get(marketIndex).kucoin;
      const openOrderPosition = this.kucoinOpenOrderExposureList[marketIndex];
      const orderList = this.agentState.openOrders.get(marketIndex).kucoin;

      // Take a new position if the total position is different from 0
      const delta = driftPosition + kucoinPosition + openOrderPosition;

      // If no open order -> Place one limit order
      if (orderList.size > 1) {

        const orderId = orderList.keys().next().value;
        // There should always be only one Kucoin order at the same time. If there is more than one. Cancel one
        logger.info(`Cancel order ${orderId} because too many positions are opened`);
        this.cancelKucoinOrder(marketIndex, orderId);
      } else if (Math.trunc(Math.abs(delta)) > 0) {

        // If position smaller than 0.5 do not update the position as you wont be able to take the position on kucoin
        const orderList = this.agentState.openOrders.get(marketIndex).kucoin;
        if (orderList.size === 0) {
          const side = delta > 0 ? 'sell' : 'buy';
          const price = this.getKucoinPriceTarget(marketIndex, side);
          const size = Math.trunc(Math.abs(delta));

          logger.info(`Hedging ${symbol} Open position, taking order ${side} ${Math.abs(delta)}`);
          this.placeKucoinOrder(marketIndex, side, symbol, price, size);

        } else {

          const orderId = orderList.keys().next().value;
          // If the exposure is non-zero, cancel the order so that a new one can be bplaced
            logger.debug(`Cancel order ${orderId} due to exposure change`);
            this.cancelKucoinOrder(marketIndex, orderId);
        }
      } else if (orderList.size === 1) {

        const [orderId, order] = orderList.entries().next().value;

        if (!this.isPriceCloseToTargetKucoin(marketIndex, order)) {

          // Verify that the current price is not too far from the open order price otherwise cancel
          logger.debug(`Cancel order ${orderId} due to position change`);
          this.cancelKucoinOrder(marketIndex, orderId);
        }
      }
    } finally {
      release();
    }
  }

  private async placeDriftOrder(orderParams: OptionalOrderParams) {
    const name = INDEX_TO_NAME[orderParams.marketIndex];
    try {
      if (!this.dryRun) {
        const tx = await this.driftClient.placePerpOrder(orderParams);
        logger.info(
          `${name} placing order ${this.driftClient
          .getUserAccount()
          .authority.toBase58()}: ${tx}`
        );
      } else {
        logger.info(
          `${name} placing order ${this.driftClient
          .getUserAccount()
          .authority.toBase58()}-: dryRun`
        );
      }
      if (isVariant(orderParams.direction, 'long')) {
        this.coolingPeriod[orderParams.marketIndex].long = new Date().getTime() + 2000;
      } else {
        this.coolingPeriod[orderParams.marketIndex].short = new Date().getTime() + 2000;
      }
    } catch (e) {
      logger.error(`${name} Error placing a long order`);
      logger.error(e);
    }
  }

  private async cancelDriftOrder(orderId: number, marketIndex: number, isLong: boolean) {
    const name = INDEX_TO_NAME[marketIndex];
    try {
      if (!this.dryRun) {
        const tx = await this.driftClient.cancelOrder(orderId);
        logger.info(
          `${name} cancelling order ${this.driftClient
          .getUserAccount()
          .authority.toBase58()}-${orderId}: ${tx}`
        );
      } else {
        logger.info(
          `${name} cancelling order ${this.driftClient
          .getUserAccount()
          .authority.toBase58()}-${orderId}: dryRun`
        );
      }

      if (isLong) {
        this.coolingPeriod[marketIndex].long = new Date().getTime() + 2000;
      } else {
        this.coolingPeriod[marketIndex].short = new Date().getTime() + 2000;
      }
    } catch (e) {
      logger.error(`${name} Error cancelling an long order ${orderId}`);
    }
  }

  private async updateLimitForClosingPosition(marketIndex: number, isLong: boolean, orders: DriftOrder[], bestBidBn: BN, bestAskBn: BN, oracle: OraclePriceData) {
    // const name = INDEX_TO_NAME[marketIndex];
    // const biasNum = new BN(100);
    // const biasDenom = new BN(100);
    // const oracleSpread = isLong ? oracle.price.sub(bestBidBn) : bestAskBn.sub(oracle.price);
    const offset = isLong ? bestBidBn.sub(oracle.price) : bestAskBn.sub(oracle.price);
    // console.log(convertToNumber(offset, PRICE_PRECISION), convertToNumber(oracleSpread, PRICE_PRECISION));
    if (orders.length === 1) {
      // const order = orders[0];
      // const currentPrice = convertToNumber(order.price, PRICE_PRECISION);
      // if (Math.abs(targetPrice - currentPrice) > this.priceDistanceBeforeUpdateLimit[marketIndex] || !oracle.hasSufficientNumberOfDataPoints) {
      //   logger.info(`${name} Cancelling limit price of ${isLong ? 'long' : 'short'} order $${targetPrice}`);
      //   logger.info(`Has Sufficient datapoint: ${oracle.hasSufficientNumberOfDataPoints} - confidence: ${oracle.confidence} - oracle: ${oraclePrice} - order: ${currentPrice} target Price: ${targetPrice}`);
      //   await this.cancelOrder(order.orderId, marketIndex, isLong);
      //   return;
      // }
    } else if (!oracle.hasSufficientNumberOfDataPoints) {
      return;
    } else {
      const size = new BN(Math.round(this.positionDrift[marketIndex] * this.futureKucoinContract[marketIndex].multiplier) * BASE_PRECISION.toNumber());
      logger.info(`Placing closing short limit order`);
      logger.info(` .        oracleSlot:  ${oracle.slot.toString()}`);
      logger.info(` .        oracleConf:  ${oracle.confidence.toString()}`);

      await this.placeOrder({
        marketIndex: marketIndex,
        orderType: OrderType.LIMIT,
        direction: isLong ? PositionDirection.LONG : PositionDirection.SHORT,
        baseAssetAmount: size,
        oraclePriceOffset: offset.toNumber(),
        postOnly: PostOnlyParams.MUST_POST_ONLY
      });
    }
  }

  private async updateLimitForTakingRisk(marketIndex: number, isLong: boolean, orders: DriftOrder[], bestBidBn: BN, bestAskBn: BN) {
    const name = INDEX_TO_NAME[marketIndex];

    if (this.vwapKucoin[marketIndex].asks === 0 || this.vwapKucoin[marketIndex].bids === 0) return;

    const factor = this.exchangeDelta[marketIndex];
    const kucoinAskAdjusted = this.vwapKucoin[marketIndex].asks / factor;
    const kucoinBidAdjusted = this.vwapKucoin[marketIndex].bids / factor;

    const pt = this.profitThreshold[marketIndex];
    const driftBid = convertToNumber(bestBidBn, PRICE_PRECISION);
    const driftAsk = convertToNumber(bestAskBn, PRICE_PRECISION);

    let acceptablePrice = isLong ? kucoinAskAdjusted / (1 + this.kucoinTakerFee + pt) : (1 + this.kucoinTakerFee + pt) * kucoinBidAdjusted;
    if (isLong && acceptablePrice > driftBid && bestBidBn.lt(bestAskBn)) {
      acceptablePrice = driftBid;
    } else if (!isLong && driftAsk < driftAsk && bestBidBn.lt(bestAskBn)) {
      acceptablePrice = driftAsk;
    }

    if (orders.length === 1) {
      const order = orders[0];
      const currentPrice = convertToNumber(order.price, PRICE_PRECISION);
      if (Math.abs(acceptablePrice - currentPrice) > this.priceDistanceBeforeUpdateLimit[marketIndex]) {
        logger.info(`${name} Cancelling limit price of ${isLong ? "long" : "short"} order $${acceptablePrice}`);
        await this.cancelOrder(order.orderId, marketIndex, isLong);
        return;
      }
    } else if (this.deltaKucoinDrift[marketIndex].getItems().length > 5) {
      const newPrice = new BN(acceptablePrice * PRICE_PRECISION.toNumber());
      const currentPositionToAdd = this.positionDrift[marketIndex] - Math.trunc(this.positionDrift[marketIndex]);

      const size = new BN((this.maxTradeSize[marketIndex] - currentPositionToAdd) * BASE_PRECISION.toNumber());
      logger.info(`${name} 📝 Placing limit price of ${isLong ? "long" : "short"} order $${acceptablePrice}`);
      await this.placeOrder({
        marketIndex: marketIndex,
        orderType: OrderType.LIMIT,
        direction: isLong ? PositionDirection.LONG : PositionDirection.SHORT,
        baseAssetAmount: size,
        price: newPrice,
        postOnly: PostOnlyParams.MUST_POST_ONLY,
      });
    }

  }

  private async updateLimit(openOrders: DriftOrder[], marketIndex: number, direction: 'long' | 'short', currentState: StateType) {
    const name = INDEX_TO_NAME[marketIndex];
    const isLong = direction === 'long';
    const mutex = isLong ? this.takingLimitPositionLongMutexes[marketIndex] : this.takingLimitPositionShortMutexes[marketIndex];
    const coolingPeriod = isLong ? this.coolingLimitLongPeriod[marketIndex] : this.coolingLimitShortPeriod[marketIndex];
    if (mutex.isLocked() || coolingPeriod > new Date().getTime()) {
      return;
    }
    const release = await mutex.acquire();
    try {
      const orders = openOrders.filter(item => isVariant(item.direction, direction));
      if (orders.length > 1 || (orders.length === 1 && ((!isLong && currentState === StateType.CLOSING_SHORT) || (isLong && currentState === StateType.CLOSING_LONG)))) {
        logger.info(`${name} - ${isLong ? "long" : "short"} - Closing order because there are too many`);
        await this.cancelOrder(orders[0].orderId, marketIndex, isLong);
        return;
      }

      if (this.exchangeDelta[marketIndex] === 0) {
        return;
      }

      const oracle = this.driftClient.getOracleDataForPerpMarket(marketIndex);
      const [bid, ask] = calculateBidAskPrice(
        this.driftClient.getPerpMarketAccount(marketIndex).amm,
        this.driftClient.getOracleDataForPerpMarket(marketIndex)
      );

      const bestAskBn = this.dlob.getBestAsk(marketIndex, ask, this.slotSubscriber.getSlot(), MarketType.PERP, oracle);
      const bestBidBn = this.dlob.getBestBid(marketIndex, bid, this.slotSubscriber.getSlot(), MarketType.PERP, oracle);

      if ((currentState === StateType.CLOSING_LONG && !isLong) || (currentState === StateType.CLOSING_SHORT && isLong)) {
        await this.updateLimitForClosingPosition(marketIndex, isLong, orders, bestBidBn, bestAskBn, oracle);
      } else if (currentState && currentState !== StateType.CLOSING_LONG && currentState !== StateType.CLOSING_SHORT && currentState !== StateType.NOT_STARTED) {
        await this.updateLimitForTakingRisk(marketIndex, isLong, orders, bestBidBn, bestAskBn);
      }

    } catch (e) {
      logger.error(`Uncaught error in updateLimit: ${e}`);
      console.log(e);
    } finally {
      release();
    }

  }

  private async checkAndUpdateLimitOrders(marketIndex: number) {

    if (!this.isActive) return;
    const openOrders = this.agentState.openOrders.get(marketIndex) ?? [];

    if (this.kucoin.handlers[INDEX_TO_SYMBOL[marketIndex]].lastRcv < (new Date().getTime() - 2000)) return;
    const currentState = this.agentState.stateType.get(marketIndex);
    await Promise.all([
      this.updateLimit(openOrders, marketIndex, 'long', currentState),
      this.updateLimit(openOrders, marketIndex, 'short', currentState)
    ]);
  }

  /**
   * Draws an action based on the current state of the bot.
   *
   */
  private async checkForAuction(market: PerpMarketAccount) {

    const marketIndex = market.marketIndex;
    try {
      // get nodes available to fill in the jit auction
      const nodesToFill = this.dlob.findJitAuctionNodesToFill(
        marketIndex,
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

        const startPrice = convertToNumber(
          nodeToFill.node.order.auctionStartPrice,
          PRICE_PRECISION
        );
        const endPrice = convertToNumber(
          nodeToFill.node.order.auctionEndPrice,
          PRICE_PRECISION
        );

        const kucoinBook = this.bookKucoin[marketIndex];

        if (!kucoinBook) {
          logger.error(
            `Kucoin price not found for market ${marketIndex}`
          );
          continue;
        } else if (kucoinBook.datetime.getTime() < new Date().getTime() - 10000) {
          logger.error(
            `Kucoin price not updated for market ${marketIndex} since ${(new Date().getTime() - kucoinBook.datetime.getTime()) / 1000} seconds`
          );
          continue;
        }

        const baseAmountToBeFilled = nodeToFill.node.order.baseAssetAmount.sub(
          nodeToFill.node.order.baseAssetAmountFilled
        );
        const kucoinLotSize = this.futureKucoinContract[marketIndex].multiplier;

        const amountAvailable = convertToNumber(baseAmountToBeFilled, BASE_PRECISION);
        const maxSizeInLot = this.maxTradeSizeInLot[marketIndex];
        if (amountAvailable < kucoinLotSize) continue;

        const amountAvailableInLot = Math.trunc(amountAvailable / kucoinLotSize);
        const amountToTakeInLot = Math.min(maxSizeInLot, amountAvailableInLot);
        const baseFillAmountNumber = amountToTakeInLot * kucoinLotSize;
        const jitMakerBaseAssetAmount = new BN(
          baseFillAmountNumber * BASE_PRECISION.toNumber()
        );

        const amount = convertToNumber(jitMakerBaseAssetAmount, BASE_PRECISION);
        if (amount === 0) {
          continue;
        }
        const amountToFill = convertToNumber(nodeToFill.node.order.baseAssetAmount, BASE_PRECISION);
        const amountFilled = convertToNumber(nodeToFill.node.order.baseAssetAmountFilled, BASE_PRECISION);

        const askKucoin = kucoinBook.bestAsk(amount);
        const bidKucoin = kucoinBook.bestBid(amount);

        if (this.exchangeDelta[marketIndex] === 0) {
          continue;
        }

        const factor = this.exchangeDelta[marketIndex];

        const askKucoinUsdc = askKucoin / factor;
        const bidKucoinUsdc = bidKucoin / factor;

        if (endPrice < 0 || startPrice < 0) continue;
        if (askKucoinUsdc * 2 < endPrice || bidKucoinUsdc / 2 > endPrice) continue;

        // calculate jit maker order params
        const orderDirection = nodeToFill.node.order.direction;
        const jitMakerDirection = isVariant(orderDirection, 'long')
          ? PositionDirection.SHORT
          : PositionDirection.LONG;

        const currentState = this.agentState.stateType.get(marketIndex);
        if (this.RESTRICT_POSITION_SIZE) {
          if (
            currentState === StateType.CLOSING_LONG &&
            jitMakerDirection === PositionDirection.LONG
          ) {
            continue;
          }
          if (
            currentState === StateType.CLOSING_SHORT &&
            jitMakerDirection === PositionDirection.SHORT
          ) {
            continue;
          }
        }

        this.tradeIndex += 1;
        const idx = this.tradeIndex;

        logger.info(
          `${idx} - node slot: ${
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
          `${idx} - quoting order for node: ${nodeToFill.node.userAccount.toBase58()} - ${nodeToFill.node.order.orderId.toString()}, orderBaseFilled: ${convertToNumber(
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
        let pt = this.profitThreshold[marketIndex];
        let takePosition = false;
        let offeredPrice = 0;
        let virtualPnLRel = 0;
        let positionOnDrift = '';
        let bestKucoinValue;
        let acceptablePrice = 0;
        if (jitMakerDirection === PositionDirection.LONG) {
          if (currentState === StateType.CLOSING_SHORT) pt = this.profitThresholdIfReduce[marketIndex];
          acceptablePrice = bidKucoinUsdc / (1 + this.kucoinTakerFee + pt);
          offeredPrice = Math.min(startPrice, acceptablePrice);
          virtualPnLRel = bidKucoinUsdc / offeredPrice - 1 - this.kucoinTakerFee;
          bestKucoinValue = bidKucoinUsdc;
          positionOnDrift = 'long';
          if (acceptablePrice >= endPrice) {
            takePosition = true;
          } else {
            virtualPnLRel = bidKucoinUsdc / endPrice - 1 - this.kucoinTakerFee;
          }
        } else {
          if (currentState === StateType.CLOSING_LONG) pt = this.profitThresholdIfReduce[marketIndex];
          acceptablePrice = (1 + this.kucoinTakerFee + pt) * askKucoinUsdc;
          offeredPrice = Math.max(startPrice, acceptablePrice);
          bestKucoinValue = askKucoinUsdc;
          virtualPnLRel = offeredPrice / askKucoinUsdc - 1 - this.kucoinTakerFee;
          positionOnDrift = 'short';
          if (acceptablePrice <= endPrice) {
            takePosition = true;
          } else {
            virtualPnLRel = endPrice / askKucoinUsdc - 1 - this.kucoinTakerFee;
          }
        }

        logger.info(`${idx} - New Auction found on ${INDEX_TO_NAME[market.marketIndex]} : Taker is ${positionOnDrift === "long" ? "⬇️" : "⬆️"}️ / Maker is ${positionOnDrift === "long" ? "⬆️️" : "⬇️"}️ start price: ${startPrice.toFixed(4)} slot: ${currSlot} / end price: ${endPrice.toFixed(4)} slot: ${aucEnd}`);
        logger.info(`${idx} - Start price: ${startPrice.toFixed(4)} slot: ${currSlot} / End price: ${endPrice.toFixed(4)} slot: ${aucEnd}`);
        logger.info(`${idx} - Amount: ${amount} / Total to fill: ${amountToFill} / Already filled: ${amountFilled}`);
        logger.info(`${idx} - Kucoin Adjusted: Bid ${bidKucoinUsdc.toFixed(4)} Ask ${askKucoinUsdc.toFixed(4)}, factor: ${factor}`);
        logger.info(`${idx} - Kucoin: Bid ${bidKucoin.toFixed(4)} Ask ${askKucoin.toFixed(4)}`);
        const virtualPnL = amount * virtualPnLRel * offeredPrice;

        if (currSlot >= aucEnd) {
          logger.warn(`${idx} - Processing error too late delta between curr slot and aucEnd: ${currSlot - aucEnd}`);
          continue;
        }

        logger.debug(`${idx} - aucend - currslot ${aucEnd - currSlot}`);

        if (takePosition) {
          logger.info(`${idx} - Bidding this auction ${positionOnDrift} at: ${offeredPrice.toFixed(4)}, hedge on Kucoin for ${bestKucoinValue}`);
          logger.info(`${idx} - Virtual PnL: $ ${virtualPnL.toFixed(2)} / ${(virtualPnLRel * 100).toFixed(2)}%`);

          const mutex = this.takingPositionMutexes[marketIndex];
          const offeredPriceBn = new BN(offeredPrice * PRICE_PRECISION.toNumber());
          if (mutex.isLocked()) {
            logger.info(`${idx} - ❌ Client is locked - skipping`);
            continue;
          }

          const release = await mutex.acquire();
          try {

            const txSig = await this.executeAction({
              baseAssetAmount: jitMakerBaseAssetAmount,
              marketIndex: nodeToFill.node.order.marketIndex,
              direction: jitMakerDirection,
              price: offeredPriceBn,
              node: nodeToFill.node,
            }, idx);

            this.coolingPeriod[marketIndex] = new Date().getTime() + 6000;
            logger.info(`${idx} - ✅ JIT auction submitted (account: ${nodeToFill.node.userAccount.toString()} - ${nodeToFill.node.order.orderId.toString()}), Tx: ${txSig}`);
          } catch (e) {
            nodeToFill.node.haveFilled = false;

            // If we get an error that order does not exist, assume its been filled by somebody else and we
            // have received the history record yet
            const errorCode = getErrorCode(e);

            if (errorCode) {
              this.errorCounter.add(1, { errorCode: errorCode.toString() });

              if (errorCode === 6061) {
                logger.warn(`${idx} - ❌ JIT auction (account: ${nodeToFill.node.userAccount.toString()} - ${nodeToFill.node.order.orderId.toString()}): too late, offer dont exists anymore`);
              } else {
                logger.error(
                  `${idx} - ❌ Error (${errorCode}) filling JIT auction (account: ${nodeToFill.node.userAccount.toString()} - ${nodeToFill.node.order.orderId.toString()})`
                );
              }
            } else {
              logger.error(`${idx} - ❌ Error Other error while doing transaction`);
              console.log(e);
            }

            // console.error(error);
          } finally {
            release();
          }
        } else {
          logger.info(`${idx} - ⛔ Skip offering, acceptable price ${acceptablePrice} - Pnl would be ${(virtualPnLRel * 100).toFixed(2)}% below the limit ${(pt * 100).toFixed(2)}%`);
        }
      }
    } catch (e) {
      logger.error("Uncaught error in drawAndExecuteAction");
      console.log(e);
    }
  }

  private async executeAction(action: Action, tradeIdx: number):
    Promise<TransactionSignature> {

    const t1 = (new Date()).getTime();
    const takerUserAccount = (
      await this.userMap.mustGet(action.node.userAccount.toString())
    ).getUserAccount();
    const takerAuthority = takerUserAccount.authority;
    const t2 = (new Date()).getTime();
    const takerUserStats = await this.userStatsMap.mustGet(
      takerAuthority.toString()
    );
    const t3 = (new Date()).getTime();
    const takerUserStatsPublicKey = takerUserStats.userStatsAccountPublicKey;
    const referrerInfo = takerUserStats.getReferrerInfo();

    const orderParams = {
      orderType: OrderType.LIMIT,
      marketIndex: action.marketIndex,
      baseAssetAmount: action.baseAssetAmount,
      direction: action.direction,
      price: action.price,
      postOnly: PostOnlyParams.MUST_POST_ONLY,
      immediateOrCancel: true,
    };

    const takerInfo = {
      taker: action.node.userAccount,
      order: action.node.order,
      takerStats: takerUserStatsPublicKey,
      takerUserAccount: takerUserAccount,
    };

    const ix = await this.driftClient.getPlaceAndMakePerpOrderIx(
      orderParams,
      takerInfo,
      referrerInfo
    );

    const t4 = (new Date()).getTime();

    const ixSetComputeUniteLimit = ComputeBudgetProgram.setComputeUnitLimit({
      units: COMPUTE_UNITS,
    });

    const provider = this.driftClient.provider;
    const connection = this.driftClient.connection;
    const latestBlockhash = await connection.getLatestBlockhash('confirmed');
    const t5 = (new Date()).getTime();
    logger.debug(`t1 -> t2: ${t2 - t1} ms, t2 -> t3: ${t3 - t2} ms, t3 -> t4: ${t4 - t3} ms, t4 -> t5: ${t5 - t4} ms`);
    const message = new TransactionMessage({
      payerKey: provider.publicKey,
      recentBlockhash: latestBlockhash.blockhash,
      instructions: [ixSetComputeUniteLimit, ix]
    }).compileToV0Message();

    const transaction = new VersionedTransaction(message);
    transaction.sign([(provider.wallet as Wallet).payer]);

    logger.info(`${tradeIdx} - Sending transaction...`);

    if (!
      this.dryRun
    ) {
      const sig = await connection.sendTransaction(transaction, { maxRetries: 2 });

      logger.info(`${tradeIdx} - Transaction sent, pending confirmation, sig ${sig}`);

      const confirmation = await connection.confirmTransaction({
        signature: sig,
        blockhash: latestBlockhash.blockhash,
        lastValidBlockHeight: latestBlockhash.lastValidBlockHeight
      }, 'confirmed');

      if (confirmation.value && confirmation.value.err) {
        throw confirmation.value.err;
      }

      this.driftClient.perpMarketLastSlotCache.set(orderParams.marketIndex, confirmation.context.slot);
      return sig;
    } else {
      logger.info(`${tradeIdx} - Transaction sent, pending confirmation`);
      return "dryrun";
    }
  }

  private async executeTakeAction(action: ActionTake, tradeIdx: number):
    Promise<TransactionSignature> {

    const t1 = (new Date()).getTime();

    const orderParams = getMarketOrderParams({
      direction: action.direction,
      baseAssetAmount: action.baseAssetAmount,
      reduceOnly: false,
      marketIndex: action.marketIndex,
    });

    const ix = await this.driftClient.getPlacePerpOrderIx(orderParams);

    const t2 = (new Date()).getTime();

    const ixSetComputeUniteLimit = ComputeBudgetProgram.setComputeUnitLimit({
      units: COMPUTE_UNITS,
    });

    const provider = this.driftClient.provider;
    const connection = this.driftClient.connection;
    const latestBlockhash = await connection.getLatestBlockhash('confirmed');
    const t3 = (new Date()).getTime();
    logger.debug(`t1 -> t2: ${t2 - t1} ms, t2 -> t3: ${t3 - t2} ms`);
    const message = new TransactionMessage({
      payerKey: provider.publicKey,
      recentBlockhash: latestBlockhash.blockhash,
      instructions: [ixSetComputeUniteLimit, ix]
    }).compileToV0Message();

    const transaction = new VersionedTransaction(message);
    transaction.sign([(provider.wallet as Wallet).payer]);

    logger.info(`${tradeIdx} - Sending Take transaction...`);

    if (!this.dryRun) {
      const sig = await connection.sendTransaction(transaction, { maxRetries: 2 });

      logger.info(`${tradeIdx} - Transaction sent, pending confirmation, sig ${sig}`);

      const confirmation = await connection.confirmTransaction({
        signature: sig,
        blockhash: latestBlockhash.blockhash,
        lastValidBlockHeight: latestBlockhash.lastValidBlockHeight
      }, 'confirmed');

      if (confirmation.value && confirmation.value.err) {
        throw confirmation.value.err;
      }

      this.driftClient.perpMarketLastSlotCache.set(orderParams.marketIndex, confirmation.context.slot);
      return sig;
    } else {
      logger.info(`${tradeIdx} - Transaction sent, pending confirmation`);
      return "dryrun";
    }
  }

  private async processKucoin(marketIndex: number) {
    if (this.coolingPeriod[marketIndex].kucoin < new Date().getTime()) {
      await this.checkKucoinOpenOrders(marketIndex);
    }
    if (this.coolingPeriod[marketIndex].kucoin < new Date().getTime()) {
      await this.hedgeWithKucoin(marketIndex);
    }
  }

  private async process(market: PerpMarketAccount) {

    this.processKucoin(market.marketIndex).catch(e => logger.error(e));

    if (this.coolingPeriod[market.marketIndex].jit < new Date().getTime()) {
      this.checkForAuction(market).catch(e => logger.error(e));
    }
    this.checkAndUpdateLimitOrders(market.marketIndex).catch(e => logger.error(`Error updating limit orders: ${e}`));
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

        if (this.isActive) {

          await this.updateAgentState();

          await Promise.all(
            // TODO: spot
            this.driftClient.getPerpMarketAccounts().map((marketAccount) => {
              if (this.marketEnabled.includes(marketAccount.marketIndex)) {
                this.process(marketAccount);
              }
            })
          );
        } else {
          let allContractReady = true;
          for (const marketId of this.marketEnabled) {
            if (!this.futureKucoinContract[marketId] || !this.bookKucoin[marketId]) {
              allContractReady = false;
              break;
            }
          }
          if (this.allInitDone && allContractReady) {
            this.isActive = true;
            logger.info(`Bot is now active`);
          }
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

  private marketDataCallback(obs: ObservableResult<Attributes>) {
    if (this.isActive) {
      for (const marketIndex of this.marketEnabled) {
        const name = INDEX_TO_NAME[marketIndex];
        obs.observe(this.vwapKucoin[marketIndex].asks, { type: 'vwap', side: 'asks', exchange: 'kucoin', market: name });
        obs.observe(this.vwapKucoin[marketIndex].bids, { type: 'vwap', side: 'bids', exchange: 'kucoin', market: name });
        obs.observe(this.bookKucoin[marketIndex].bestBid(1), { type: 'best_price', side: 'bids', exchange: 'kucoin', market: name });
        obs.observe(this.bookKucoin[marketIndex].bestAsk(1), { type: 'best_price', side: 'asks', exchange: 'kucoin', market: name });
      }
    }
  }
}
