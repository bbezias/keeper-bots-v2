import {
  BASE_PRECISION,
  BN,
  calculateBidAskPrice,
  convertToNumber,
  DLOB,
  DLOBNode,
  DriftClient,
  EventMap,
  getOrderSignature,
  isVariant,
  MarketType,
  NewUserRecord,
  OptionalOrderParams,
  Order as DriftOrder,
  OrderRecord,
  OrderType,
  PerpMarketAccount,
  PerpPosition,
  PositionDirection,
  PostOnlyParams,
  PRICE_PRECISION,
  QUOTE_PRECISION,
  SlotSubscriber,
  SpotPosition,
  UserMap,
  UserStatsMap,
  Wallet,
  WrappedEvent,
  ZERO,
} from '@drift-labs/sdk';
import { E_ALREADY_LOCKED, Mutex, tryAcquire, withTimeout } from 'async-mutex';

import { ComputeBudgetProgram, PublicKey, TransactionMessage, TransactionSignature, VersionedTransaction } from '@solana/web3.js';

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { Bot } from '../types';

import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import { Attributes, Counter, Histogram, Meter, ObservableGauge, ObservableResult } from '@opentelemetry/api';
import { MeterProvider } from '@opentelemetry/sdk-metrics';
import { metricAttrFromUserAccount, RuntimeSpec } from '../metrics';
import { JitMakerConfig } from '../config';
import { KucoinController } from '../kucoin-api/kucoin';
import { Order as KucoinOrder, OrderBook } from '../kucoin-api/models';
import { PositionChangeOperationResponse } from '../kucoin-api/ws';
import { Contract } from '../kucoin-api/market';
import {
  calculateVolatility,
  calculateVWCP,
  INDEX_TO_LETTERS,
  INDEX_TO_NAME,
  INDEX_TO_SYMBOL,
  MaxSizeList,
  StateType,
  stateTypeToCode,
  SYMBOL_TO_INDEX
} from './utils';
import { DriftMarketData, KucoinMarketData } from './marketDataUtils';
import { convertPrice } from '../kucoin-api/utils';

type Action = {
  baseAssetAmount: BN;
  marketIndex: number;
  direction: PositionDirection;
  price: BN;
  node: DLOBNode;
};

type State = {
  driftState: Map<number, StateType>;
  driftStateCode: Map<number, number>;
  overallState: Map<number, StateType>;
  overallStateCode: Map<number, number>;
  spotMarketPosition: Map<number, SpotPosition>;
  perpMarketPosition: Map<number, PerpPosition>;
  kucoinMarketData: Map<number, KucoinMarketData>;
  driftMarketData: Map<number, DriftMarketData>;
  kucoinContract: Map<number, Contract>;
  positions: Map<number, { kucoin: number, drift: number }>;
  exchangeDeltaTime: Map<number, number>;
  deltaKucoinDrift: Map<number, MaxSizeList>;
  openOrders: Map<number, { kucoin: Map<string, KucoinOrder>, drift: Map<string, DriftOrder> }>;
  expectingOrderDataFrom: Map<number, string | undefined>;
  kucoinOpenOrderExposureList: Map<number, number>;
  isActive: Map<number, boolean>;
  volatility: Map<number, number>;
  centerOfMass: Map<number, number>;
  driftPositionValue: Map<number, number>;
  collateral: Map<number, number>;
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
  public readonly defaultIntervalMs: number = 2000;

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
  private modelGauge: ObservableGauge;
  private orderGauge: ObservableGauge;
  private readonly runtimeSpec: RuntimeSpec;
  private mutexBusyCounter: Counter;
  private errorCounter: Counter;
  private actionCounter: Counter;
  private tryJitDurationHistogram: Histogram;

  private watchdogTimerMutex = new Mutex();
  private watchdogTimerLastPatTime = Date.now();

  private readonly MAX_EXCHANGE_EXPOSURE;
  private readonly MAX_POSITION_EXPOSURE;
  private kucoin: KucoinController;

  private readonly mutexes: { [id: number]: { kucoin: Mutex, long: Mutex, short: Mutex, jit: Mutex } };
  private exchangeDelta: { [id: number]: number } = {};
  private readonly profitThreshold: { [id: number]: number };
  private readonly kucoinMakerFee: number;
  private maxTradeSize: { [id: number]: number } = {};
  private maxTradeSizeInLot: { [id: number]: number } = {};
  private coolingPeriod: { [id: number]: { jit: number, kucoin: number, long: number, short: number } } = {};
  private kucoinOpenOrderExposureList: { [id: number]: number } = {};
  private marketEnabled: number[] = [];
  private contractEnabled: string[] = [];
  private allInitDone = false;
  private tradeIndex = 0;
  private targetPrices: Map<number, { kucoin: { buy: number, sell: number }, drift: { buy: number, sell: number } }>; // Target Price

  // k1: volatility coefficient, k2: spread coefficient, k3: drift/kucoin coefficient, k4: risk appetite, k5: Maximum price drift, Weight of the model center point By VWAP
  private readonly modelCoefficients: { k1: number, k2: number, k3: number, k4: number, k5: number, k6: number, k7: number, k8: number };

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
    this.MAX_EXCHANGE_EXPOSURE = config.maxExchangeExposure;
    this.MAX_POSITION_EXPOSURE = config.maxPositionExposure;
    this.modelCoefficients = config.modelCoefficients;

    this.mutexes = {};
    this.targetPrices = new Map<number, { kucoin: { buy: number, sell: number }, drift: { buy: number, sell: number } }>();

    this.agentState = {
      driftState: new Map<number, StateType>(),
      driftStateCode: new Map<number, number>(),
      overallState: new Map<number, StateType>(),
      overallStateCode: new Map<number, number>(),
      spotMarketPosition: new Map<number, SpotPosition>(),
      perpMarketPosition: new Map<number, PerpPosition>(),
      openOrders: new Map<number, { kucoin: Map<string, KucoinOrder>, drift: Map<string, DriftOrder> }>(),
      expectingOrderDataFrom: new Map<number, string>(),
      kucoinOpenOrderExposureList: new Map<number, number>(),
      kucoinMarketData: new Map<number, KucoinMarketData>(),
      driftMarketData: new Map<number, DriftMarketData>(),
      positions: new Map<number, { kucoin: number, drift: number }>(),
      kucoinContract: new Map<number, Contract>(),
      exchangeDeltaTime: new Map<number, number>(),
      deltaKucoinDrift: new Map<number, MaxSizeList>(),
      isActive: new Map<number, boolean>(),
      volatility: new Map<number, number>(),
      centerOfMass: new Map<number, number>(),
      driftPositionValue: new Map<number, number>(),
      collateral: new Map<number, number>()
    };

    for (const i of [0, 1, 2, 3, 4, 5]) {
      const x = INDEX_TO_LETTERS[i];
      const name = INDEX_TO_NAME[i];
      if (!config.marketEnabled[x]) continue;
      this.marketEnabled.push(i);
      this.contractEnabled.push(INDEX_TO_SYMBOL[i]);
      this.profitThreshold[i] = config.profitThreshold[x];
      this.maxTradeSize[i] = config.maxTradeSize[x];
      this.kucoinOpenOrderExposureList[i] = 0;
      this.coolingPeriod[i] = { jit: 0, kucoin: 0, long: 0, short: 0 };
      this.exchangeDelta[i] = 0;
      this.mutexes[i] = { jit: new Mutex(), short: new Mutex(), long: new Mutex(), kucoin: new Mutex() };
      this.targetPrices.set(i, { kucoin: { buy: 0, sell: 0 }, drift: { buy: 0, sell: 0 } });
      this.agentState.deltaKucoinDrift.set(i, new MaxSizeList(20));
      this.agentState.exchangeDeltaTime.set(i, 0);
      this.agentState.positions.set(i, { kucoin: 0, drift: 0 });
      this.agentState.driftState.set(i, StateType.NOT_STARTED);
      this.agentState.driftStateCode.set(i, 0);
      this.agentState.openOrders.set(i, { kucoin: new Map(), drift: new Map() });
      this.agentState.expectingOrderDataFrom.set(i, undefined);
      this.agentState.kucoinOpenOrderExposureList.set(i, 0);
      this.agentState.isActive.set(i, false);
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

    this.actionCounter = this.meter.createCounter('open_orders', {
      description: 'Place order / Cancel order',
    });

    this.modelGauge = this.meter.createObservableGauge(
      'model_gauge',
      {
        description: "Model Gauge"
      }
    );

    this.marketDataGauge = this.meter.createObservableGauge(
      'market_data',
      {
        description: "All the market data"
      }
    );

    this.orderGauge = this.meter.createObservableGauge(
      'order_data',
      {
        description: "All the order data"
      }
    );

    this.marketDataGauge.addCallback(obs => this.marketDataCallback(obs));
    this.modelGauge.addCallback(obs => this.modelCallback(obs));
    this.orderGauge.addCallback(obs => this.orderCallback(obs));

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

  /*
    Retrieve list of open order at launch
   */
  private async updateKucoinOpenOrders(): Promise<boolean> {
    try {
      const p = await this.kucoin.api.orderList({ type: "limit", status: 'active' });

      for (const marketIndex of this.marketEnabled) {
        const symbol = INDEX_TO_SYMBOL[marketIndex];
        const orders = new Map<string, KucoinOrder>();
        for (const o of p.data.items) {
          if (o.symbol === symbol) {
            const modifiedO: KucoinOrder = {
              orderId: o.id, symbol: o.symbol, size: o.size.toPrecision(3), price: o.price,
              orderType: o.type, status: o.status, exchange: 'kucoin', matchSize: o.filledSize.toPrecision(3), timestamp: o.orderTime,
              side: o.side, matchLotSize: o.filledSize.toPrecision(3), lotSize: o.size.toPrecision(3)
            };
            orders.set(o.id, modifiedO);
          }
        }
        this.agentState.openOrders.get(marketIndex).kucoin = orders;
      }

      return true;
    } catch (error) {
      console.error(`Error fetching open orders`);
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

    // Get current open orders
    await this.updateKucoinOpenOrders();

    // Lot size
    const contractList = await this.kucoin.api.contractList();
    for (const x of contractList.data) {
      if (this.contractEnabled.includes(x.symbol)) {
        const marketIndex = SYMBOL_TO_INDEX[x.symbol];
        logger.info(`Multiplier for ${x.symbol}: ${x.multiplier}`);
        this.agentState.kucoinContract.set(marketIndex, x);
        this.maxTradeSizeInLot[marketIndex] = Math.trunc(this.maxTradeSize[marketIndex] / x.multiplier);

        // Prepare order book first
        const orderBookResponse = await this.kucoin.api.orderBook({ symbol: x.symbol, depth: 'depth20' });
        const asks: [number, number][] = orderBookResponse.data.asks.map(([price, volume]) => [parseFloat(price), volume]);
        const bids: [number, number][] = orderBookResponse.data.asks.map(([price, volume]) => [parseFloat(price), volume]);
        const ob = new OrderBook('kucoin', orderBookResponse.data.symbol, asks, bids);
        this.agentState.kucoinMarketData.set(marketIndex, new KucoinMarketData(ob));
        await sleep(500);
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
    await this.refreshDelta();
    await this.updateTargetPrice();

  }

  public async init(): Promise<void> {

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

    await Promise.all(initPromises);
    logger.info(`All init done`);

    // Start function running periodically
    this.intervalIds.push(setInterval(this.refreshDelta.bind(this), 2000));
    this.intervalIds.push(setInterval(this.updateTargetPrice.bind(this), 60000));
    this.intervalIds.push(setInterval(this.control.bind(this), 3500));

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

  /**
   * Update target price around which the open limit orders will be placed
   */
  private async updateTargetPrice() {
    for (const marketIndex of this.marketEnabled) {
      const symbol = INDEX_TO_SYMBOL[marketIndex];
      const from = new Date().getTime() - 1000 * 60 * this.modelCoefficients.k8;

      // Retrieve the past hour of data to calculate VWCP and Vol
      const r = await this.kucoin.api.kline({ granularity: 1, symbol, from });
      const vwcp = calculateVWCP(r);
      const volatility = calculateVolatility(r);
      this.agentState.volatility.set(marketIndex, volatility);

      // Center of mass if how far we want our price target to be from the vwcp relatives to the current price
      const centerOfMass = (vwcp * this.modelCoefficients.k6 + r.data[r.data.length - 1][4] * (1 - this.modelCoefficients.k6));
      this.agentState.centerOfMass.set(marketIndex, centerOfMass);

      // Adjust for Drift
      const centerOfMassDrift = centerOfMass / this.exchangeDelta[marketIndex];

      // Retrieving the exposure. We apply premium if we are currently exposed to one direction
      // Only apply to drift
      const driftPosition = Math.round(this.agentState.positions.get(marketIndex).drift);
      const kucoinPosition = this.agentState.positions.get(marketIndex).kucoin;
      const n = Math.trunc(driftPosition + kucoinPosition);
      const premiumBuyDrift = this.modelCoefficients.k3 * (1 + Math.max(n, 0) * this.modelCoefficients.k7);
      const premiumSellDrift = this.modelCoefficients.k3 * (1 - Math.min(n, 0) * this.modelCoefficients.k7);

      // Take a new position if the total position is different from 0

      const targets = { kucoin: { buy: 0, sell: 0 }, drift: { buy: 0, sell: 0 } };
      targets.kucoin.buy = centerOfMass - volatility * this.modelCoefficients.k1;
      targets.kucoin.sell = centerOfMass + volatility * this.modelCoefficients.k1;
      targets.drift.buy = centerOfMassDrift - volatility / this.modelCoefficients.k1 * premiumBuyDrift;
      targets.drift.sell = centerOfMassDrift + volatility / this.modelCoefficients.k1 * premiumSellDrift;
      this.targetPrices.set(marketIndex, targets);
      logger.info(`Target price updated`);
      logger.info(`Kucoin: ${targets.kucoin.buy} / ${targets.kucoin.sell}`);
      logger.info(`Drift: ${targets.drift.buy} / ${targets.drift.sell}`);
      logger.debug(`Center of mass: ${centerOfMass} / volatility: ${volatility} / n ${n}`);
      logger.debug(`Center of mass Drift: ${centerOfMassDrift} / premium Drift: ${premiumBuyDrift.toPrecision(5)} ${premiumSellDrift.toPrecision(5)}/ n ${n}`);
    }
  }

  /**
   * Update agent state with upcoming kucoin order book
   */
  private updateKucoinMarketData(r: any) {
    const marketIndex = SYMBOL_TO_INDEX[r.data.symbol];
    if (this.marketEnabled.includes(marketIndex)) {
      this.agentState.kucoinMarketData.set(marketIndex, new KucoinMarketData(r.data));
    }
  }

  /**
   * Update agent state with upcoming drift order book
   */
  private updateDriftMarketData(marketIndex: number) {
    if (this.marketEnabled.includes(marketIndex)) {
      const newData = new DriftMarketData(marketIndex, this.dlob, this.driftClient, this.slotSubscriber.getSlot());
      this.agentState.driftMarketData.set(marketIndex, newData);
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
        const driftPosition = baseValue / multiplier;

        this.agentState.driftPositionValue.set(p.marketIndex, Math.abs(positionValue));
        this.agentState.collateral.set(p.marketIndex, accountCollateral);
        this.agentState.positions.get(p.marketIndex).drift = driftPosition;

        const exposure = Math.abs(positionValue / accountCollateral);
        logger.info(`${name} - Drift Position has changed from ${previousBaseValue.toPrecision(4)} to ${baseValue.toPrecision(4)} - New Exposure ${(exposure * 100).toPrecision(2)}`);

        // update current position based on market position
        this.agentState.perpMarketPosition.set(p.marketIndex, p);

        // update Drift state
        let driftState = this.agentState.driftState.get(p.marketIndex);
        if (!driftState) driftState = StateType.NOT_STARTED;
        if (exposure >= this.MAX_EXCHANGE_EXPOSURE && p.baseAssetAmount.gt(new BN(0))) {
          driftState = StateType.CLOSING_LONG;
        } else if (exposure >= this.MAX_EXCHANGE_EXPOSURE && p.baseAssetAmount.lt(new BN(0))) {
          driftState = StateType.CLOSING_SHORT;
        } else if (p.baseAssetAmount.gt(new BN(0))) {
          driftState = StateType.LONG;
        } else if (p.baseAssetAmount.lt(new BN(0))) {
          driftState = StateType.SHORT;
        } else {
          driftState = StateType.NEUTRAL;
        }

        console.log('drift state', name, driftState);

        // Set the values
        this.agentState.driftState.set(i, driftState);
        this.agentState.driftStateCode.set(i, stateTypeToCode[driftState]);
      }

      for (const i of this.marketEnabled) {
        // Take a new position if the total position is different from 0
        const multiplier = this.agentState.kucoinContract.get(i).multiplier;
        const kucoinPosition = this.agentState.positions.get(i).kucoin;
        const driftPosition = this.agentState.positions.get(i).drift;
        const overallPosition = (driftPosition + kucoinPosition) * multiplier * this.agentState.kucoinMarketData.get(i).bestPrice.bid;
        let overallState = this.agentState.overallState.get(i);

        if (!overallState) overallState = StateType.NOT_STARTED;
        if ((overallState === StateType.CLOSING_LONG && overallPosition > 0) || (overallPosition >= this.MAX_POSITION_EXPOSURE)) {
          overallState = StateType.CLOSING_LONG;
        } else if ((overallState === StateType.CLOSING_SHORT && overallPosition < 0) || (overallPosition <= -1 * this.MAX_POSITION_EXPOSURE)) {
          overallState = StateType.CLOSING_SHORT;
        } else if (overallPosition > 0)
        {
          overallState = StateType.LONG;
        } else if (overallPosition < 0) {
          overallState = StateType.SHORT;
        } else {
          overallState = StateType.NEUTRAL;
        }

        console.log('global state', i, overallState);

        // Set the values
        this.agentState.overallState.set(i, overallState);
        this.agentState.overallStateCode.set(i, stateTypeToCode[overallState]);
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

        if (!isVariant(o.orderType, 'limit') || o.baseAssetAmountFilled.gte(o.baseAssetAmount)) return;

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
    return this.targetPrices.get(marketIndex).kucoin[side];
  }

  /**
   * Provide the Drift price target to place limit order
   */
  private getDriftPriceTarget(marketIndex: number, side: 'buy' | 'sell'): number {
    return this.targetPrices.get(marketIndex).drift[side];
  }

  /**
   * Provide the Kucoin price target to place limit order
   */
  private isPriceCloseToTargetKucoin(marketIndex: number, order: KucoinOrder): boolean {
    const target = this.getKucoinPriceTarget(marketIndex, order.side);
    const maxDelta = target * this.modelCoefficients.k5;
    return order.side === 'buy' ? target - +order.price < maxDelta : +order.price - target < maxDelta;
  }

  /**
   * Provide the Drift price target to place limit order
   */
  private isPriceCloseToTargetDrift(marketIndex: number, order: DriftOrder): boolean {
    const side = isVariant(order.direction, 'long') ? 'buy' : 'sell';
    const target = this.getDriftPriceTarget(marketIndex, side);
    const maxDelta = target * this.modelCoefficients.k5;
    const price = convertToNumber(order.price, PRICE_PRECISION);
    return side === 'buy' ? target - price < maxDelta : price - target < maxDelta;
  }

  /**
   * Cancel Order
   */
  private cancelKucoinOrder(marketIndex: number, orderId: string, side: 'buy' | 'sell'): void {
    const now = (new Date()).getTime();
    this.coolingPeriod[marketIndex].kucoin = now + 3000;
    const name = INDEX_TO_NAME[marketIndex];
    this.actionCounter.add(1, { type: 'cancel', side, exchange: 'kucoin', market: name });
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
    const name = INDEX_TO_NAME[marketIndex];
    const clientOid = `${symbol}-${(new Date()).getTime()}`;
    this.coolingPeriod[marketIndex].kucoin = now + 3000;
    this.actionCounter.add(1, { type: 'place', side, exchange: 'kucoin', market: name });
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

        const [orderId, order] = orderList.entries().next().value;
        // There should always be only one Kucoin order at the same time. If there is more than one. Cancel one
        logger.info(`Cancel order ${orderId} because too many positions are opened`);
        this.cancelKucoinOrder(marketIndex, orderId, order.side);
      } else if (Math.trunc(Math.abs(delta)) > 0) {

        // If position smaller than 0.5 do not update the position as you wont be able to take the position on kucoin
        const orderList = this.agentState.openOrders.get(marketIndex).kucoin;
        if (orderList.size === 0) {
          const side = delta > 0 ? 'sell' : 'buy';
          let price = this.getKucoinPriceTarget(marketIndex, side);

          // Make sure to not post an order that can be filled immediately
          const marketData = this.agentState.kucoinMarketData.get(marketIndex);
          price = side === 'buy' ? Math.min(price, marketData.bestPrice.bid) : Math.max(price, marketData.bestPrice.ask);

          // Convert price into a valid Kucoin price
          const tickSize = this.agentState.kucoinContract.get(marketIndex).tickSize;
          price = convertPrice(price, tickSize);

          const size = Math.trunc(Math.abs(delta));

          logger.info(`Hedging ${symbol} Open position, taking order ${side} ${Math.abs(delta)}`);
          this.placeKucoinOrder(marketIndex, side, symbol, price, size);

        } else {

          const [orderId, order] = orderList.entries().next().value;
          // If the exposure is non-zero, cancel the order so that a new one can be bplaced
          logger.debug(`Cancel order ${orderId} due to exposure change`);
          this.cancelKucoinOrder(marketIndex, orderId, order.side);
        }
      } else if (orderList.size === 1) {

        const [orderId, order] = orderList.entries().next().value;

        if (!this.isPriceCloseToTargetKucoin(marketIndex, order)) {

          // Verify that the current price is not too far from the open order price otherwise cancel
          logger.debug(`Cancel order ${orderId} due to position change`);
          this.cancelKucoinOrder(marketIndex, orderId, order.side);
        }
      }
    } finally {
      release();
    }
  }

  /**
   * Place limit order
   */
  private async placeDriftOrder(orderParams: OptionalOrderParams) {
    const name = INDEX_TO_NAME[orderParams.marketIndex];
    this.actionCounter.add(1, {
      type: 'place',
      side: isVariant(orderParams.direction, 'long') ? 'buy' : 'sell',
      exchange: 'drift',
      market: name
    });
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
        this.coolingPeriod[orderParams.marketIndex].long = new Date().getTime() + 4000;
      } else {
        this.coolingPeriod[orderParams.marketIndex].short = new Date().getTime() + 4000;
      }
    } catch (e) {
      logger.error(`${name} Error placing a long order`);
      logger.error(e);
    }
  }

  /**
   * Cancel limit order
   */
  private async cancelDriftOrder(orderId: number, marketIndex: number, isLong: boolean) {
    const name = INDEX_TO_NAME[marketIndex];
    try {
      this.actionCounter.add(1, { type: 'cancel', side: isLong ? 'buy' : 'sell', exchange: 'drift', market: name });
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
        this.coolingPeriod[marketIndex].long = new Date().getTime() + 4000;
      } else {
        this.coolingPeriod[marketIndex].short = new Date().getTime() + 4000;
      }
    } catch (e) {
      logger.error(`${name} Error cancelling an long order ${orderId}`);
    }
  }

  /**
   * Maintain the open order to 1 per side and close to the target price
   */
  private async processDriftBySide(
    openOrders: DriftOrder[],
    marketIndex: number,
    direction: 'long' | 'short',
    currentDriftState: StateType,
    currentOverallState: StateType
  ) {

    const name = INDEX_TO_NAME[marketIndex];
    const isLong = direction === 'long';
    const closingShort = currentOverallState === StateType.CLOSING_SHORT || currentDriftState === StateType.CLOSING_SHORT;
    const closingLong = currentOverallState === StateType.CLOSING_LONG || currentDriftState === StateType.CLOSING_LONG;

    const mutex = isLong ? this.mutexes[marketIndex].long : this.mutexes[marketIndex].short;
    const coolingPeriod = isLong ? this.coolingPeriod[marketIndex].long : this.coolingPeriod[marketIndex].short;
    if (mutex.isLocked() || coolingPeriod > new Date().getTime()) {
      return;
    }
    const release = await mutex.acquire();
    try {

      const orders = openOrders.filter(item => isVariant(item.direction, direction));
      if (orders.length > 1) {

        // Only 1 open order allowed
        logger.info(`${name} - ${isLong ? "long" : "short"} - Closing order because there are too many`);
        await this.cancelDriftOrder(orders[0].orderId, marketIndex, isLong);
      } else if (orders.length === 1 && !isLong && closingShort) {

        // Short open position not allowed if state is closing short
        logger.info(`${name} - ${isLong ? "long" : "short"} - Closing short order because we are in closing short mode`);
        await this.cancelDriftOrder(orders[0].orderId, marketIndex, isLong);
      } else if (orders.length === 1 && isLong && closingLong) {

        // Long open position not allowed if state is closing long
        logger.info(`${name} - ${isLong ? "long" : "short"} -  Closing long order because we are in closing long mode`);
        await this.cancelDriftOrder(orders[0].orderId, marketIndex, isLong);
      } else if (orders.length === 1) {

        // Verify that the open order is close enough to the target price otherwise cancel it
        const order = orders.values().next().value;
        if (!this.isPriceCloseToTargetDrift(marketIndex, order)) {

          logger.info(`${name} - ${isLong ? "long" : "short"} -  Price is now too far from target drift price`);
          await this.cancelDriftOrder(orders[0].orderId, marketIndex, isLong);
        }
      } else {

        if ((closingLong && isLong) || (closingShort && !isLong)) {
          return;
        }

        // Get the target price and place limit order
        let targetPrice = this.getDriftPriceTarget(marketIndex, isLong ? 'buy' : 'sell');

        // Make sure to not post an order that can be filled immediately
        const marketData = this.agentState.driftMarketData.get(marketIndex);
        targetPrice = isLong ? Math.min(targetPrice, marketData.bestPrice.bid) : Math.max(targetPrice, marketData.bestPrice.ask);

        const targetPriceBn = new BN(targetPrice * PRICE_PRECISION.toNumber());
        const size = new BN(this.maxTradeSize[marketIndex] * BASE_PRECISION.toNumber());

        logger.info(`${name} 📝 Placing limit price of ${isLong ? "long" : "short"} order $${targetPrice}`);

        await this.placeDriftOrder({
          marketIndex: marketIndex,
          orderType: OrderType.LIMIT,
          direction: isLong ? PositionDirection.LONG : PositionDirection.SHORT,
          baseAssetAmount: size,
          price: targetPriceBn,
          postOnly: PostOnlyParams.MUST_POST_ONLY,
        });
      }

    } catch (e) {
      logger.error(`Uncaught error in updateLimit: ${e}`);
      console.log(e);
    } finally {
      release();
    }

  }

  /**
   * Processing the limit orders
   */
  private async processDrift(marketIndex: number) {

    const openOrderMap = this.agentState.openOrders.get(marketIndex).drift ?? new Map<string, DriftOrder>();
    const currentDriftState = this.agentState.driftState.get(marketIndex);
    const currentOverallState = this.agentState.overallState.get(marketIndex);
    const openOrders = [...openOrderMap.values()];

    await Promise.all([
      this.processDriftBySide(openOrders, marketIndex, 'long', currentDriftState, currentOverallState),
      this.processDriftBySide(openOrders, marketIndex, 'short', currentDriftState, currentOverallState)
    ]);
  }

  /*
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

        const orderId = nodeToFill.node.order.orderId;
        const userAccount = nodeToFill.node.userAccount.toBase58();

        const startPrice = convertToNumber(
          nodeToFill.node.order.auctionStartPrice,
          PRICE_PRECISION
        );
        const endPrice = convertToNumber(
          nodeToFill.node.order.auctionEndPrice,
          PRICE_PRECISION
        );

        const kucoinMarketData = this.agentState.kucoinMarketData.get(marketIndex);

        const kucoinBook = kucoinMarketData.book;

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

        // Verify that there is no problem with the auction prices
        if (endPrice < 0 || startPrice < 0) continue;

        // Verify that we are not processing the auction too late
        const aucDur = nodeToFill.node.order.auctionDuration;
        const orderSlot = nodeToFill.node.order.slot.toNumber();
        const currSlot = this.slotSubscriber.getSlot();
        const aucEnd = orderSlot + aucDur;
        if (currSlot >= aucEnd) {
          logger.warn(`${orderId} - Processing error too late delta between curr slot and aucEnd: ${currSlot - aucEnd}`);
          continue;
        }

        // Skip if auction is long and state is reduce_long or auction is short and state is reduce_short
        const orderDirection = nodeToFill.node.order.direction;
        const jitMakerDirection = isVariant(orderDirection, 'long')
          ? PositionDirection.SHORT
          : PositionDirection.LONG;

        const isLong = isVariant(orderDirection, 'short');

        const currentState = this.agentState.driftState.get(marketIndex);
        if (currentState === StateType.CLOSING_LONG && isLong) {
          continue;
        }
        if (currentState === StateType.CLOSING_SHORT && !isLong) {
          continue;
        }

        // Get the amount to be filled
        const baseAmountToBeFilled = nodeToFill.node.order.baseAssetAmount.sub(
          nodeToFill.node.order.baseAssetAmountFilled
        );
        const amountAvailable = convertToNumber(baseAmountToBeFilled, BASE_PRECISION);
        const amount = Math.min(amountAvailable, this.maxTradeSize[marketIndex]);
        const jitMakerBaseAssetAmount = new BN(amount * BASE_PRECISION.toNumber());

        if (amount <= 0) {
          continue;
        }

        const amountToFill = convertToNumber(nodeToFill.node.order.baseAssetAmount, BASE_PRECISION);
        const amountFilled = convertToNumber(nodeToFill.node.order.baseAssetAmountFilled, BASE_PRECISION);

        const targetPrice = this.getDriftPriceTarget(marketIndex, isLong ? 'buy' : 'sell');

        if ((isLong && targetPrice > startPrice * 3) || (!isLong && targetPrice < startPrice * 3)) {
          continue;
        }

        logger.info(`${orderId} - New Auction found on ${INDEX_TO_NAME[market.marketIndex]} : ${isLong ? "⬆️️" : "⬇️"}️ start price: ${startPrice.toFixed(4)} slot: ${currSlot} / end price: ${endPrice.toFixed(4)} slot: ${aucEnd}`);
        logger.info(`${orderId} - Price target: ${targetPrice}`);
        logger.info(`${orderId} - Amount: ${amount} / Total to fill: ${amountToFill} / Already filled: ${amountFilled}`);
        logger.info(`${orderId} - node slot: ${orderSlot}, cur slot: ${currSlot}`);
        logger.info(`${orderId} - quoting order for node: ${userAccount} - ${orderId}, orderBaseFilled: ${amountFilled}/${amountToFill}`);

        if ((isLong && targetPrice >= endPrice) || (!isLong && targetPrice <= endPrice)) {

          // If Drift Target price is better than endPrice then we consider bidding for it
          this.orderLastSeenBaseAmount.set(
            getOrderSignature(
              nodeToFill.node.order.orderId,
              nodeToFill.node.userAccount
            ),
            nodeToFill.node.order.baseAssetAmountFilled
          );

          // Do not bid for a price worse than startPrice
          const biddingPrice = isLong ? Math.min(targetPrice, startPrice) : Math.max(targetPrice, startPrice);

          logger.info(`${orderId} - Bidding this auction ${biddingPrice.toFixed(4)}`);
          const mutex = this.mutexes[marketIndex].jit;
          if (mutex.isLocked()) {
            logger.info(`${orderId} - ❌ Client is locked - skipping`);
            continue;
          }

          const release = await mutex.acquire();
          try {
            const biddingPriceBn = new BN(biddingPrice * PRICE_PRECISION.toNumber());
            this.tradeIndex += 1;
            const txSig = await this.executeAction({
              baseAssetAmount: jitMakerBaseAssetAmount,
              marketIndex: nodeToFill.node.order.marketIndex,
              direction: jitMakerDirection,
              price: biddingPriceBn,
              node: nodeToFill.node,
            }, this.tradeIndex);

            this.coolingPeriod[marketIndex].jit = new Date().getTime() + 6000;
            logger.info(`${orderId} - ✅ JIT auction submitted (account: ${userAccount}), Tx: ${txSig}`);
          } catch (e) {
            nodeToFill.node.haveFilled = false;

            // If we get an error that order does not exist, assume its been filled by somebody else and we
            // have received the history record yet
            const errorCode = getErrorCode(e);

            if (errorCode) {
              this.errorCounter.add(1, { errorCode: errorCode.toString() });

              if (errorCode === 6061) {
                logger.warn(`${orderId} - ❌ JIT auction (account: ${userAccount}: too late, offer dont exists anymore`);
              } else {
                console.log(e);
                logger.error(`${orderId} - ❌ Error (${errorCode}) filling JIT auction (account: ${userAccount})`);
              }
            } else {
              logger.error(`${orderId} - ❌ Error Other error while doing transaction`);
              console.log(e);
            }

            // console.error(error);
          } finally {
            release();
          }
        } else {
          logger.info(`${orderId} - ⛔ Skip offering, acceptable price ${targetPrice} while best price is ${endPrice}`);
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

  private async process(market: PerpMarketAccount) {

    // Process Kucoin orders
    if (this.coolingPeriod[market.marketIndex].kucoin < new Date().getTime()) {
      await this.hedgeWithKucoin(market.marketIndex);
    }

    // Process Drift new auctions
    if (this.coolingPeriod[market.marketIndex].jit < new Date().getTime()) {
      this.checkForAuction(market).catch(e => logger.error(e));
    }

    // Process Drift orders
    this.processDrift(market.marketIndex).catch(e => logger.error(`Error updating limit orders: ${e}`));
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

        // Update all Drift market data
        for (const marketIndex of this.marketEnabled) {
          try {
            this.updateDriftMarketData(marketIndex);
          } catch (e) {
            this.agentState.isActive.set(marketIndex, false);
          }
        }

        if (this.allInitDone) {

          await this.updateAgentState();

          await Promise.all(
            // TODO: spot
            this.driftClient.getPerpMarketAccounts().map((marketAccount) => {
              if (this.marketEnabled.includes(marketAccount.marketIndex) && this.agentState.isActive.get(marketAccount.marketIndex)) {
                this.process(marketAccount);
              }
            })
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

  /*
   Responsible to send market data to prometheus
   */
  private marketDataCallback(obs: ObservableResult<Attributes>) {
    for (const marketIndex of this.marketEnabled) {
      const name = INDEX_TO_NAME[marketIndex];

      // Add kucoin data
      const marketDataKucoin = this.agentState.kucoinMarketData.get(marketIndex);
      if (marketDataKucoin.bestPrice.bid > 0 && marketDataKucoin.bestPrice.ask > 0) {
        obs.observe(marketDataKucoin.vwap.ask, { type: 'vwap', side: 'asks', exchange: 'kucoin', market: name });
        obs.observe(marketDataKucoin.vwap.bid, { type: 'vwap', side: 'bids', exchange: 'kucoin', market: name });
        obs.observe(marketDataKucoin.bestPrice.bid, { type: 'best_price', side: 'bids', exchange: 'kucoin', market: name });
        obs.observe(marketDataKucoin.bestPrice.ask, { type: 'best_price', side: 'asks', exchange: 'kucoin', market: name });
        obs.observe(marketDataKucoin.mid, { type: 'mid', side: 'mid', exchange: 'kucoin', market: name });
      }

      // Add drift data
      const marketDataDrift = this.agentState.driftMarketData.get(marketIndex);
      if (marketDataDrift.bestPrice.bid > 0 && marketDataDrift.bestPrice.ask > 0) {
        obs.observe(marketDataDrift.bestPrice.bid, { type: 'best_price', side: 'bids', exchange: 'drift', market: name });
        obs.observe(marketDataDrift.bestPrice.ask, { type: 'best_price', side: 'asks', exchange: 'drift', market: name });
        obs.observe(marketDataDrift.mid, { type: 'mid', side: 'mid', exchange: 'drift', market: name });
        obs.observe(marketDataDrift.spread, { type: 'mid', side: 'mid', exchange: 'drift', market: name });
      }

      const volatility = this.agentState.volatility.get(marketIndex);
      if (volatility) {
        obs.observe(volatility, { type: 'volatility', side: '', exchange: 'kucoin', market: name });
      }

      const centerOfMass = this.agentState.centerOfMass.get(marketIndex);
      if (centerOfMass) {
        obs.observe(centerOfMass, { type: 'centerOfMass', side: '', exchange: 'kucoin', market: name });
      }

      obs.observe(this.exchangeDelta[marketIndex], { type: 'exchange_delta', side: '', exchange: '', market: name });
    }
  }

  /*
   Responsible to send model data to prometheus
   */
  private modelCallback(obs: ObservableResult<Attributes>) {

    for (const marketIndex of [0, 1, 2, 3, 5]) {
      const name = INDEX_TO_NAME[marketIndex];
      const isActive = this.agentState.isActive.get(marketIndex);
      obs.observe(isActive ? 1 : 0, { type: 'is_active', side: '', exchange: '', market: name });
    }

    for (const marketIndex of this.marketEnabled) {
      const name = INDEX_TO_NAME[marketIndex];
      const targetPrices = this.targetPrices.get(marketIndex);

      // Add kucoin data
      if (targetPrices.kucoin.buy > 0) {
        obs.observe(targetPrices.kucoin.buy, { type: 'target', side: 'bids', exchange: 'kucoin', market: name });
      }
      if (targetPrices.kucoin.sell > 0) {
        obs.observe(targetPrices.kucoin.sell, { type: 'target', side: 'asks', exchange: 'kucoin', market: name });
      }

      // Add drift data
      if (targetPrices.drift.buy > 0) {
        obs.observe(targetPrices.drift.buy, { type: 'target', side: 'bids', exchange: 'drift', market: name });
      }
      if (targetPrices.drift.sell > 0) {
        obs.observe(targetPrices.drift.sell, { type: 'target', side: 'asks', exchange: 'drift', market: name });
      }
    }
  }

  /*
   Responsible to send order data to prometheus
   */
  private orderCallback(obs: ObservableResult<Attributes>) {

    for (const marketIndex of this.marketEnabled) {
      const name = INDEX_TO_NAME[marketIndex];
      const positions = this.agentState.positions.get(marketIndex);
      const openOrders = this.agentState.openOrders.get(marketIndex);
      const driftState = this.agentState.driftStateCode.get(marketIndex);
      const overallState = this.agentState.overallStateCode.get(marketIndex);
      const collateral = this.agentState.collateral.get(marketIndex);
      const positionValue = this.agentState.driftPositionValue.get(marketIndex);

      // Add status code and everything related to exposure
      obs.observe(positions.drift, { type: 'position', side: '', exchange: 'drift', market: name });
      obs.observe(positions.kucoin, { type: 'position', side: '', exchange: 'kucoin', market: name });
      obs.observe(collateral, { type: 'collateral', side: '', exchange: 'drift', market: name });
      obs.observe(positionValue, { type: 'positionValue', side: '', exchange: 'drift', market: name });
      obs.observe(driftState, { type: 'state_type', side: '', exchange: 'drift', market: name });
      obs.observe(overallState, { type: 'state_type', side: '', exchange: 'overall', market: name });

      // Observe open order prices and volumes
      let openOrdersStats = {
        buyPrice: undefined,
        buyVolume: undefined,
        buyTime: undefined,
        sellPrice: undefined,
        sellVolume: undefined,
        sellTime: undefined
      };
      for (const x of openOrders.kucoin.values()) {
        if (x.side === 'sell') {
          openOrdersStats.sellPrice = +x.price;
          openOrdersStats.sellVolume = +x.size;
          openOrdersStats.sellTime = x.timestamp;
        } else {
          openOrdersStats.buyPrice = +x.price;
          openOrdersStats.buyVolume = +x.size;
          openOrdersStats.buyTime = x.timestamp;
        }
      }
      if (openOrdersStats.sellPrice !== undefined) obs.observe(openOrdersStats.sellPrice, {
        type: 'open_order_price',
        side: 'sell',
        exchange: 'kucoin',
        market: name
      });

      if (openOrdersStats.sellVolume !== undefined) obs.observe(openOrdersStats.sellVolume, {
        type: 'open_order_volume',
        side: 'sell',
        exchange: 'kucoin',
        market: name
      });

      if (openOrdersStats.sellTime !== undefined) obs.observe(openOrdersStats.sellTime, {
        type: 'open_order_time',
        side: 'sell',
        exchange: 'kucoin',
        market: name
      });

      if (openOrdersStats.buyPrice !== undefined) obs.observe(openOrdersStats.buyPrice, {
        type: 'open_order_price',
        side: 'buy',
        exchange: 'kucoin',
        market: name
      });

      if (openOrdersStats.buyPrice !== undefined) obs.observe(openOrdersStats.buyVolume, {
        type: 'open_order_volume',
        side: 'buy',
        exchange: 'kucoin',
        market: name
      });

      if (openOrdersStats.buyTime !== undefined) obs.observe(openOrdersStats.buyTime, {
        type: 'open_order_time',
        side: 'buy',
        exchange: 'kucoin',
        market: name
      });

      openOrdersStats = {
        buyPrice: undefined,
        buyVolume: undefined,
        buyTime: undefined,
        sellPrice: undefined,
        sellVolume: undefined,
        sellTime: undefined
      };
      for (const x of openOrders.drift.values()) {
        if (isVariant(x.direction, 'long')) {
          openOrdersStats.buyPrice = convertToNumber(x.price, PRICE_PRECISION);
          openOrdersStats.buyVolume = convertToNumber(x.baseAssetAmount, BASE_PRECISION);
        } else {
          openOrdersStats.sellPrice = convertToNumber(x.price, PRICE_PRECISION);
          openOrdersStats.sellVolume = convertToNumber(x.baseAssetAmount, BASE_PRECISION);
        }
      }
      if (openOrdersStats.sellPrice !== undefined) obs.observe(openOrdersStats.sellPrice, {
        type: 'open_order_price',
        side: 'sell',
        exchange: 'drift',
        market: name
      });

      if (openOrdersStats.sellVolume !== undefined) obs.observe(openOrdersStats.sellVolume, {
        type: 'open_order_volume',
        side: 'sell',
        exchange: 'drift',
        market: name
      });

      if (openOrdersStats.buyPrice !== undefined) obs.observe(openOrdersStats.buyPrice, {
        type: 'open_order_price',
        side: 'buy',
        exchange: 'drift',
        market: name
      });

      if (openOrdersStats.buyVolume !== undefined) obs.observe(openOrdersStats.buyVolume, {
        type: 'open_order_volume',
        side: 'buy',
        exchange: 'drift',
        market: name
      });


    }
  }

  /*
   Verify that all the data is in place and not too old, if not set is active to false
   */
  private control() {

    for (const marketIndex of this.marketEnabled) {
      const driftMarketData = this.agentState.driftMarketData.get(marketIndex);
      const kucoinMarketData = this.agentState.kucoinMarketData.get(marketIndex);
      const targetPrices = this.targetPrices.get(marketIndex);

      if (!this.allInitDone) {
        this.agentState.isActive.set(marketIndex, false);
        continue;
      }

      if (!driftMarketData || driftMarketData.bestPrice.bid === 0 || driftMarketData.bestPrice.ask === 0) {
        this.agentState.isActive.set(marketIndex, false);
        continue;
      }
      if (!kucoinMarketData || kucoinMarketData.bestPrice.bid === 0 || kucoinMarketData.bestPrice.ask === 0) {
        this.agentState.isActive.set(marketIndex, false);
        continue;
      }
      if (targetPrices.drift.sell <= 0 || targetPrices.drift.buy <= 0 || targetPrices.kucoin.sell <= 0 || targetPrices.kucoin.buy <= 0) {
        this.agentState.isActive.set(marketIndex, false);
        continue;
      }
      if (kucoinMarketData.book.datetime.getTime() < new Date().getTime() - 10000) {
        this.agentState.isActive.set(marketIndex, false);
        continue;
      }

      this.agentState.isActive.set(marketIndex, true);
    }
  }
}
