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
  MarketType, PostOnlyParams, calculateBidAskPrice, Wallet, ZERO, getMarketOrderParams, OptionalOrderParams, OraclePriceData,
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
import { GPTBotConfig, JitMakerConfig } from '../config';
import { KucoinController } from '../kucoin-api/kucoin';
import { Order as KucoinOrder, OrderBook } from '../kucoin-api/models';
import { PositionChangeOperationResponse } from '../kucoin-api/ws';
import { Contract } from '../kucoin-api/market';
import { INDEX_TO_SYMBOL, MaxSizeList, StateType } from './utils';
import { MarketMakerBot } from './GPTMarketMaker';

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
  exchangeDeltaTime: Map<number, number>;
  deltaKucoinDrift: Map<number, MaxSizeList>;
  openOrders: Map<number, Map<string, DriftOrder>>;
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
 * This bot is responsible for placing market making trades to various parameters
 *
 */
export class GPTMaker implements Bot {
  public readonly name: string;
  public readonly dryRun: boolean;
  public readonly defaultIntervalMs: number = 500;

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
  private intervalIds: Array<NodeJS.Timer> = [];

  private agentState: State;

  private watchdogTimerMutex = new Mutex();
  private watchdogTimerLastPatTime = Date.now();

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

  /**
   * if a position's notional value passes this percentage of account
   * collateral, the position enters a CLOSING_* state.
   */
  private readonly MAX_POSITION_EXPOSURE;

  private kucoin: KucoinController;
  private coolingPeriod: { [id: number]: number } = {};
  private marketEnabled: number[] = [];
  private contractEnabled: string[] = [];
  private isActive = false;
  private allInitDone = false;
  private tradeIndex = 0;
  private bots = new Map<number, MarketMakerBot>();

  constructor(
    clearingHouse: DriftClient,
    slotSubscriber: SlotSubscriber,
    runtimeSpec: RuntimeSpec,
    config: GPTBotConfig
  ) {
    this.name = config.botId;
    this.dryRun = config.dryRun;
    this.driftClient = clearingHouse;
    this.slotSubscriber = slotSubscriber;
    this.runtimeSpec = runtimeSpec;
    this.MAX_POSITION_EXPOSURE = config.maxPositionExposure;

    if (config.marketEnabled.sol) {
      this.marketEnabled.push(0);
      this.contractEnabled.push(INDEX_TO_SYMBOL[0]);
      logger.info("SOLANA enabled");
    }
    if (config.marketEnabled.btc) {
      this.marketEnabled.push(1);
      this.contractEnabled.push(INDEX_TO_SYMBOL[1]);
      logger.info("BITCOIN enabled");
    }
    if (config.marketEnabled.eth) {
      this.marketEnabled.push(2);
      this.contractEnabled.push(INDEX_TO_SYMBOL[2]);
      logger.info("ETHEREUM enabled");
    }
    if (config.marketEnabled.apt) {
      this.marketEnabled.push(3);
      this.contractEnabled.push(INDEX_TO_SYMBOL[3]);
      logger.info("APTOS enabled");
    }
    if (config.marketEnabled.matic) {
      this.marketEnabled.push(5);
      this.contractEnabled.push(INDEX_TO_SYMBOL[5]);
      logger.info("MATIC enabled");
    }

    this.agentState = {
      kucoinMarketData: new Map<number, KucoinMarketData>(),
      stateType: new Map<number, StateType>(),
      spotMarketPosition: new Map<number, SpotPosition>(),
      perpMarketPosition: new Map<number, PerpPosition>(),
      openOrders: new Map<number, Map<string, DriftOrder>>(),
      exchangeDeltaTime: new Map<number, number>(),
      deltaKucoinDrift: new Map<number, MaxSizeList>()
    };

    for (const marketIndex of this.marketEnabled) {
      this.agentState.deltaKucoinDrift.set(marketIndex, new MaxSizeList(20));
      this.agentState.exchangeDeltaTime.set(marketIndex, 0);
      this.agentState.stateType.set(marketIndex, StateType.NOT_STARTED);
      this.agentState.openOrders.set(marketIndex, new Map());
    }

    this.metricsPort = config.metricsPort;
    if (this.metricsPort) {
      this.initializeMetrics();
    }

    for (const marketIndex of this.marketEnabled) {
      this.bots.set(marketIndex, new MarketMakerBot({
        name: "GPTBot",
        marketIndex,
        meter: this.meter,
        maxExposure: config.maxPositionExposure,
        k1: config.k1,
        k2: config.k2,
        k3: config.k3,
        k4: config.k4,
        k5: config.k5,
        k6: config.k6
      }));
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

    this.runtimeSpecsGauge.addCallback((obs) => {
      obs.observe(this.bootTimeMs, this.runtimeSpec);
    });
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
      this.kucoin.subscribe(contract, false, false, false, false).then(() => {
        logger.info(`✅ Websocket ${contract} subscribed with kucoin`);
      });
    }

    this.kucoin.on('book', this.updateKucoinMarketData.bind(this));
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

    initPromises.push(this.userMap.fetchAllUsers());

    await Promise.all(initPromises);
    logger.info(`All init done`);
    this.allInitDone = true;
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

  public viewDlob(): DLOB {
    return this.dlob;
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
      const perpPositions: { [id: number]: undefined | PerpPosition } = {};

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

  private async placeOrder(orderParams: OptionalOrderParams) {
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
        this.coolingLimitLongPeriod[orderParams.marketIndex] = new Date().getTime() + 2000;
      } else {
        this.coolingLimitShortPeriod[orderParams.marketIndex] = new Date().getTime() + 2000;
      }
    } catch (e) {
      logger.error(`${name} Error placing a long order`);
      logger.error(e);
    }
  }

  private async cancelOrder(orderId: number, marketIndex: number, isLong: boolean) {
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
        this.coolingLimitLongPeriod[marketIndex] = new Date().getTime() + 2000;
      } else {
        this.coolingLimitShortPeriod[marketIndex] = new Date().getTime() + 2000;
      }
    } catch (e) {
      logger.error(`${name} Error cancelling an long order ${orderId}`);
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

  private async tryMakeJitAuctionForMarket(market: PerpMarketAccount) {
    if (this.coolingPeriod[market.marketIndex] < new Date().getTime()) {
      this.checkForAuction(market).catch(e => logger.error(e));
    }
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
          await this.hedgeWithKucoin();
          await this.updateKucoinOrders();

          await Promise.all(
            // TODO: spot
            this.driftClient.getPerpMarketAccounts().map((marketAccount) => {
              if (this.marketEnabled.includes(marketAccount.marketIndex)) {
                this.tryMakeJitAuctionForMarket(marketAccount);
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
