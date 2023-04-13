import {
  DriftClient,
  SlotSubscriber,
  BASE_PRECISION,
  convertToNumber,
  ZERO,
  PerpPosition as DriftPerpPosition
} from '@drift-labs/sdk';
import { Mutex, tryAcquire, E_ALREADY_LOCKED } from 'async-mutex';

import { logger } from '../logger';
import { Bot } from '../types';
import { RuntimeSpec, metricAttrFromUserAccount } from '../metrics';
import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import { Counter, Meter, ObservableGauge } from '@opentelemetry/api';
import fs from 'fs';
import {
  MeterProvider,
} from '@opentelemetry/sdk-metrics-base';
import { HedgingConfig } from '../config';
import { BatchObservableResult } from '@opentelemetry/api-metrics';
import { INDEX_TO_LETTERS, INDEX_TO_MANGO, INDEX_TO_NAME, MaxSizeList } from './utils';
import {
  MANGO_V4_ID,
  MangoAccount,
  MangoClient,
  Group,
  PerpOrder,
  PerpOrderSide,
  BookSide
} from '@blockworks-foundation/mango-v4';
import { PublicKey } from '@solana/web3.js';
import { getDecimalCount } from './utils';

type State = {
  hedgeBasePosition: Map<number, number>;
  hedgeQuotePosition: Map<number, number>;
  positionList: Map<number, MaxSizeList>;
  openOrders: Map<number, PerpOrder[]>;

  targetHedge: Map<number, number>;
  unsettledPnL: Map<number, number>;
  asks: Map<number, BookSide>;
  bids: Map<number, BookSide>;
  price: Map<number, number>;
  balances: Map<string, number>;
  balancesUsd: Map<string, number>
};

type LevelParams = { [marketIndex: number]: { acceptableMargin: number } }
type Config = { levels: LevelParams };

enum METRIC_TYPES {
  runtime_specs = 'runtime_specs',
  mutex_busy = 'mutex_busy',
  errors = 'errors',
  unrealized_pnl = 'unrealized_pnl',
  balance = 'balance',
  perp_position_base = 'perp_position_base',
  perp_position_quote = 'perp_position_quote',
}

/**
 *
 * This bot is responsible for placing limit orders that rest on the DLOB.
 * limit price offsets are used to automatically shift the orders with the
 * oracle price, making order updating automatic.
 *
 */
export class HedgingBot implements Bot {
  public readonly name: string;
  public readonly dryRun: boolean;
  public readonly defaultIntervalMs: number;

  private readonly driftClient: DriftClient;
  private slotSubscriber: SlotSubscriber;
  private periodicTaskMutex = new Mutex();

  private intervalIds: Array<NodeJS.Timer> = [];

  // metrics
  private metricsInitialized = false;
  private readonly metricsPort: number | undefined;
  private exporter: PrometheusExporter;
  private meter: Meter;
  private bootTimeMs = Date.now();
  private runtimeSpecsGauge: ObservableGauge;
  private balanceGauge: ObservableGauge;
  private perpPositionBase: ObservableGauge;
  private perpPositionQuote: ObservableGauge;
  private unrealizedPnL: ObservableGauge;
  private runtimeSpec: RuntimeSpec;
  private mutexBusyCounter: Counter;
  private errorCounter: Counter;
  private levels: LevelParams;

  private mangoAccount: MangoAccount;

  private agentState: State;
  private marketEnabled: number[] = [];

  private GROUP = new PublicKey('78b8f4cGCwmZ9ysPFMWLaLTkkaYnUjwMJYStWe5RTSSX');
  private readonly mangoKey: PublicKey;
  private readonly mangoClient: MangoClient;
  private mangoGroup: Group;
  private readonly positionCountForAverage: number;
  private decimalPrice: Map<number, number>;

  private watchdogTimerMutex = new Mutex();
  private watchdogTimerLastPatTime = Date.now();

  constructor(
    clearingHouse: DriftClient,
    slotSubscriber: SlotSubscriber,
    runtimeSpec: RuntimeSpec,
    config: HedgingConfig
  ) {
    this.name = config.botId;
    this.dryRun = config.dryRun;
    this.defaultIntervalMs = config.defaultIntervalMs;
    this.positionCountForAverage = Math.trunc(config.timeWindowMinutes * 60 / (this.defaultIntervalMs / 1000));

    logger.info(`Mango key: ${config.mangoKey}`);
    this.mangoKey = new PublicKey(config.mangoKey);

    this.driftClient = clearingHouse;
    this.slotSubscriber = slotSubscriber;

    this.mangoClient = MangoClient.connect(clearingHouse.provider, 'mainnet-beta', MANGO_V4_ID['mainnet-beta'], {
    idsSource: 'get-program-accounts',
    postSendTxCallback: ({ txid }: { txid: string }) => {
      console.log('Transaction sent', txid);
    }});
    this.decimalPrice = new Map();

    for (const i of [0, 1, 2, 3, 4, 5, 6, 7]) {
      const x = INDEX_TO_LETTERS[i];
      const name = INDEX_TO_NAME[i];
      if (!config.marketEnabled[x]) continue;
      this.marketEnabled.push(i);
      logger.info(`${name} enabled`);
    }

    fs.readFile('./config_hedge.json', (err, data) => {
      if (err) throw err;
      const params: Config = JSON.parse(data.toString());
      this.levels = params.levels;
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
    this.balanceGauge = this.meter.createObservableGauge(
      METRIC_TYPES.balance,
      {
        description: 'account Value of the hedge account',
      }
    );

    this.unrealizedPnL = this.meter.createObservableGauge(
      METRIC_TYPES.unrealized_pnl,
      {
        description: 'The account unrealized PnL',
      }
    );
    this.perpPositionBase = this.meter.createObservableGauge(
      METRIC_TYPES.perp_position_base,
      {
        description: 'Base asset value of account perp positions',
      }
    );
    this.perpPositionQuote = this.meter.createObservableGauge(
      METRIC_TYPES.perp_position_quote,
      {
        description: 'Quote asset value of account perp positions',
      }
    );

    this.meter.addBatchObservableCallback(
      async (batchObservableResult: BatchObservableResult) => {
        // each subaccount is responsible for a market
        // record account specific metrics
        for (const user of this.driftClient.getUsers().values()) {
          const userAccount = user.getUserAccount();
          const labels = metricAttrFromUserAccount(user.userAccountPublicKey, userAccount);
          this.mangoAccount.getAssetsValue(this.mangoGroup).toNumber();

          for (const [name, balance] of this.agentState.balances.entries()) {
            const labelWithName = { ...labels };
            labelWithName.name = name;
            batchObservableResult.observe(
              this.balanceGauge,
              balance,
              labelWithName
            );
          }

          for (const marketIndex of this.marketEnabled) {

            const labelWithMarket = { ...labels };
            labelWithMarket.market = INDEX_TO_NAME[marketIndex];

            batchObservableResult.observe(
              this.unrealizedPnL,
              this.agentState.unsettledPnL.get(marketIndex),
              labelWithMarket
            );

            batchObservableResult.observe(
              this.perpPositionBase,
              this.agentState.hedgeBasePosition.get(marketIndex),
              labelWithMarket
            );

            batchObservableResult.observe(
              this.perpPositionQuote,
              this.agentState.hedgeQuotePosition.get(marketIndex),
              labelWithMarket
            );
          }
        }
      },
      [
        this.perpPositionBase,
        this.perpPositionQuote,
        this.unrealizedPnL,
        this.balanceGauge
      ]
    );

  }

  private async fetchGroup() {
    this.mangoGroup = await this.mangoClient.getGroup(this.GROUP);
    for (const marketIndex of this.marketEnabled) {
      const mangoIndex = INDEX_TO_MANGO[marketIndex];
      const perpMarket = this.mangoGroup.getPerpMarketByMarketIndex(mangoIndex);
      this.decimalPrice.set(marketIndex, getDecimalCount(perpMarket.minOrderSize));
      logger.debug(`${perpMarket.name}: decimal count: ${this.decimalPrice.get(marketIndex)}`);
    }
  }

  private async fetchMangoAccount() {
    this.mangoAccount = await this.mangoClient.getMangoAccount(this.mangoKey);
  }

  public async init(): Promise<void> {
    logger.info(`${this.name} initiating`);
    this.agentState = {
      unsettledPnL: new Map<number, number>(),
      openOrders: new Map<number, PerpOrder[]>(),
      hedgeQuotePosition: new Map<number, number>(),
      hedgeBasePosition: new Map<number, number>(),
      positionList: new Map<number, MaxSizeList>(),
      targetHedge: new Map<number, number>(),
      asks: new Map<number, BookSide>(),
      bids: new Map<number, BookSide>(),
      balances: new Map<string, number>(),
      balancesUsd: new Map<string, number>(),
      price: new Map<number, number>()
    };

    const initPromises: Array<Promise<any>> = [];

    initPromises.push(this.fetchGroup());
    initPromises.push(this.fetchMangoAccount());

    await Promise.all(initPromises);

    await this.updateAgentState();
  }

  public async reset(): Promise<void> {
    for (const intervalId of this.intervalIds) {
      clearInterval(intervalId);
    }
  }

  public async startIntervalLoop(intervalMs: number): Promise<void> {
    await this.main();
    const intervalId = setInterval(
      this.main.bind(this),
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

  // eslint-disable-next-line @typescript-eslint/no-unused-vars,@typescript-eslint/explicit-module-boundary-types
  public async trigger(record: any): Promise<void> {

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
  private async updateAgentState(): Promise<void> {

    const perpPositions: { [marketIndex: number]: DriftPerpPosition } = {};

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

      const mangoIndex = INDEX_TO_MANGO[marketIndex];
      const name = INDEX_TO_NAME[marketIndex];
      this.mangoAccount = await this.mangoClient.getMangoAccount(this.mangoKey);
      const perpPosition = this.mangoAccount.getPerpPosition(mangoIndex);
      const perpMarket = this.mangoGroup.getPerpMarketByMarketIndex(mangoIndex);

      const basePosition = perpPosition ? perpPosition.getBasePositionUi(perpMarket) : 0;
      const quotePosition = perpPosition ? perpPosition.getQuotePositionUi(perpMarket) : 0;
      const unsettledPnL = perpPosition ? perpPosition.getUnsettledPnlUi(perpMarket) : 0;

      this.agentState.hedgeBasePosition.set(marketIndex, basePosition);
      this.agentState.hedgeQuotePosition.set(marketIndex, quotePosition);
      this.agentState.unsettledPnL.set(marketIndex, unsettledPnL);

      logger.debug(`${name}: hedge base position: ${basePosition.toFixed(4)} $${quotePosition.toFixed(4)}`);
      logger.debug(`${name}: Unsettled PnL: ${unsettledPnL.toFixed(4)}`);

      const openOrders = await this.mangoAccount.loadPerpOpenOrdersForMarket(this.mangoClient, this.mangoGroup, perpMarket.perpMarketIndex, true);
      this.agentState.openOrders.set(marketIndex, openOrders);

      const baseAmount = convertToNumber(p.baseAssetAmount, BASE_PRECISION);
      logger.debug(`${name}: baseAmount: ${baseAmount.toFixed(4)}`);

      let positionList = this.agentState.positionList.get(marketIndex);

      if (!positionList) {
        this.agentState.positionList.set(marketIndex, new MaxSizeList(this.positionCountForAverage));
        positionList = this.agentState.positionList.get(marketIndex);
      }
      this.agentState.positionList.get(marketIndex).add(baseAmount);

      const targetHedge = positionList.getAverage();
      if (positionList.getItems().length >= 1) {
        this.agentState.targetHedge.set(marketIndex, targetHedge);
      }
      logger.debug(`${name}: target Hedge: ${targetHedge}`);

      const price = this.mangoGroup.toUiPrice(perpMarket.price, perpMarket.baseDecimals);
      this.agentState.asks.set(marketIndex, await perpMarket.loadAsks(this.mangoClient, true));
      this.agentState.bids.set(marketIndex, await perpMarket.loadBids(this.mangoClient, true));
      this.agentState.price.set(marketIndex, price);
      logger.info(`${name}: Current price on Mango: ${price}`);
    }

    for (const [name, banks] of this.mangoGroup.banksMapByName.entries()) {
      const bank = banks[0];
      const balance = this.mangoAccount.getTokenBalanceUi(bank);
      const balanceUsd = bank.uiPrice * balance;
      this.agentState.balances.set(name, balance);
      this.agentState.balancesUsd.set(name, balanceUsd);
      logger.debug(`${name}: Balance: ${balance.toFixed(4)} ${balanceUsd.toFixed(4)}`);
    }

  }

  public updateParams(config: Config): void {

    fs.writeFile('./config_hedge.json', JSON.stringify(config), (err) => {
      if (err) throw err;
      console.log('The file has been saved!');
    });

    this.levels = config.levels;

    console.log('Hedge Config updated');
    console.log(this.levels);
  }

  private async updateHedges(marketIndex: number) {

    const mangoMarketIndex = INDEX_TO_MANGO[marketIndex];
    const name = INDEX_TO_NAME[marketIndex];

    const openOrders = this.agentState.openOrders.get(marketIndex);

    let cancelOrders = false;

    const exposure = this.agentState.targetHedge.get(marketIndex);
    const hedge = this.agentState.hedgeBasePosition.get(marketIndex);
    const price = this.agentState.price.get(marketIndex);
    const deltaCurrentAndTargetHedge = exposure - hedge;
    const deltaInUsd = deltaCurrentAndTargetHedge * price;

    if (openOrders.length > 1) {
      cancelOrders = true;
      logger.info(`${name} - Cancel all orders, too many orders}`);
    } else if (openOrders.length === 1) {
      const openOrder = openOrders[0];
      const openOrderSizeUsd = openOrder.size * price;

      logger.debug(`${name} open order found ${openOrder.size} $${openOrder.price}`);

      if (Math.abs(deltaInUsd - openOrderSizeUsd) > this.levels[marketIndex].acceptableMargin * 0.5) {
        logger.info(`${name} - Cancel all orders, size difference, ${deltaInUsd.toFixed(4)} / ${openOrderSizeUsd.toFixed(4)}}`);
        logger.debug(`${name} - Acceptable margin ${this.levels[marketIndex].acceptableMargin}}`);
        cancelOrders = true;
      }
    }

    if (cancelOrders) {
      // this.mangoClient.perpCancelAllOrders(this.mangoGroup, this.mangoAccount, mangoMarketIndex, 30).then(
      //   () => logger.info(`${name} - Hedge open Order cancelled`)
      // // ).catch(e => logger.error(`${name} - Hedge open Order cancel error: ${e}`));
      // ).catch(e => console.log(e));
      for (const openOrder of openOrders) {
        this.mangoClient.perpCancelOrder(this.mangoGroup, this.mangoAccount, mangoMarketIndex, openOrder.orderId).then(
        () => logger.info(`${name} - Hedge open Order cancelled`));
      // ).catch(e => logger.error(`;
      }
    }

    if (openOrders.length === 0 && Math.abs(deltaInUsd) > this.levels[marketIndex].acceptableMargin) {
      const side = deltaCurrentAndTargetHedge > 0 ? PerpOrderSide.bid : PerpOrderSide.ask;

      let price: number;
      if (deltaCurrentAndTargetHedge > 0) {
        logger.debug(`${name} - best price ${this.agentState.asks.get(marketIndex).best().price} - min change ${1 / 10 ** this.decimalPrice.get(marketIndex)}`);
        price = this.agentState.asks.get(marketIndex).best().price - 1 / (10 ** this.decimalPrice.get(marketIndex));
      } else {
        logger.debug(`${name} - best price ${this.agentState.bids.get(marketIndex).best().price} - min change ${1 / 10 ** this.decimalPrice.get(marketIndex)}`);
        price = this.agentState.bids.get(marketIndex).best().price + 1 / (10 ** this.decimalPrice.get(marketIndex));
      }

      const quantity = Math.abs(deltaCurrentAndTargetHedge);

      logger.info(`${name} placing Order: ${deltaCurrentAndTargetHedge > 0 ? 'buy' : 'sell'} $${price.toFixed(4)} Qty: ${quantity.toFixed(4)}`);

      // this.mangoClient.perpPlaceOrder(
      //   this.mangoGroup,
      //   this.mangoAccount,
      //   mangoMarketIndex,
      //   side,
      //   price,
      //   quantity,
      //   null,
      //   null,
      //   PerpOrderType.postOnlySlide,
      //   null,
      //   new Date().getTime() + 60000
      // ).then(
      //   () => logger.info(`${name} - Hedge open Order cancelled`)
      // ).catch(e => logger.error(`${name} - Hedge open Order cancel error: ${e}`));
    }

  }

  private async main() {
    const start = Date.now();
    let ran = false;
    try {
      await tryAcquire(this.periodicTaskMutex).runExclusive(async () => {

        await this.updateAgentState();
        await Promise.all(
          this.driftClient.getPerpMarketAccounts().map((marketAccount) => {
            if (!this.marketEnabled.includes(marketAccount.marketIndex)) return;
            this.updateHedges(marketAccount.marketIndex);
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
      } else {
        throw e;
      }
    } finally {
      if (ran) {
        logger.debug(`${this.name} Bot took ${Date.now() - start}ms to run`);

        await this.watchdogTimerMutex.runExclusive(async () => {
          this.watchdogTimerLastPatTime = Date.now();
        });
      }
    }
  }
}
