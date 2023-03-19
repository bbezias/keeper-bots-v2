import {
  DriftClient,
  PerpMarketAccount,
  SlotSubscriber,
  OrderRecord,
  NewUserRecord,
  PerpPosition,
  SpotPosition,
  DLOB,
  UserMap,
  UserStatsMap, MarketType,
} from '@drift-labs/sdk';
import { Mutex, tryAcquire, withTimeout } from 'async-mutex';

import { logger } from '../logger';
import { Bot } from '../types';

import { RuntimeSpec } from '../metrics';
import { JitMakerConfig, TestingBotConfig } from '../config';

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

/**
 *
 * This bot is responsible for placing small trades during an order's JIT auction
 * in order to partially fill orders and collect maker fees. The bot also tracks
 * its position on all available markets in order to limit the size of open positions.
 *
 */
export class TestingBot implements Bot {
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

  private intervalIds: Array<NodeJS.Timer> = [];

  private agentState: State;

  // metrics
  private metricsPort: number | undefined;
  private runtimeSpec: RuntimeSpec;
  private watchdogTimerMutex = new Mutex();
  private watchdogTimerLastPatTime = Date.now();
  private isActive = false;

  constructor(
    clearingHouse: DriftClient,
    slotSubscriber: SlotSubscriber,
    runtimeSpec: RuntimeSpec,
    config: TestingBotConfig
  ) {
    this.name = config.botId;
    this.dryRun = config.dryRun;
    this.driftClient = clearingHouse;
    this.slotSubscriber = slotSubscriber;
    this.runtimeSpec = runtimeSpec;
    this.metricsPort = config.metricsPort;
  }

  public async init(): Promise<void> {

    logger.info(`${this.name} initiating`);
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

    initPromises.push(this.fetchAllUserStats());

    this.dlob = new DLOB();
    initPromises.push(
      this.dlob.initFromUserMap(this.userMap, this.slotSubscriber.getSlot())
    );

    this.agentState = {
      stateType: new Map<number, StateType>(),
      spotMarketPosition: new Map<number, SpotPosition>(),
      perpMarketPosition: new Map<number, PerpPosition>(),
    };

    initPromises.push(this.fetchAllUsers());

    await Promise.all(initPromises);
    logger.info(`All init done`);
    this.isActive = true;

    const intervalId = setInterval(this.pingBlockHash.bind(this), 5000);
    this.intervalIds.push(intervalId);
  }

  private async fetchAllUsers() {
    const t1 = new Date().getTime();
    await this.userMap.fetchAllUsers();
    const t2 = new Date().getTime();
    logger.info(`Fetch all Users took ${t2 - t1} ms, contains ${this.userMap.size()} users`);
  }

  private async fetchAllUserStats() {
    const t1 = new Date().getTime();
    await this.userStatsMap.fetchAllUserStats();
    const t2 = new Date().getTime();
    logger.info(`Fetch all Users stats took ${t2 - t1} ms, contains ${this.userStatsMap.size()} users`);
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

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  private async logicRun(marketAccount: PerpMarketAccount) {
    const nodesToFill = this.dlob.findJitAuctionNodesToFill(
      marketAccount.marketIndex,
      this.slotSubscriber.getSlot(),
      this.driftClient.getOracleDataForPerpMarket(marketAccount.marketIndex),
      MarketType.PERP
    );
    for (const nodeToFill of nodesToFill) {
      const userAccount = nodeToFill.node.userAccount;
      const t1 = new Date().getTime();
      const account = this.userMap.get(userAccount.toString());
      const t2 = new Date().getTime();
      const authority = account.getUserAccount().authority;
      const t3 = new Date().getTime();
      await this.userStatsMap.mustGet(
        authority.toString()
      );
      const t4 = new Date().getTime();
      logger.debug(`Get user account: ${t2 - t1} ms, found: ${!!account}`);
      logger.debug(`Authority found in : ${t3 - t2} ms`);
      logger.debug(`Stats in : ${t4 - t3} ms`);
    }
  }

  private async pingBlockHash() {
    const t1 = new Date().getTime();
    const connection = this.driftClient.connection;
    const latestBlockhash = await connection.getLatestBlockhash('confirmed');
    const t2 = new Date().getTime();
    logger.info(`Blockhash Ping: ${t2 - t1} ms, found: ${latestBlockhash}`);
  }

  private async tryMake() {
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
          this.driftClient.getPerpMarketAccounts().map((marketAccount) => {
            this.logicRun(marketAccount);
          })
        );

        ran = true;
      });
    } catch (e) {
      if (e === dlobMutexError) {
        logger.error(`${this.name} dlobMutexError timeout`);
      } else {
        throw e;
      }
    } finally {
      if (ran) {
        // logger.debug(`${this.name} Bot took ${Date.now() - start}ms to run`);
        await this.watchdogTimerMutex.runExclusive(async () => {
          this.watchdogTimerLastPatTime = Date.now();
        });
      }
    }
  }
}
