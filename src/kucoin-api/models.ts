import { ObjectId } from 'mongodb';

export declare type PositionStatus = 'failed' | 'cancelled' | 'closed' | 'opened';
export declare type BuySell = 'buy' | 'sell';

export interface Balance {
  netBalance: number,
  health: number,
  exchange: string
}

export interface Order {
  orderId: string;
  symbol: string;
  exchange: 'kucoin' | 'mango';
  status: 'match' | 'open' | 'done';
  orderType: 'limit' | 'market' | 'limit_stop' | 'market_stop' | 'unknown';
  side: 'sell' | 'buy';
  price: string;
  size: string;
  lotSize: string;
  matchLotSize: string;
  matchSize: string;
  timestamp: number;
}

export interface Fill {
  transactionId: string;
  orderId: string;
  symbol: string;
  exchange: 'kucoin' | 'mango';
  orderType: 'limit' | 'market' | 'limit_stop' | 'market_stop' | 'unknown';
  tradeType: 'trade' | 'liquidation' | 'ADL' | 'settlement';
  side: 'sell' | 'buy';
  price: string;
  size: string;
  lotSize: string;
  timestamp: number;
}

export interface Position {
  exchange: 'kucoin' | 'mango';
  symbol: string;
  currentQty: number;
  markValue: number;
  avgEntryPrice: number;
  markPrice: number;
  unrealisedPnl: number;
  unrealisedPnlPcnt: number;
  realisedPnl: number;
  unrealisedCost?: number;
  realisedCost?: number;
}

export interface PositionPriceChange {
  exchange: 'kucoin' | 'mango';
  symbol: string;
  markValue: number;
  markPrice: number;
  unrealisedPnl: number;
  unrealisedPnlPcnt: number;
  realisedPnl?: number;
}

export class OrderBook {
  _asks: [number, number][]; // [price, volume]
  _bids: [number, number][]; // [price, volume]
  datetime: Date;
  source: string;
  symbol: string;

  constructor(source: string, symbol: string, asks: [number, number][] = [], bids: [number, number][] = []) {
    this._asks = asks;
    this._bids = bids;
    this.datetime = new Date();
    this.source = source;
    this.symbol = symbol;
  }

  bestAsk(min = 1): number | undefined {
    let totalSize = 0;
    for (const x of this._asks) {
      totalSize += x[1];
      if (totalSize >= min) return x[0];
    }
  }

  bestBid(min = 1): number | undefined {
    let totalSize = 0;
    for (const x of this._bids) {
      totalSize += x[1];
      if (totalSize >= min) return x[0];
    }
  }

  updateData(asks: [number, number][] | undefined = undefined, bids: [number, number][] | undefined = undefined) {
    this.datetime = new Date();
    if (asks) this._asks = asks;
    if (bids) this._bids = bids;
  }

  toDict(): any {
    return {
      asks: this._asks, bids: this._bids, source: this.source, symbol: this.symbol, datetime: this.datetime
    };
  }
}

export interface Exposure {
  buyOpen: number;
  sellOpen: number;
  exposure: number;
  unrealisedPnl: number;
  realisedPnl: number;
}

export interface Trades {
  datetime: Date;
  trades: any[];
  symbol: string;
  source: string;
}

export interface StrategyPosition {
  _id: ObjectId | undefined,
  label: string;
  time: Date;
  closedTime: Date | undefined;
  pnl: number;
  status: PositionStatus;
  strategyName: string;
  strategyVersion: string;
  version: string;
  trades: [];
  data: any;
}

export interface PlaceLimitResponse {
  reason?: string;
  status: 'success' | 'error';
  orderId: string;
}

export interface CancelOrderResponse {
  reason?: string;
  status: 'success' | 'error';
  orderId: string;
}

export interface CancelAllOrdersResponse {
  reason?: string;
  status: 'success' | 'error';
  orderIds: string[];
}

export interface Config {
  name: string;
  label: string;
  version: string;
  env: string;
  data: {
    minimumHealth: number;
    symbol: string;
    size: number;
    kucoin: {
      contract: string;
      healthThreshold: number;
      leverage: number;
      minPriceIncrement: string;
      contractSize: string;
    };
    mango: {
      contract: string;
      minPriceIncrement: string;
      sleepTime: number;
    };
    strategy: {
      exposure: {
        exchange_risk_hard: number;
        minimum_qty: number;
        maker_qty: number;
        taker_qty: number;
        exchange_risk_soft: number;
      };
      long: {
        maker_spread: number;
        taker_spread: number;
        reduce_exposure_spread: number;
      };
      short: {
        maker_spread: number;
        taker_spread: number;
        reduce_exposure_spread: number;
      }
    }
  }
}
