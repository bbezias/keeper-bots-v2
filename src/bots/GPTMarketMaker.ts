import { INDEX_TO_NAME, MaxSizeList } from './utils';
import { Meter } from '@opentelemetry/api-metrics';
import { Attributes, ObservableGauge, ObservableResult, UpDownCounter } from '@opentelemetry/api';

export interface Order {
  amount: number;
  direction: 'buy' | 'sell';
  price: number;
  amountFilled: number;
}

export interface ConfigBot {
  k1: number;
  k2: number;
  k3: number;
  k4: number;
  k5: number;
  k6: number;
  maxExposure: number;
  marketIndex: number;
  meter: Meter;
  name: string;
}

export class MarketMakerBot {
  marketIndex: number;
  bidPrice: number;
  askPrice: number;
  midPrice: number;
  realisedPnL = 0;
  lastExposureUpdateCurrentPrice: number;
  k1: number;
  k2: number;
  k3: number;
  k4: number;
  k5: number;
  k6: number;
  volatility: number;
  currentBidAskSpread: number;
  tradeVolume: number;
  orderBook: { bids: [number, number][], asks: [number, number][] };
  maxExposure: number;
  currentExposure: number;
  vwapBid: number;
  vwapAsk: number;
  openOrders: { bid: Order, ask: Order };
  kucoinPriceDelta: number;
  midPriceHistory: MaxSizeList;
  meter: Meter;
  marketDataGauge: ObservableGauge<Attributes>;
  botGauge: ObservableGauge<Attributes>;
  pnlCounter: UpDownCounter;
  isActive = false;
  name = "";
  marketName = "";

  constructor(config: ConfigBot) {
    this.marketName = INDEX_TO_NAME[this.marketIndex];
    this.k1 = config.k1;
    this.k2 = config.k2;
    this.k3 = config.k3;
    this.k4 = config.k4;
    this.k5 = config.k5;
    this.k6 = config.k6;
    this.marketIndex = config.marketIndex;
    this.maxExposure = config.maxExposure;
    this.midPriceHistory = new MaxSizeList(100);
    this.meter = config.meter;
    this.initializeMetrics();
  }

  initializeMetrics(): void {
    this.marketDataGauge = this.meter.createObservableGauge(
      `market_data_${this.name}_${this.marketName}`,
      {
        description: `Market Data Points for ${this.name}_${this.marketName}`
      }
    );
    this.marketDataGauge.addCallback(obs => this.marketDataCallback(obs));

    this.pnlCounter = this.meter.createUpDownCounter(
      `bot_${this.name}_${this.marketName}`,
      {
        description: `Bot data for ${this.name}_${this.marketName}`
      }
    );

    this.botGauge = this.meter.createObservableGauge(
      `market_data_${this.name}_${this.marketName}`,
      {
        description: `Bot Data Points for ${this.name}_${this.marketName}`
      }
    );
    this.botGauge.addCallback(obs => this.botCallback(obs));
  }

  calculateVWAP(): void {
    const calculateSideVWAP = (orders) => {
      const totalVolume = orders.reduce((acc, [, volume]) => acc + volume, 0);
      const weightedSum = orders.reduce((acc, [price, volume]) => acc + price * volume, 0);
      return weightedSum / totalVolume;
    };

    this.vwapBid = calculateSideVWAP(this.orderBook.bids);
    this.vwapAsk = calculateSideVWAP(this.orderBook.asks);
  }

  activate(): void {
    this.isActive = true;
  }

  deactivate(): void {
    this.isActive = false;
  }

  calculateBidAskSpread(): void {
    this.currentBidAskSpread = this.orderBook.asks[0][0] - this.orderBook.bids[0][0];
  }

  takeProfitLimit(side: 'buy' | 'sell'): number {
    const m = side === 'buy' ? 1 : -1;
    return this.midPrice + this.volatility * this.k5 * m;
  }

  stopLossLimit(side: 'buy' | 'sell'): number {
    const m = side === 'buy' ? 1 : -1;
    return this.midPrice - this.volatility * this.k6 * m;
  }

  pnl(): number {
    return this.unrealisedPnL() + this.realisedPnL;
  }

  process(_orderBook: { bids: [number, number][], asks: [number, number][] }, _tradeVolume: number): void {
    this.orderBook = _orderBook;
    this.tradeVolume = _tradeVolume;
    this.midPrice = (_orderBook.bids[0][0] + _orderBook.bids[0][0]) / 2;

    this.midPriceHistory.add(this.midPrice);
    this.calculateVWAP();
    this.calculateBidAskSpread();

    // Calculate volatility
    // Replace this line with your actual calculation method
    this.volatility = this.midPriceHistory.getVolatility();

    // Calculate bidPrice and askPrice using VWAP
    this.bidPrice = this.vwapBid - (this.k1 * this.volatility) - (this.k2 * this.currentBidAskSpread) + (this.k3 * this.tradeVolume) - (this.k4 * this.tradeVolume);
    this.askPrice = this.vwapAsk + (this.k1 * this.volatility) + (this.k2 * this.currentBidAskSpread) - (this.k3 * this.tradeVolume) + (this.k4 * this.tradeVolume);
  }

  processTrade(amount: number, price: number, side: 'buy' | 'sell'): void {
    const pnlBefore = this.pnl();
    const m = side === 'sell' ? -1 : 1;
    this.currentExposure += amount * m;
    this.realisedPnL += amount * price * m - this.currentExposure;
    this.lastExposureUpdateCurrentPrice = price;
    this.pnlCounter.add(this.pnl() - pnlBefore, { type: 'total', name: this.name, market: this.marketName });
    this.pnlCounter.add(amount * price * m - this.currentExposure, { type: 'realised', name: this.name, market: this.marketName });
  }

  isOrderPriceCloseToTarget(side: 'bid' | 'ask'): boolean {
    const orderPrice = this.openOrders[side].price;
    const targetPrice = side === 'bid' ? this.bidPrice : this.askPrice;
    return Math.abs(orderPrice - targetPrice) <= this.kucoinPriceDelta;
  }

  unrealisedPnL(): number {
    return this.currentExposure * (this.midPrice - this.lastExposureUpdateCurrentPrice);
  }

  private marketDataCallback(obs: ObservableResult<Attributes>) {
    if (this.isActive) {
      obs.observe(this.vwapAsk, { type: 'vwap', side: 'asks', name: this.name, market: this.marketName });
      obs.observe(this.vwapBid, { type: 'vwap', side: 'bids', name: this.name, market: this.marketName });
      obs.observe(this.midPrice, { type: 'mid', side: '', name: this.name, market: this.marketName });
      obs.observe(this.volatility, { type: 'volatility', side: '', name: this.name, market: this.marketName });
      obs.observe(this.currentBidAskSpread, { type: 'spread', side: '', name: this.name, market: this.marketName });
    }
  }

  private botCallback(obs: ObservableResult<Attributes>) {
    if (this.isActive) {
      obs.observe(this.bidPrice, { type: 'target_price', side: 'asks', name: this.name, market: this.marketName });
      obs.observe(this.askPrice, { type: 'target_price', side: 'bids', name: this.name, market: this.marketName });
      obs.observe(this.stopLossLimit('buy'), { type: 'stop_loss', side: 'buy', name: this.name, market: this.marketName });
      obs.observe(this.stopLossLimit('sell'), { type: 'stop_loss', side: 'sell', name: this.name, market: this.marketName });
      obs.observe(this.takeProfitLimit('buy'), { type: 'take_profit', side: 'buy', name: this.name, market: this.marketName });
      obs.observe(this.takeProfitLimit('sell'), { type: 'take_profit', side: 'sell', name: this.name, market: this.marketName });
    }
  }
}
