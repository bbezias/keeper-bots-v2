import { OrderBook } from '../kucoin-api/models';
import {
  BN,
  calculateBidAskPrice,
  convertToNumber,
  DLOB,
  DriftClient,
  MarketType,
  OraclePriceData,
  PRICE_PRECISION,
  ZERO
} from '@drift-labs/sdk';

export class KucoinMarketData {
  book: OrderBook;
  vwap: { bid: number, ask: number };
  bestPrice: { bid: number, ask: number };
  mid: number;

  constructor(book: OrderBook) {
    this.book = book;
    this.vwap = { bid: book.vwap('bids', 5), ask: book.vwap('asks', 5) };
    this.bestPrice = { bid: book.bestBid(10), ask: book.bestAsk(10) };
  }
}

export class DriftMarketData {
  public marketIndex: number;
  public slot: number;
  public dlob: DLOB;
  public bestPrice: { bid: number, ask: number };
  public bestPriceBn: { bid: BN, ask: BN };
  public mid: number;
  public spread: number
  public oraclePrice: OraclePriceData;
  private driftClient: DriftClient;

  constructor(marketIndex: number, dlob: DLOB, driftClient: DriftClient, slot: number) {
    this.marketIndex = marketIndex;
    this.dlob = dlob;
    this.driftClient = driftClient;
    this.bestPrice = {bid: 0, ask: 0};
    this.bestPriceBn = {bid: ZERO, ask: ZERO};
    this.slot = slot;
    this.process();
  }


  private process() {
    this.oraclePrice = this.driftClient.getOracleDataForPerpMarket(this.marketIndex);
      const [bid, ask] = calculateBidAskPrice(
        this.driftClient.getPerpMarketAccount(this.marketIndex).amm,
        this.driftClient.getOracleDataForPerpMarket(this.marketIndex)
      );
      this.bestPriceBn.ask = this.dlob.getBestAsk(this.marketIndex, ask, this.slot, MarketType.PERP, this.oraclePrice);
      this.bestPriceBn.bid = this.dlob.getBestBid(this.marketIndex, bid, this.slot, MarketType.PERP, this.oraclePrice);

      this.bestPrice = {bid: convertToNumber(this.bestPriceBn.bid, PRICE_PRECISION), ask: convertToNumber(this.bestPriceBn.ask, PRICE_PRECISION)};
      this.mid = (this.bestPrice.ask + this.bestPrice.bid) / 2;

      this.spread = this.bestPrice.ask - this.bestPrice.bid;
  }
}
