export interface KlineResponse {
  code: string;
  data: [number, number, number, number, number, number][]
}

export interface KlineParams {
  symbol: string;
  granularity: number;
  from?: number;
  to?: number;
}

export interface Contract {
  symbol: string;
  rootSymbol: string;
  type: string;
  firstOpenDate: number;
  expireDate: number | null;
  settleDate: number | null;
  baseCurrency: string;
  quoteCurrency: string;
  settleCurrency: string;
  maxOrderQty: number;
  maxPrice: number;
  lotSize: number;
  tickSize: number;
  indexPriceTickPrice: number;
  multiplier: number;
  initialMargin: number;
  maintainMargin: number;
  maxRiskLimit: number;
  minRiskLimit: number;
  riskStep: number;
  makerFeeRate: number;
  takerFeeRate: number;
  takerFixFee: number;
  makerFixFee: number;
  settlementFee: number | null;
  isDeleverage: boolean;
  isQuanto: boolean;
  isInverse: boolean;
  markMethod: string;
  fairMethod: string;
  fundingBaseSymbol: string;
  fundingQuoteSymbol: string;
  fundingRateSymbol: string;
  indexSymbol: string;
  settlementSymbol: string;
  status: string;
  fundingFeeRate: number;
  predictedFundingFeeRate: number;
  openInterest: string;
  turnoverOf24h: number;
  volumeOf24h: number;
  markPrice: number;
  indexPrice: number;
  lastTradePrice: number;
  nextFundingRateTime: number;
  maxLeverage: number;
  sourceExchanges: string[];
  premiumSymbol1M: string;
  premiumSymbol8H: string;
  fundingBaseSymbol1M: string;
  fundingQuoteSymbol1M: string;
  lowPrice: number;
  highPrice: number;
  priceChgPct: number;
  priceChg: number;
}

export interface ContractListResponse {
  code: string,
  data: Contract[];
}

export interface ContractResponse {
  code: string,
  data: Contract;
}

export interface ContractParams {
  symbol: string;
}

export interface TimeResponse {
  code: string,
  data: number;
}

export interface StatusResponse {
  code: string,
  data: { status: string, msg: string };
}

export interface HistoricalDataResponse {
  code: string,
  data: {
    sequence: number,
    tradeId: string,
    takerOrderId: string,
    makerOrderId: string,
    price: string,
    size: number,
    side: string,
    ts: number
  }[];
}

export interface HistoricalDataParams {
  symbol: string;
}

export interface TickerResponse {
  code: string,
  data: {
    sequence: number,
    symbol: string,
    side: string,
    size: number,
    price: string,
    bestBidSize: number,
    bestAskSize: number,
    bestBidPrice: string,
    bestAskPrice: string,
    tradeId: string,
    ts: number
  };
}

export interface TickerParams {
  symbol: string;
}

export interface OrderBookResponse {
  code: string,
  data: {
    sequence: number,
    symbol: string,
    asks: [string, number][],
    bids: [string, number][],
    ts: number
  };
}

export interface OrderBookParams {
  symbol: string;
  depth?: 'depth20' | 'depth100';
}
