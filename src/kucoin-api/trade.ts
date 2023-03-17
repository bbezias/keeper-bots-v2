export interface PlaceLimitOrderParams {
  clientOid?: string;
  side: 'buy' | 'sell';
  symbol: string;
  type?: 'limit';
  leverage: number;
  remark?: string;
  stop?: 'down' | 'up';
  stopPriceType?: 'TP' | 'IP' | 'MP';
  stopPrice?: string;
  reduceOnly?: boolean;
  closeOrder?: boolean;
  forceHold?: boolean;

  price: string;
  size: number;
  timeInForce?: 'GTC' | 'IOC';
  postOnly?: boolean;
  hidden?: boolean;
  iceberg?: boolean;
  visibleSize?: boolean
}

export interface PlaceMarketOrderParams {
  clientOid: string;
  side: 'buy' | 'sell';
  symbol: string;
  type?: 'market';
  leverage: number;
  remark?: string;
  stop?: 'down' | 'up';
  stopPriceType?: 'TP' | 'IP' | 'MP';
  stopPrice?: string;
  reduceOnly?: boolean;
  closeOrder?: boolean;
  forceHold?: boolean;

  size: number;
}

export interface PlaceOrderResponse {
  code: string;
  msg?: string;
  data: {
    orderId: string
  }
}

export interface Order {
  id: string;
  symbol: string;
  type: 'limit' | 'market' | 'limit_stop' | 'market_stop';
  side: 'buy' | 'sell';
  price: string;
  size: number;
  value: string;
  dealValue: string;
  dealSize: number;
  stp: string;
  stop: string;
  stopPriceType: string;
  stopTriggered: boolean;
  stopPrice?: string;
  timeInForce: string;
  postOnly: boolean;
  hidden: boolean;
  iceberg: boolean;
  leverage: string;
  forceHold: boolean;
  closeOrder: boolean;
  visibleSize?: number;
  clientOid: string;
  remark?: string;
  tags?: string;
  isActive: boolean;
  cancelExist: boolean;
  createdAt: number;
  updatedAt: number;
  endAt: number;
  orderTime: number;
  settleCurrency: string;
  status: 'match' | 'done' | 'open';
  filledValue: string;
  filledSize: number;
  reduceOnly: boolean
}

export interface OrderListParams {
  status?: 'active' | 'done';
  symbol?: string;
  side?: 'buy' | 'sell';
  type?: 'limit' | 'market' | 'limit_stop' | 'market_stop';
  startAt?: number;
  endAt?: number;
}

export interface OrderListResponse {
  code: string;
  data: {
    currentPage: number;
    pageSize: number;
    totalNum: number;
    totalPage: number;
    items: Order[];
  }
}

export interface SingleOrderParams {
  orderId?: string;
  clientOid?: string;
}

export interface SingleOrderResponse {
  code: string;
  data: Order;
}

export interface StopOrderListParams {
  symbol?: string;
}

export interface StopOrderListResponse {
  code: string;
  data: {
    currentPage: number;
    pageSize: number;
    totalNum: number;
    totalPage: number;
    items: Order[];
  }
}

export interface RecentDoneOrdersResponse {
  code: string;
  data: Order[];
}

export interface FillsParams {
  orderId?: string;
  symbol?: string;
  side?: 'buy' | 'sell';
  type?: 'limit' | 'market' | 'limit_stop' | 'market_stop';
  startAt?: number;
  endAt?: number;
}

export interface Fill {
  symbol: string;
  tradeId: string;
  orderId: string;
  side: 'sell' | 'buy';
  liquidity: 'taker';
  forceTaker: boolean;
  price: string;
  size: number;
  value: string;
  feeRate: string;
  fixFee: string;
  feeCurrency: string;
  stop: string;
  fee: string;
  orderType: 'limit' | 'market' | 'limit_stop' | 'market_stop';
  tradeType: 'trade' | 'liquidation' | 'ADL' | 'settlement';
  createdAt: number;
  settleCurrency: string;
  tradeTime: number;
}

export interface FillsResponse {
  code: string;
  data: {
    currentPage: number;
    pageSize: number;
    totalNum: number;
    totalPage: number;
    items: Fill[];
  };
}

export interface RecentFillsResponse {
  code: string;
  data: Fill[];
}

export interface OpenOrderStatisticsParams {
  symbol: string;
}

export interface OpenOrderStatisticsResponse {
  code: string;
  data: {
    openOrderBuySize: number,
    openOrderSellSize: number,
    openOrderBuyCost: string,
    openOrderSellCost: string,
    settleCurrency: string
  };
}

export interface Position {
  id: string;
  symbol: string;
  autoDeposit: boolean;
  mainMarginReq: number;
  riskLimit: number;
  realLeverage: number
  crossMode: boolean;
  delevPercentage: number;
  openingTimestamp: number;
  currentTimestamp: number;
  currentQty: number;
  currentCost: number;
  unrealisedCost: number;
  realisedGrossCost: number;
  realisedCost: number;
  isOpen: boolean;
  markPrice: number;
  markValue: number;
  posCost: number;
  posCross: number;
  posInit: number;
  posComm: number;
  posLoss: number;
  posMargin: number;
  posMaint: number;
  maintMargin: number;
  realisedGrossPnl: number;
  realisedPnl: number;
  unrealisedPnl: number;
  unrealisedPnlPcnt: number;
  unrealisedRoePcnt: number;
  avgEntryPrice: number;
  liquidationPrice: number;
  bankruptPrice: number;
  settleCurrency: string;
  maintainMargin: number;
  riskLimitLevel: number;
}

export interface PositionParams {
  symbol: string;
}

export interface PositionResponse {
  code: string;
  data: Position;
}

export interface PositionListResponse {
  code: string;
  data: Position[];
}

export interface CancelOrderParams {
  orderId: string;
}

export interface CancelOrderResponse {
  code: string;
  data: {cancelledOrderIds: string[]};
  msg?: string;
}

export interface CancelAllOrdersParams {
  symbol?: string;
}

export interface CancelAllOrdersResponse {
  code: string;
  data: {cancelledOrderIds: string[]};
  msg?: string;
}
