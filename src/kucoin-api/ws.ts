import ReconnectingWebSocket from 'reconnecting-websocket';
import WS from 'ws';
import { Sign, sign } from './utils';
import axios, { AxiosResponse } from 'axios';
import chalk from 'chalk';

export interface WsParams {
  baseUrl: string;
  passphrase: string | undefined;
  environment: string;
  apiKey: string | undefined;
  secretKey: string | undefined;
}

export enum WsSocketEndpoints {
  Ticker = 'ticker',
  TickerV2 = 'tickerv2',
  Orderbook = 'orderbook',
  Execution = 'execution',
  FullMatch = 'fullMatch',
  Depth5 = 'depth5',
  Depth50 = 'depth50',
  Market = 'market',
  Snapshot = 'snapshot',
  Announcement = 'announcement',
  OrdersMarket = 'ordersMarket',
  Orders = 'orders',
  AdvancedOrders = 'advancedOrders',
  Balances = 'balances',
  Position = 'position',
}

export interface SubscriptionParams {
  endpoint: WsSocketEndpoints;
  symbol: string | undefined;
  callback: (msg: any, topic: string) => void;
}

export interface UnsubscriptionParams {
  endpoint: WsSocketEndpoints;
  symbol: string | undefined;
}

export interface Level2Response {
  asks: [number, number][];
  bids: [number, number][];
  sequence: number;
  ts: number;
  timestamp: number;
}

export interface ExecutionResponse {
  symbol: string;
  makerUserId: string;
  sequence: number;
  side: string;
  size: number;
  price: number;
  takerOrderId: string;
  makerOrderId: string;
  tradeId: string;
  ts: number;
}

export interface OrderChangeResponse {
  orderId: string;
  symbol: string;
  type: "open" | "match" | "filled" | "canceled" | "update";
  status: "match" | "open" | "done";
  matchSize?: string;
  matchPrize?: string;
  orderType: 'market' | 'limit';
  side: 'buy' | 'sell';
  price: string;
  size: string;
  remainSize?: string;
  filledSize?: string;
  canceledSize?: string;
  tradeId?: string;
  clientOid: string;
  orderTime: number;
  oldSize?: string;
  liquidity?: string;
  ts: number;
}

export interface PositionChangeOperationResponse {
  realisedGrossPnl: number;
  symbol: string;
  crossMode: false;
  liquidationPrice: number;
  posLoss: number;
  avgEntryPrice: number;
  unrealisedPnl: number;
  markPrice: number;
  posMargin: number;
  autoDeposit: boolean;
  riskLimit: number;
  unrealisedCost: number;
  posComm: number;
  posMaint: number;
  posCost: number;
  maintMarginReq: number;
  bankruptPrice: number;
  realisedCost: number;
  markValue: number;
  posInit: number;
  realisedPnl: number;
  maintMargin: number;
  realLeverage: number;
  changeReason: 'marginChange' | 'positionChange' | 'liquidation' | 'autoAppendMarginStatusChange' | 'adl' | 'markPriceChange';
  currentCost: number;
  openingTimestamp: number;
  currentQty: number;
  delevPercentage: number;
  currentComm: number;
  realisedGrossCost: number;
  isOpen: boolean;
  posCross: number;
  currentTimestamp: number;
  unrealisedRoePcnt: number;
  unrealisedPnlPcnt: number;
  settleCurrency: string;
}

export interface PositionChangePriceResponse {
  symbol: string;
  unrealisedPnl: number;
  markPrice: number;
  markValue: number;
  maintMargin: number;
  realLeverage: number;
  delevPercentage: number;
  currentTimestamp: number;
  unrealisedRoePcnt: number;
  unrealisedPnlPcnt: number;
  settleCurrency: string;
}

function topics(topic: WsSocketEndpoints, symbol = ''): { endpoint: string, type: string } {
  if (topic === WsSocketEndpoints.Ticker) {
    return { endpoint: "/contractMarket/ticker:" + symbol, type: 'public' };
  } else if (topic === WsSocketEndpoints.TickerV2) {
    return { endpoint: "/contractMarket/tickerV2:" + symbol, type: 'public' };
  } else if (topic === WsSocketEndpoints.Orderbook) {
    return { endpoint: "/contractMarket/level2:" + symbol, type: 'public' };
  } else if (topic === WsSocketEndpoints.Execution) {
    return { endpoint: "/contractMarket/execution:" + symbol, type: 'public' };
  } else if (topic === WsSocketEndpoints.FullMatch) {
    return { endpoint: "/contractMarket/level3v2:" + symbol, type: 'public' };
  } else if (topic === WsSocketEndpoints.Depth5) {
    return { endpoint: "/contractMarket/level2Depth5:" + symbol, type: 'public' };
  } else if (topic === WsSocketEndpoints.Depth50) {
    return { endpoint: "/contractMarket/level2Depth50:" + symbol, type: 'public' };
  } else if (topic === WsSocketEndpoints.Market) {
    return { endpoint: "/contract/instrument:" + symbol, type: 'public' };
  } else if (topic === WsSocketEndpoints.Announcement) {
    return { endpoint: "/contract/announcement", type: 'public' };
  } else if (topic === WsSocketEndpoints.Snapshot) {
    return { endpoint: "/contractMarket/snapshot:" + symbol, type: 'public' };
  } else if (topic === WsSocketEndpoints.OrdersMarket) {
    return { endpoint: "/contractMarket/tradeOrders:" + symbol, type: 'private' };
  } else if (topic === WsSocketEndpoints.Orders) {
    return { endpoint: "/contractMarket/tradeOrders", type: 'private' };
  } else if (topic === WsSocketEndpoints.AdvancedOrders) {
    return { endpoint: "/contractMarket/advancedOrders", type: 'private' };
  } else if (topic === WsSocketEndpoints.Balances) {
    return { endpoint: "/contractAccount/wallet", type: 'private' };
  } else if (topic === WsSocketEndpoints.Position) {
    return { endpoint: "/contract/position:" + symbol, type: 'private' };
  } else {
    throw Error('Topic is not found');
  }
}

class KucoinFutureWebSocket {

  signature: { [key: string]: any } | undefined;
  baseUrl: string;
  environment: string;
  passphrase: string | undefined;
  apiKey: string | undefined;
  secretKey: string | undefined;
  heartbeat: NodeJS.Timer | undefined;
  ws: ReconnectingWebSocket | undefined;
  subscriptions: { [topic: string]: (msg: any, topic: string) => void } = {};
  openingTime?: Date;

  constructor(params: WsParams) {
    this.baseUrl = params.baseUrl;
    this.environment = params.environment;
    this.passphrase = params.passphrase;
    this.apiKey = params.apiKey;
    this.secretKey = params.secretKey;

    if (this.passphrase && this.apiKey && this.secretKey) {
      const signatureParams: Sign = {
        params: {},
        endpoint: '/api/v1/bullet-private',
        method: 'POST',
        passphrase: this.passphrase,
        apiKey: this.apiKey,
        secretKey: this.secretKey
      };
      this.signature = sign(signatureParams);
    } else {
      this.signature = undefined;
    }
  }

  async getPublicWsToken(): Promise<AxiosResponse | undefined> {
    const url = this.baseUrl + '/api/v1/bullet-public';
    return await axios.post(url, {}).catch(e => {
      console.log(chalk.red('ERROR getPublicWsToken', url, e));
      return undefined;
    });
  }

  async getPrivateWsToken(): Promise<AxiosResponse | undefined> {
    const url = this.baseUrl + '/api/v1/bullet-private';
    return await axios.post(url, {}, this.signature).catch(e => {
      console.log(chalk.red('ERROR getPublicWsToken', url, e));
      return undefined;
    });
  }

  async getSocketEndpoint(): Promise<string> {
    let r: AxiosResponse | undefined;
    if (this.signature) {
      r = await this.getPrivateWsToken();
    } else {
      r = await this.getPublicWsToken();
    }

    if (!r) return '';

    const data = r.data.data;
    const token = data.token;
    const instanceServer = data.instanceServers[0];

    if (instanceServer) {
      if (this.environment === 'live') {
        return `${instanceServer.endpoint}?token=${token}&[connectId=${Date.now()}]`;
      } else {
        return `${instanceServer.endpoint}?token=${token}&[connectId=${Date.now()}]`;
      }
    } else {
      throw Error("No Kucoin WS servers running");
    }
  }

  _subscribe(sub: SubscriptionParams) {

    if (!this.ws) throw Error('Websocket not initialised');

    const details = topics(sub.endpoint, sub.symbol);

    if (details.endpoint in this.subscriptions) {
      throw Error('Subscription already exists');
    }

    if (details.type === 'private') {
      this.ws.send(JSON.stringify({
        id: Date.now(),
        type: 'subscribe',
        topic: details.endpoint,
        privateChannel: true,
        response: true
      }));
      console.log("Kucoin - Subscribed to ", details.endpoint);
    } else {
      this.ws.send(JSON.stringify({
        id: Date.now(),
        type: 'subscribe',
        topic: details.endpoint,
        privateChannel: false,
        response: true
      }));
      console.log("Kucoin - Subscribed to ", details.endpoint);
    }

    this.subscriptions[details.endpoint] = sub.callback;
  }

  _unsubscribe(sub: UnsubscriptionParams): void {

    if (!this.ws) throw Error('Websocket not initialised');

    const details = topics(sub.endpoint, sub.symbol);

    if (!(details.endpoint in this.subscriptions)) {
      throw Error('Not subscribed');
    }

    if (details.type === 'private') {
      this.ws.send(JSON.stringify({
        id: Date.now(),
        type: 'unsubscribe',
        topic: details.endpoint,
        privateChannel: true,
        response: true
      }));
      console.log("Unsubscribed from ", details.endpoint);
    } else {
      this.ws.send(JSON.stringify({
        id: Date.now(),
        type: 'unsubscribe',
        topic: details.endpoint,
        privateChannel: false,
        response: true
      }));
      console.log("Unsubscribed from ", details.endpoint);
    }

    delete this.subscriptions[details.endpoint];
    return;
  }

  async initialise(subs: SubscriptionParams[] = []): Promise<void> {
    const websocketPath: string = await this.getSocketEndpoint();
    this.ws = new ReconnectingWebSocket(websocketPath, [], { WebSocket: WS });
    this.ws.addEventListener('open', () => {
      console.log('Kucoin futures websocket opened');
      this.openingTime = new Date();
      this.heartbeat = setInterval(() => {
        this.ws?.send(JSON.stringify({ id: Date.now(), type: 'ping' }));
      }, 20000);
      for (const x of subs) {
        this._subscribe(x);
      }
    });
    this.ws.addEventListener('message', (msg) => {
      const data = JSON.parse(msg.data);
      if (data.topic && data.topic in this.subscriptions) {
        this.subscriptions[data.topic](data.data, data.topic);
      }
    });
    this.ws.addEventListener('close', () => {
      this.ws = undefined;
      clearInterval(this.heartbeat);
      console.log(`Kucoin Websocket closed, time opened: ${(new Date().getTime() - (this.openingTime ? this.openingTime.getTime() : new Date().getTime())) / 1000}`);
    });
  }

  async subscribe(subs: SubscriptionParams[]): Promise<void> {
    if (!this.ws) {
      await this.initialise();
    }

    for (const x of subs) {
      this._subscribe(x);
    }
  }

  async unsubscribe(subs: UnsubscriptionParams[]): Promise<void> {
    if (!this.ws) {
      throw Error('Impossible to unsubscribe, Websocket not opened');
    }

    for (const x of subs) {
      this._unsubscribe(x);
    }
  }

  async terminate(): Promise<void> {

    console.log('Terminate');
    if (!this.ws) {
      return;
    }
    this.ws.close();
  }

  async level2Depth5(symbol: string, callback: (msg: Level2Response, topic: string) => void): Promise<void> {
    await this.subscribe([{
      endpoint: WsSocketEndpoints.Depth5, symbol, callback
    }]);
  }

  async level2Depth50(symbol: string, callback: (msg: Level2Response, topic: string) => void): Promise<void> {
    await this.subscribe([{
      endpoint: WsSocketEndpoints.Depth50, symbol, callback
    }]);
  }

  async ordersMarket(symbol: string, callback: (msg: OrderChangeResponse, topic: string) => void): Promise<void> {
    await this.subscribe([{
      endpoint: WsSocketEndpoints.OrdersMarket, symbol, callback
    }]);
  }

  async position(symbol: string, callback: (msg: PositionChangePriceResponse | PositionChangeOperationResponse, topic: string) => void): Promise<void> {
    await this.subscribe([{
      endpoint: WsSocketEndpoints.Position, symbol, callback
    }]);
  }

  async execution(symbol: string, callback: (msg: ExecutionResponse, topic: string) => void): Promise<void> {
    await this.subscribe([{
      endpoint: WsSocketEndpoints.Execution, symbol, callback
    }]);
  }

}

export default KucoinFutureWebSocket;
