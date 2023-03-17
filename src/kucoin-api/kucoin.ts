import { Order, OrderBook, Position, PositionPriceChange, Trades } from './models';
import KucoinFutureApi, { KucoinFutureApiArgs } from '../kucoin-api/futures';
import chalk from 'chalk';
import EventEmitter from 'events';
import { PositionChangeOperationResponse, PositionChangePriceResponse } from './ws';
import ReconnectingWebSocket from 'reconnecting-websocket';
import { logger } from '../logger';


export class KucoinController extends EventEmitter {

  handlers: { [symbol: string]: { lastRcv: number, ready: boolean, book?: OrderBook } } = {};
  api: KucoinFutureApi;
  intervals: NodeJS.Timer[] = [];

  websocketHasBeenOpen = false;

  constructor() {

    super();

    console.log('Starting Kucoin initialisation...');

    const apiKey = process.env.KUCOIN_API_KEY as string;
    const secretKey = process.env.KUCOIN_SECRET_KEY as string;
    const passphrase = process.env.KUCOIN_PASSPHRASE as string;
    const params: KucoinFutureApiArgs = { apiKey, secretKey, passphrase, environment: 'live' };

    this.api = new KucoinFutureApi(params);
    if (!this.api.canSign) throw Error('Impossible to sign transaction on Kucoin due to missing keys');
    logger.info(`Kucoin configured on ${params.environment}`);
    this.intervals.push(setInterval(this.checkIfResubscribe.bind(this), 10000));

  }

  async checkIfResubscribe(): Promise<void> {
    const now = (new Date()).getTime();

    if (!this.websocketHasBeenOpen) return;

    for (const [symbol, value] of Object.entries(this.handlers)) {
      if (value.lastRcv + 10000 < now) {
        value.ready = false;
        if (this.api.ws.ws.readyState === ReconnectingWebSocket.OPEN) {
          logger.warn(`Websocket subscription seems lost for ${symbol}, trying to resubscribe`);
          await this.subscribe(symbol);
        } else {
          logger.warn(`Websocket subscription seems lost for ${symbol}, trying to reset the websocket`);
        }
      }
    }
  }

  async initialise(): Promise<void> {
    await this.api.ws.terminate();
    await this.api.ws.initialise();
    console.log("Websocket opened with kucoin");
  }

  async subscribe(symbol: string): Promise<void> {

    if (!(symbol in this.handlers)) {
      this.handlers[symbol] = { lastRcv: 0, ready: false };
    }

    if (!this.api.ws.ws || this.api.ws.ws.readyState === ReconnectingWebSocket.OPEN) {
      throw new Error("First you need to open the socket");
    }

    setTimeout(() => {
      this.api.ws.level2Depth50(symbol, (msg) => {
        const od = new OrderBook('kucoin', symbol, msg.asks, msg.bids);
        const handler = this.handlers[symbol];
        handler.lastRcv = (new Date()).getTime();
        handler.ready = true;
        handler.book = od;
        this.emit('book', { source: 'kucoin', data: od });
      }).catch(e => {
        console.log(chalk.red('error kucoin', e));
      });
    }, 1000);

    setTimeout(() => {
      this.api.ws.ordersMarket(symbol, (msg) => {
        // Convert size in lot to size of notional
        const size = +msg.size;
        const matchSize = +msg.filledSize;

        if (!msg.price) return;

        const o: Order = {
          orderId: msg.orderId,
          exchange: 'kucoin',
          price: msg.price,
          side: msg.side,
          orderType: 'unknown',
          symbol: msg.symbol,
          timestamp: msg.ts / 1000000,
          status: msg.status,
          size: size.toPrecision(3),
          lotSize: msg.size,
          matchSize: matchSize.toPrecision(3),
          matchLotSize: msg.matchSize ? msg.matchSize : '0'
        };
        this.emit('order', o);
      });
    }, 1400);

    setTimeout(() => {
      this.api.ws.position(symbol, (msg) => {
        if ((msg as PositionChangeOperationResponse).changeReason !== 'markPriceChange') {
          const x = msg as PositionChangeOperationResponse;
          const size = x.currentQty;
          const p: Position = {
            currentQty: size,
            symbol: x.symbol,
            realisedPnl: x.realisedPnl,
            unrealisedPnl: x.unrealisedPnl,
            markPrice: x.markPrice,
            avgEntryPrice: x.avgEntryPrice,
            markValue: x.markValue,
            exchange: 'kucoin',
            unrealisedPnlPcnt: x.unrealisedPnlPcnt,
            unrealisedCost: x.unrealisedCost,
            realisedCost: x.realisedCost
          };
          this.emit('positionOperationChange', p);
        } else {
          const x = msg as PositionChangePriceResponse;
          const p: PositionPriceChange = {
            markPrice: x.markPrice,
            exchange: 'kucoin',
            markValue: x.markValue,
            symbol: x.symbol,
            unrealisedPnl: x.unrealisedPnl,
            unrealisedPnlPcnt: x.unrealisedPnlPcnt
          };
          this.emit('positionPriceChange', p);
        }

      });
    }, 1800);

    setTimeout(() => {
      this.api.ws.execution(symbol, (msg) => {
        const trades: Trades = {
          trades: [msg], symbol: symbol, source: 'kucoin', datetime: new Date()
        };
        this.emit('trades', { source: 'kucoin', data: trades });
      });
    }, 1200);

    this.websocketHasBeenOpen = true;
  }

  exit(): void {
    this.api.ws.terminate().then();
    this.websocketHasBeenOpen = false;
    for (const [symbol, value] of Object.entries(this.handlers)) {
      value.ready = false;
      logger.info(`${symbol} unsubscribed from Kucoin`);
    }
    return;
  }

}
