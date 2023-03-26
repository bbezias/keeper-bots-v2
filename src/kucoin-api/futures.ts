import KucoinFutureWebSocket, { WsParams } from './ws';
import { formatQuery, Sign, sign } from './utils';
import {
  ContractListResponse,
  ContractParams,
  ContractResponse, HistoricalDataParams, HistoricalDataResponse,
  KlineParams,
  KlineResponse, OrderBookParams, OrderBookResponse, StatusResponse, TickerParams, TickerResponse, TimeResponse
} from './market';
import axios from 'axios';
import { AccountOverviewParams, AccountOverviewResponse, TransactionHistoryParams, TransactionHistoryResponse } from './account';
import {
  CancelAllOrdersParams,
  CancelAllOrdersResponse,
  CancelOrderParams, CancelOrderResponse,
  FillsParams, FillsResponse, OpenOrderStatisticsParams, OpenOrderStatisticsResponse,
  OrderListParams,
  OrderListResponse,
  PlaceLimitOrderParams,
  PlaceMarketOrderParams,
  PlaceOrderResponse, PositionListResponse, PositionParams, PositionResponse, RecentDoneOrdersResponse, RecentFillsResponse,
  SingleOrderParams, SingleOrderResponse, StopOrderListParams, StopOrderListResponse
} from './trade';
import { logger } from '../logger';

export interface KucoinFutureApiArgs {
  environment: string;
  secretKey: string | undefined;
  apiKey: string | undefined;
  passphrase: string | undefined;
}

export class KucoinFutureApi {

  environment: string;
  baseUrl: string;
  secretKey: string | undefined;
  apiKey: string | undefined;
  passphrase: string | undefined;
  canSign = false;
  ws: KucoinFutureWebSocket

  constructor(config: KucoinFutureApiArgs) {

    let url = 'https://api-sandbox-futures.kucoin.com';
    if (config.environment === 'live') {
      url = 'https://api-futures.kucoin.com';
    }

    this.environment = config.environment;
    this.baseUrl = url;
    this.secretKey = config.secretKey;
    this.apiKey = config.apiKey;
    this.passphrase = config.passphrase;

    if (this.secretKey && this.apiKey && this.passphrase) {
      this.canSign = true;
      console.log('Api keys and passphrase provided, Can signed transactions');
    } else {
      console.log('Api keys and passphrase not provided, only public endpoints available');
    }

    const wsArgs: WsParams = {
      apiKey: this.apiKey,
      secretKey: this.secretKey, passphrase: this.passphrase,
      baseUrl: this.baseUrl, environment: this.environment
    };

    this.ws = new KucoinFutureWebSocket(wsArgs);

  }

  async kline(params: KlineParams): Promise<KlineResponse> {
    const endpoint = '/api/v1/kline/query';
    const url = this.baseUrl + endpoint + formatQuery(params);
    logger.debug(`kucoin query ${url}`);
    const result = await axios.get(url);
    return result.data;
  }

  async contractList(): Promise<ContractListResponse> {
    const endpoint = '/api/v1/contracts/active';
    const url = this.baseUrl + endpoint;
    logger.debug(`kucoin query ${url}`);
    const result = await axios.get(url);
    return result.data;
  }

  async contract(params: ContractParams): Promise<ContractResponse> {
    const endpoint = '/api/v1/contracts';
    const url = this.baseUrl + endpoint + '/' + params.symbol;
    logger.debug(`kucoin query ${url}`);
    const result = await axios.get(url);
    return result.data;
  }

  async time(): Promise<TimeResponse> {
    const endpoint = '/api/v1/timestamp';
    const url = this.baseUrl + endpoint;
    logger.debug(`kucoin query ${url}`);
    const result = await axios.get(url);
    return result.data;
  }

  async status(): Promise<StatusResponse> {
    const endpoint = '/api/v1/status';
    const url = this.baseUrl + endpoint;
    logger.debug(`kucoin query ${url}`);
    const result = await axios.get(url);
    return result.data;
  }

  async historicalData(params: HistoricalDataParams): Promise<HistoricalDataResponse> {
    const endpoint = '/api/v1/trade/history';
    const url = this.baseUrl + endpoint + formatQuery(params);
    logger.debug(`kucoin query ${url}`);
    const result = await axios.get(url);
    return result.data;
  }

  async ticker(params: TickerParams): Promise<TickerResponse> {
    const endpoint = '/api/v1/ticker';
    const url = this.baseUrl + endpoint + formatQuery(params);
    logger.debug(`kucoin query ${url}`);
    const result = await axios.get(url);
    return result.data;
  }

  async orderBook(params: OrderBookParams): Promise<OrderBookResponse> {
    const endpoint = '/api/v1/level2';

    const p = '?symbol=' + params.symbol;
    let url: string;
    if (params.depth) {
      url = this.baseUrl + endpoint + '/' + params.depth + p;
    } else {
      url = this.baseUrl + endpoint + '/snapshot' + p;
    }
    const result = await axios.get(url);
    return result.data;
  }

  async accountOverview(params: AccountOverviewParams): Promise<AccountOverviewResponse> {
    const endpoint = '/api/v1/account-overview';
    const url = this.baseUrl + endpoint + formatQuery(params);
    logger.debug(`kucoin query ${url}`);
    const result = await axios.get(url, this.sign(endpoint, 'GET', params));
    return result.data;
  }

  async transactionHistory(params: TransactionHistoryParams): Promise<TransactionHistoryResponse> {
    const endpoint = '/api/v1/transaction-history';
    const url = this.baseUrl + endpoint + formatQuery(params);
    logger.debug(`kucoin query ${url}`);
    const result = await axios.get(url, this.sign(endpoint, 'GET', params));
    return result.data;
  }

  async placeLimitOrder(params: PlaceLimitOrderParams): Promise<PlaceOrderResponse> {
    const endpoint = '/api/v1/orders';
    params.type = 'limit';
    const url = this.baseUrl + endpoint;
    logger.debug(`kucoin query ${url}`);
    const result = await axios.post(url, params, this.sign(endpoint, 'POST', params));
    return result.data;
  }

  async placeMarketOrder(params: PlaceMarketOrderParams): Promise<PlaceOrderResponse> {
    const endpoint = '/api/v1/orders';
    params.type = 'market';
    const url = this.baseUrl + endpoint;
    logger.debug(`kucoin query ${url}`);
    const result = await axios.post(url, params, this.sign(endpoint, 'POST', params));
    return result.data;
  }

  async cancelOrder(params: CancelOrderParams): Promise<CancelOrderResponse> {
    const endpoint = '/api/v1/orders' + '/' + params.orderId;
    const url = this.baseUrl + endpoint;
    logger.debug(`kucoin query ${url}`);
    const result = await axios.delete(url, this.sign(endpoint, 'DELETE', {}));
    return result.data;
  }

  async cancelAllOrder(params: CancelAllOrdersParams): Promise<CancelAllOrdersResponse> {
    const endpoint = '/api/v1/orders';
    const url = this.baseUrl + endpoint + formatQuery(params);
    logger.debug(`kucoin query ${url}`);
    const result = await axios.delete(url, this.sign(endpoint, 'DELETE', params));
    return result.data;
  }


  async orderList(params: OrderListParams): Promise<OrderListResponse> {
    const endpoint = '/api/v1/orders';
    const url = this.baseUrl + endpoint + formatQuery(params);
    logger.debug(`kucoin query ${url}`);
    const result = await axios.get(url, this.sign(endpoint, 'GET', params));
    return result.data;
  }

  async stopOrderList(params: StopOrderListParams): Promise<StopOrderListResponse> {
    const endpoint = '/api/v1/stopOrders';
    const url = this.baseUrl + endpoint + formatQuery(params);
    logger.debug(`kucoin query ${url}`);
    const result = await axios.get(url, this.sign(endpoint, 'GET', params));
    return result.data;
  }

  async recentDoneOrders(): Promise<RecentDoneOrdersResponse> {
    const endpoint = '/api/v1/recentDoneOrders';
    const url = this.baseUrl + endpoint;
    logger.debug(`kucoin query ${url}`);
    const result = await axios.get(url, this.sign(endpoint, 'GET', {}));
    return result.data;
  }

  async singleOrder(params: SingleOrderParams): Promise<SingleOrderResponse> {
    const endpoint = '/api/v1/orders';
    const url = this.baseUrl + endpoint + formatQuery(params);
    logger.debug(`kucoin query ${url}`);
    const result = await axios.get(url, this.sign(endpoint, 'GET', params));
    return result.data;
  }

  async fills(params: FillsParams): Promise<FillsResponse> {
    const endpoint = '/api/v1/fills';
    const url = this.baseUrl + endpoint + formatQuery(params);
    logger.debug(`kucoin query ${url}`);
    const result = await axios.get(url, this.sign(endpoint, 'GET', params));
    return result.data;
  }

  async recentFills(): Promise<RecentFillsResponse> {
    const endpoint = '/api/v1/recentFills';
    const url = this.baseUrl + endpoint;
    logger.debug(`kucoin query ${url}`);
    const result = await axios.get(url, this.sign(endpoint, 'GET', {}));
    return result.data;
  }

  async openOrderStatistics(params: OpenOrderStatisticsParams): Promise<OpenOrderStatisticsResponse> {
    const endpoint = '/api/v1/openOrderStatistics';
    const url = this.baseUrl + endpoint + formatQuery(params);
    logger.debug(`kucoin query ${url}`);
    const result = await axios.get(url, this.sign(endpoint, 'GET', params));
    return result.data;
  }

  async position(params: PositionParams): Promise<PositionResponse> {
    const endpoint = '/api/v1/position';
    const url = this.baseUrl + endpoint + formatQuery(params);
    logger.debug(`kucoin query ${url}`);
    const result = await axios.get(url, this.sign(endpoint, 'GET', params));
    return result.data;
  }

  async positionList(): Promise<PositionListResponse> {
    const endpoint = '/api/v1/positions';
    const url = this.baseUrl + endpoint;
    logger.debug(`kucoin query ${url}`);
    const result = await axios.get(url, this.sign(endpoint, 'GET', {}));
    return result.data;
  }

  sign(endpoint: string, method: string, params: any): { [key: string]: any } {

    if (!this.apiKey || !this.secretKey || !this.passphrase) {
      throw Error('Keys missing, impossible to sign transaction');
    }

    const p: Sign = {
      apiKey: this.apiKey, secretKey: this.secretKey, passphrase: this.passphrase,
      method, endpoint, params
    };

    return sign(p);
  }
}

export default KucoinFutureApi;
