import KucoinFutureApi, { KucoinFutureApiArgs } from './futures';
import axios, { AxiosError } from 'axios';


const config: KucoinFutureApiArgs = {
  environment: process.env.ENVIRONMENT as string,
  apiKey: process.env.API_KEY as string,
  secretKey: process.env.SECRET_KEY as string,
  passphrase: process.env.PASSPHRASE as string,
};

const api = new KucoinFutureApi(config);
// api.kline({symbol: 'SOLUSDTM', granularity: 1}).then((r) => console.log(r));
// api.contractList({}).then((r) => console.log(r.data[0]));
// api.contract({symbol: 'SOLUSDTM'}).then((r) => console.log(r));
// api.status().then((r) => console.log(r));
// api.orderBook({symbol: 'SOLUSDTM', depth: 'depth20'}).then((r) => console.log(r.data.bids));
// api.accountOverview({currency: 'USDT'}).then((r) => console.log(r));
// api.transactionHistory({}).then((r) => console.log(r));
// api.placeLimitOrder({type: 'limit', symbol: 'SOLUSDTM', leverage: 20,
//   price: '20', side: 'buy', size: 1, clientOid: 'testApi'
// }).then((r) => console.log(r)).catch((e: Error | AxiosError) => {
//   if (axios.isAxiosError(e)) {
//     console.log(e.message, e.name);
//   } else {
//     console.log(e);
//   }
// });

// api.placeMarketOrder({symbol: 'SOLUSDTM', leverage: 20,
//   side: 'buy', size: 1, clientOid: Date.now().toString()
// }).then((r) => console.log(r)).catch((e: Error | AxiosError) => {
//   if (axios.isAxiosError(e)) {
//     console.log(e.message, e.name, e.stack);
//   } else {
//     console.log(e);
//   }
// });

// api.singleOrder({clientOid: '1657621037307'}).then((r) => console.log(r.data)).catch((e: Error | AxiosError) => {
//   if (axios.isAxiosError(e)) {
//     console.log(e.message, e.name, e.stack);
//   } else {
//     console.log(e);
//   }
// });

// api.openOrderStatistics({ symbol: 'SOLUSDTM'}).then((r) => console.log(r.data)).catch((e: Error | AxiosError) => {
//   if (axios.isAxiosError(e)) {
//     console.log(e.message, e.name, e.stack);
//   } else {
//     console.log(e);
//   }
// });
// api.positionList().then((r) => console.log(r.data)).catch((e: Error | AxiosError) => {
//   if (axios.isAxiosError(e)) {
//     console.log(e.message, e.name, e.stack);
//   } else {
//     console.log(e);
//   }
// });

// api.orderList({status: 'active'}).then((r) => console.log(r.data)).catch((e: Error | AxiosError) => {
//   if (axios.isAxiosError(e)) {
//     console.log(e.message, e.name, e.stack);
//   } else {
//     console.log(e);
//   }
// });

api.cancelOrder({orderId: '62cd48bf6b21070001a44063'}).then((r) => console.log(r.data)).catch((e: Error | AxiosError) => {
  if (axios.isAxiosError(e)) {
    console.log(e.message, e.name, e.stack);
  } else {
    console.log(e);
  }
});


//
// api.ws.subscribe([{
//   endpoint: WsSocketEndpoints.Balances, symbol: undefined, callback: (msg, topic) => {
//     console.log('balance from ', topic, msg);
//   }
// }]).then();
//
// setTimeout(() => {
//   api.ws.execution('SOLUSDTM', (msg, topic) => {
//       console.log('message from ', topic, msg)
//   }).then();
// }, 3000);
//
// setTimeout(() => {
//   api.ws.unsubscribe([{ endpoint: WsSocketEndpoints.Execution, symbol: 'SOLUSDTM' }]).then();
// }, 4000);
//
// setTimeout(() => {
//   api.ws.terminate().then();
// }, 6000);
//
// setTimeout(() => {
//   api.ws.ordersMarket('SOLUSDTM', (msg, topic) => {
//       console.log('message from ', topic, msg)
//   }).then();
// }, 8000);
//
// setTimeout(() => {
//   api.ws.level2Depth5('SOLUSDTM', (msg, topic) => {
//       console.log('message from ', topic, msg)
//   }).then();
// }, 10000);
