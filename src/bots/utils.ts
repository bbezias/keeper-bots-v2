import { KlineResponse } from '../kucoin-api/market';

export enum StateType {
  /** Flat there is no open position */
  NEUTRAL = 'neutral',
  NOT_STARTED = 'not_started',

  /** Long position on this market */
  LONG = 'long',

  /** Short position on market */
  SHORT = 'short',

  /** Current closing a long position (shorts only) */
  CLOSING_LONG = 'closing-long',

  /** Current closing a short position (long only) */
  CLOSING_SHORT = 'closing-short',
}

export enum KUCOIN_CONTRACTS {
  sol = 'SOLUSDTM',
  btc = 'XBTUSDTM',
  eth = 'ETHUSDTM',
  apt = 'APTUSDTM',
  matic = 'MATICUSDTM'
}

export const INDEX_TO_NAME = {
  0: 'Solana',
  1: 'Bitcoin',
  2: 'Ethereum',
  3: 'Aptos',
  5: 'Polygon'
};

export const INDEX_TO_LETTERS = {
  0: 'sol',
  1: 'btc',
  2: 'eth',
  3: 'apt',
  5: 'matic'
};

export const SYMBOL_TO_INDEX = {
  SOLUSDTM: 0,
  XBTUSDTM: 1,
  ETHUSDTM: 2,
  APTUSDTM: 3,
  MATICUSDTM: 5
};

export const INDEX_TO_SYMBOL = {
  0: KUCOIN_CONTRACTS.sol,
  1: KUCOIN_CONTRACTS.btc,
  2: KUCOIN_CONTRACTS.eth,
  3: KUCOIN_CONTRACTS.apt,
  5: KUCOIN_CONTRACTS.matic
};

export class MaxSizeList {
  private readonly maxSize: number;
  private readonly list: number[];

  public getAverage(): number {
    if (this.list.length === 0) {
      return 0;
    }
    const sum = this.list.reduce((accumulator, currentValue) => accumulator + currentValue);
    return sum / this.list.length;
  }

  constructor(maxSize: number) {
    this.maxSize = maxSize;
    this.list = [];
  }

  public add(item: number): void {
    if (this.list.length === this.maxSize) {
      this.list.shift(); // Remove first element
    }
    this.list.push(item);
  }

  public getItems(): number[] {
    return this.list;
  }

  public getVolatility(): number {
    if (this.list.length < 2) {
      return 0;
    }

    // Calculate the price returns
    const returns = this.list.slice(1).map((price, index) => (price - this.list[index]) / this.list[index]);

    // Calculate the mean return
    const meanReturn = returns.reduce((accumulator, currentValue) => accumulator + currentValue) / returns.length;

    // Calculate the variance of the returns
    const variance = returns.reduce((accumulator, currentValue) => {
      const diff = currentValue - meanReturn;
      return accumulator + diff * diff;
    }, 0) / (returns.length - 1);

    // Calculate the standard deviation (volatility)
    return Math.sqrt(variance);
  }

}

interface PriceData {
    time: number;
    entryPrice: number;
    highestPrice: number;
    lowestPrice: number;
    closePrice: number;
    tradingVolume: number;
}

export function calculateVWCP(apiData: KlineResponse): number {

    let sumClosePriceVolume = 0;
    let sumVolume = 0;

    for (const item of apiData.data) {
        sumClosePriceVolume += item[4] * item[5];
        sumVolume += item[5];
    }

    if (sumVolume === 0) {
        throw new Error("Total trading volume is zero.");
    }

    return sumClosePriceVolume / sumVolume;
}

export function calculateVolatility(apiData: KlineResponse): number {

    const n = apiData.data.length;

    if (n < 2) {
        throw new Error("Not enough data to calculate volatility.");
    }

    let sumClosePrice = 0;
    let sumClosePriceSquared = 0;

    for (const item of apiData.data) {
        sumClosePrice += item[4];
        sumClosePriceSquared += item[4] * item[4];
    }

    const meanClosePrice = sumClosePrice / n;
    const meanClosePriceSquared = sumClosePriceSquared / n;

    const variance = meanClosePriceSquared - meanClosePrice * meanClosePrice;
    return Math.sqrt(variance);
}