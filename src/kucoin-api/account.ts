export interface AccountOverviewParams {
  currency?: string;
}

export interface AccountOverviewResponse {
  code: string;
  data: {
    accountEquity: number,
    unrealisedPNL: number,
    marginBalance: number,
    positionMargin: number,
    orderMargin: number,
    frozenFunds: number,
    availableBalance: number,
    currency: string
  }
}

export interface TransactionHistoryParams {
  startAt?: number;
  endAt?: number;
  type?: 'RealisedPNL' | 'Deposit' | 'Withdrawal' | 'TransferIn' | 'TransferOut';
  offset?: number;
  maxCount?: number;
  currency?: 'XBT' | 'USDT';
  forward?: boolean;
}

export interface TransactionHistoryResponse {
  code: string;
  data: {
    hasMode: number;
    dataList: {
      time: number,
      type: string,
      amount: number,
      fee: number | null,
      accountEquity: number,
      status: string,
      remark: string,
      offset: number,
      currency: string
    }[]
  }
}
