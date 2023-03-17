import crypto from 'crypto';
import qs from 'qs';

export interface Sign {
  endpoint: string;
  method: string;
  params: { [key: string]: any };
  apiKey: string;
  secretKey: string;
  passphrase: string;
}

export function formatQuery(queryObj: any) {
  if (queryObj) {
    const p = qs.stringify(queryObj);
    if (p.length > 2) {
      return '?' + p;
    } else {
      return '';
    }
  } else {
    return '';
  }
}

export function sign(params: Sign): { [key: string]: any } {
  const header: { [key: string]: any } = {
    headers: {
      'Content-Type': 'application/json'
    }
  };
  const nonce = Date.now() + '';
  let strForSign: string;
  if (params.method === 'GET' || params.method === 'DELETE') {
    strForSign = nonce + params.method + params.endpoint + formatQuery(params.params);
  } else {
    strForSign = nonce + params.method + params.endpoint + JSON.stringify(params.params);
  }
  const signatureResult = crypto.createHmac('sha256', params.secretKey)
  .update(strForSign)
  .digest('base64');
  const passphraseResult = crypto.createHmac('sha256', params.secretKey)
  .update(params.passphrase)
  .digest('base64');
  header.headers['KC-API-SIGN'] = signatureResult;
  header.headers['KC-API-TIMESTAMP'] = nonce;
  header.headers['KC-API-KEY'] = params.apiKey;
  header.headers['KC-API-PASSPHRASE'] = passphraseResult;
  header.headers['KC-API-KEY-VERSION'] = 2;
  return header;
}
