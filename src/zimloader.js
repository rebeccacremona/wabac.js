//import { BaseAsyncIterReader } from 'warcio';
import { xzdec } from './xzdec';

const BATCH_SIZE = 1000;
let inited = false;

// ===========================================================================
class ZIMIndexLoader
{
  constructor(reader, prefix, mimeTypes) {
    this.reader = reader;
    this.mimeTypes = mimeTypes;
    this.prefix = prefix;

    this.batch = [];
    this.promises = [];
    this.count = 0;

    if (!inited) {
      xzdec._init();
      inited = true;
    }
  }

  addResource(res) {
    //this.promises.push(this.db.addResource(res));

    if (this.batch.length >= BATCH_SIZE) {
      this.promises.push(this.db.addResources(this.batch));
      this.batch = [];
      console.log(`Read ${this.count += BATCH_SIZE} records`);
    }

    this.batch.push(res);
  }

  async finishIndexing() {
    if (this.batch.length > 0) {
      this.promises.push(this.db.addResources(this.batch));
    }

    console.log(`Indexed ${this.count += this.batch.length} records`);

    try {
      await Promise.all(this.promises);
    } catch (e) {
      console.warn(e.toString());
    }
    this.promises = [];
  }

  async load(db) {
    this.db = db;

    let reader = this.reader;

    if (!reader.iterLines) {
      reader = new AsyncIterReader(this.reader);
    }

    for await (const origLine of reader.iterLines()) {
      let cdx;
      let url;
      let line = origLine.trimEnd();

      console.log(line);

      if (!line.startsWith("{")) {
        const inx = line.indexOf(" {");
        if (inx < 0) {
          continue;
        }
        url = this.prefix + line.substring(0, inx).trim();
        line = line.slice(inx);
      }

      try {
        cdx = JSON.parse(line);
      } catch (e) {
        console.log("JSON Parser error on: " + line);
        continue;
      }

      const mime = this.mimeTypes[cdx.mime] || "text/html";
      let status = 200;
      let redirect = undefined;

      if (cdx.redirect) {
        status = 302;
        redirect = this.prefix + cdx.redirect;
      }

      const ts = new Date().getTime();

      // use url as digest?
      const digest = url;
  
      const source = !cdx.redirect ? {start: Number(cdx.offset), length: Number(cdx.length)} : {};

      if (cdx.d_off && cdx.d_len) {
        source.d_off = cdx.d_off;
        source.d_len = cdx.d_len;
      }
  
      const entry = {url, ts, status, digest, redirect, mime, loaded: false, source};

      this.addResource(entry);
    }

    await this.finishIndexing();
  }
}

// ===========================================================================
class ZIMLoader {
  constructor(cluster, cdx, prefix) {
    this.cluster = cluster;

    this.cdx = cdx;
    this.prefix = prefix || "";

    this.compressed = (cdx.source.d_off && cdx.source.d_len);
  }

  async load() {
    const cdx = this.cdx;

    const url = cdx.url;
    const ts = cdx.ts;
    const status = cdx.status;
    const mime = cdx.mime;
    const respHeaders = {};

    let payload = undefined;

    if (this.cdx.redirect) {
      respHeaders["Location"] = this.cdx.redirect;
      payload = new Uint8Array([]);
    } else {
      if (this.compressed) {
        try {
          payload = await this.readDecomp();
        } catch (e) {
          console.warn(e);
        }
      } else {
        payload = this.cluster;
      }

    }
    respHeaders["Content-Type"] = mime;

    const digest = cdx.digest;

    const entry = {url, ts, status, mime, digest, respHeaders, payload};

    return entry;
  }

  async readDecomp() {
    const offset = this.cdx.source.d_off;
    const length = this.cdx.source.d_len;
  
    const realOutBuffer = new Uint8Array(new ArrayBuffer(length));
    let outBufferPos = 0;
    let outStreamPos = 0;

    const data = this.cluster;
    console.log(`Read ${data.length} Expected: ${this.cdx.source.length}`);
  
    const zh = xzdec._init_decompression(data.length);
  
    if (xzdec._input_empty(zh)) {  
      xzdec.HEAPU8.set(data, xzdec._get_in_buffer(zh));
      xzdec._set_new_input(zh, data.length);
    }

    let finished = false;

    while (!finished) {
      
      const res = xzdec._decompress(zh);
      console.log("xzdec res", res);
  
      if (res === 0) {
        finished = false;
      } else if (res === 1) {
        finished = true;
      } else {
        //error
        finished = true;
      }
  
      const outPos = xzdec._get_out_pos(zh);
  
      if (outPos > 0 && outStreamPos + outPos >= offset) {
        const outBuffer = xzdec._get_out_buffer(zh);
        let copyStart = offset - outStreamPos;
        if (copyStart < 0) {
          copyStart = 0;
        }

        // for (let i = copyStart; i < outPos && outBufferPos < realOutBuffer.length; i++) {
        //   realOutBuffer[outBufferPos++] = xzdec.HEAPU8[outBuffer + i];
        // }
        
        const copyLen = Math.min(outPos - copyStart, realOutBuffer.length - outBufferPos);
        realOutBuffer.set(xzdec.HEAPU8.slice(outBuffer + copyStart, outBuffer + copyStart + copyLen), outBufferPos);
        outBufferPos += copyLen;
      }
  
      outStreamPos += outPos;
  
      if (outPos > 0) {
        xzdec._out_buffer_cleared(zh);
      }
  
      if (finished || outStreamPos >= offset + length) {
        break;
      }

      //await new Promise((resolve) => setTimeout(resolve, 0));
    }
  
    xzdec._release(zh);

    return realOutBuffer;
  }
}

export { ZIMIndexLoader, ZIMLoader };