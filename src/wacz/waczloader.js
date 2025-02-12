import yaml from "js-yaml";

import { MAX_FULL_DOWNLOAD_SIZE } from "../utils";
import { WARCLoader } from "../warcloader";
import { ZipRangeReader } from "./ziprangereader";

export const MAIN_PAGES_JSON = "pages/pages.jsonl";
export const EXTRA_PAGES_JSON = "pages/extraPages.jsonl";

const PAGE_BATCH_SIZE = 500;


// ============================================================================
export class SingleWACZLoader
{
  constructor(loader, config, loadId = null) {
    this.loader = loader;
    this.config = config;
    this.loadId = loadId;

    this.canLoadOnDemand = config.onDemand;

    this.zipreader = null;

    this.waczname = config.loadUrl;
  }

  async load(db, progressUpdate, fullTotalSize) {
    this.zipreader = db.zipreader ? db.zipreader : new ZipRangeReader(this.loader);

    const entries = await this.zipreader.load(true);

    //todo: a bit hacky, store the full arrayBuffer for blob loader
    // if size less than MAX_FULL_DOWNLOAD_SIZE
    if (this.canLoadOnDemand) {
      if (db.fullConfig && this.loader.arrayBuffer &&
        this.loader.arrayBuffer.byteLength <= MAX_FULL_DOWNLOAD_SIZE) {
        if (!db.fullConfig.extra) {
          db.fullConfig.extra = {};
        }
        db.fullConfig.extra.arrayBuffer = this.loader.arrayBuffer;
      }
    }

    let metadata;

    if (entries["datapackage.json"]) {
      metadata = await this.loadMetadata(db, entries, "datapackage.json");
    } else if (entries["webarchive.yaml"]) {
      metadata = await this.loadMetadataYAML(db, entries, "webarchive.yaml");
    }

    if (this.canLoadOnDemand) {
      // just add wacz file here
      await db.addWACZFile(this.waczname, entries);
    } else {

      await this.loadWACZFull(db, entries, progressUpdate, fullTotalSize);
    }

    return metadata || {};
  }

  async loadWACZFull(db, entries, progressUpdateCallback = null, fullTotalSize = 0) {
    let offsetTotal = 0;

    const progressUpdate = (percent, error, offset/*, total*/) => {
      offset += offsetTotal;
      if (progressUpdateCallback && fullTotalSize) {
        progressUpdateCallback(Math.round(offset * 100.0 / fullTotalSize), null, offset, fullTotalSize);
      }
    };

    // load CDX and IDX
    for (const filename of Object.keys(entries)) {
      const entryTotal = this.zipreader.getCompressedSize(filename);
      if (filename.endsWith(".warc.gz") || filename.endsWith(".warc")) {
        await this.loadWARC(db, filename, progressUpdate, entryTotal);
      }

      offsetTotal += entryTotal;
    }
  }

  async loadPages(db, filename = MAIN_PAGES_JSON) {
    const reader = await this.zipreader.loadFile(filename, {unzip: true});

    let pageListInfo = null;

    let pages = [];

    for await (const textLine of reader.iterLines()) {
      const page = JSON.parse(textLine);

      if (!pageListInfo) {
        pageListInfo = page;
        continue;
      }

      pages.push(page);

      if (pages.length === PAGE_BATCH_SIZE) {
        await db.addPages(pages);
        pages = [];
      }
    }

    if (pages.length) {
      await db.addPages(pages);
    }

    return pageListInfo;
  }

  async loadWARC(db, filename, progressUpdate, total) {
    const reader = await this.zipreader.loadFile(filename, {unzip: true});

    const loader = new WARCLoader(reader, null, filename);
    loader.detectPages = false;

    return await loader.load(db, progressUpdate, total);
  }

  async loadTextEntry(db, filename) {
    const reader = await this.zipreader.loadFile(filename);
    const text = new TextDecoder().decode(await reader.readFully());
    return text;
  }

  // New WACZ 1.0.0 Format
  async loadMetadata(db, entries, filename) {
    const text = await this.loadTextEntry(db, filename);

    const root = JSON.parse(text);

    if (root.config !== undefined && db.initConfig) {
      db.initConfig(root.config);
    }

    const metadata = root.metadata || {};

    // All Pages
    if (entries[MAIN_PAGES_JSON]) {
      const pageInfo = await loadPages(db, this.zipreader, this.waczname, MAIN_PAGES_JSON);

      if (pageInfo.hasText) {
        db.textIndex = metadata.textIndex = MAIN_PAGES_JSON;
      }
    }

    if (entries[EXTRA_PAGES_JSON]) {
      db.textIndex = metadata.textIndex = EXTRA_PAGES_JSON;
    }

    return metadata;
  }

  // Old WACZ 0.1.0 Format
  async loadMetadataYAML(db, entries, filename) {
    const text = await this.loadTextEntry(db, filename);

    const root = yaml.load(text);

    const metadata = {
      desc: root.desc,
      title: root.title
    };

    if (root.textIndex) {
      metadata.textIndex = root.textIndex;
      if (!root.config) {
        root.config = {};
      }
      root.config.textIndex = root.textIndex;
    }

    if (root.config !== undefined) {
      db.initConfig(root.config);
    }

    if (!metadata.title) {
      metadata.title = this.config.sourceName;
    }

    // All pages
    const pages = root.pages || [];

    if (pages && pages.length) {
      await db.addPages(pages);
    } else {
      if (entries["pages.csv"]) {
        await db.loadPagesCSV(db, "pages.csv");
      }
    }

    // Curated Pages
    const pageLists = root.pageLists || [];

    if (pageLists && pageLists.length) {
      await db.addCuratedPageLists(pageLists, "pages", "show");
    }

    return metadata;
  }
}

// ==========================================================================
export class JSONMultiWACZLoader
{
  constructor(json, baseUrl) {
    this.json = json;
    this.baseUrl = baseUrl;
  }

  async load(db)  {
    const metadata = {
      title: this.json.title,
      desc: this.json.description
    };

    const files = this.loadFiles(this.baseUrl);

    await db.syncWACZ(files);

    return metadata;
  }

  loadFiles() {
    return this.json.resources.map((res) => {
      return new URL(res.path, this.baseUrl).href;
    });
  }
}

// ==========================================================================
export async function loadPages(db, zipreader, waczname, filename = MAIN_PAGES_JSON) {
  const reader = await zipreader.loadFile(filename, {unzip: true});

  let pageListInfo = null;

  let pages = [];

  for await (const textLine of reader.iterLines()) {
    const page = JSON.parse(textLine);

    page.wacz = waczname;

    if (!pageListInfo) {
      pageListInfo = page;
      continue;
    }

    pages.push(page);

    if (pages.length === PAGE_BATCH_SIZE) {
      await db.addPages(pages);
      pages = [];
    }
  }

  if (pages.length) {
    await db.addPages(pages);
  }

  return pageListInfo;
}