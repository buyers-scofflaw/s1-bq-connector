// index.js — S1 (ad_widget_daily) → GCS → BigQuery (hourly-friendly)

import fetch from 'node-fetch';
import { BigQuery } from '@google-cloud/bigquery';
import { Storage } from '@google-cloud/storage';

/* ========== ENV ========== */
const HOST        = process.env.S1_HOST || 'https://reports.system1.com';
const REPORT_TYPE = process.env.S1_REPORT_TYPE || 'syndication_rsoc_online_ad_widget_daily';
const DAYS        = process.env.S1_DAYS || '1';        // daily grain
const DATE        = process.env.S1_DATE || '';         // YYYY-MM-DD or blank (today)

const AUTH_KEY    = process.env.S1_AUTH_KEY;           // single-site key
const SITE_NAME   = process.env.SITE_NAME || 'site';   // label for folder & final table

const BQ_PROJECT  = process.env.BQ_PROJECT;
const BQ_DATASET  = process.env.BQ_DATASET || 'rsoc_clicks';
const BQ_FINAL    = process.env.BQ_TABLE   || 's1_ad_widget_daily'; // destination table (partitioned by date)

// Robust bucket handling: accept either GCS_BUCKET (with or w/o gs://) or GCS_BUCKET_NAME
const RAW_BUCKET  = process.env.GCS_BUCKET || process.env.GCS_BUCKET_NAME;
const BUCKET_NAME = RAW_BUCKET ? RAW_BUCKET.replace(/^gs:\/\//, '') : '';
const GCS_PREFIX  = process.env.GCS_PREFIX || 's1';    // folder under the bucket

/* ========== CLIENTS ========== */
const bq = new BigQuery({ projectId: BQ_PROJECT });
const storage = new Storage();

/* ========== HELPERS ========== */
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function targetYMD() {
  if (DATE) return DATE;                // fixed backfill date, if provided
  const d = new Date();                 // otherwise: "today" (UTC) for hourly refresh
  return d.toISOString().slice(0,10);
}

async function requestReport(auth_key) {
  const qs = new URLSearchParams({ report_type: REPORT_TYPE, days: String(DAYS), auth_key });
  if (DATE) qs.set('date', DATE);
  const url = `${HOST}/partner/v1/report?${qs}`;
  const res = await fetch(url, { method: 'POST' });
  if (!res.ok) throw new Error(`Report request failed ${res.status}`);
  const j = await res.json();
  if (!j.report_id) throw new Error(`No report_id`);
  return j.report_id;
}

async function pollStatus(reportId, auth_key) {
  const url = `${HOST}/partner/v1/report/${encodeURIComponent(reportId)}/status?auth_key=${encodeURIComponent(auth_key)}`;
  let tries = 0;
  while (tries < 60) { // up to ~30 minutes
    const res = await fetch(url);
    if (res.status === 429) {
      const ra = Number(res.headers.get('Retry-After') || 30);
      await sleep((ra + 1) * 1000);
      continue;
    }
    if (!res.ok) throw new Error(`Status failed ${res.status}`);
    const j = await res.json();
    const s = (j.report_status || '').toUpperCase();
    if (s === 'SUCCESS' && j.content_url) {
      let cu = j.content_url;
      if (!cu.includes('auth_key=')) {
        cu += (cu.includes('?') ? '&' : '?') + `auth_key=${encodeURIComponent(auth_key)}`;
      }
      return cu.startsWith('http') ? cu : `${HOST}${cu}`;
    }
    if (s === 'FAILED') throw new Error('Report FAILED');
    if (s === 'RUNNING') {
      await sleep(30 * 1000);
      tries++;
      continue;
    }
    throw new Error(`Unexpected status: ${JSON.stringify(j)}`);
  }
  throw new Error('Timed out waiting for SUCCESS');
}

// Download contentUrl → GCS (bucketName + objectPath are passed separately)
async function downloadToGCS(contentUrl, bucketName, objectPath) {
  const res = await fetch(contentUrl, { redirect: 'follow' });
  if (!res.ok) throw new Error(`Download failed ${res.status}`);

  const file = storage.bucket(bucketName).file(objectPath);
  await new Promise((resolve, reject) => {
    res.body.pipe(file.createWriteStream({ resumable: false }))
      .on('finish', resolve)
      .on('error', reject);
  });
  console.log(`[debug] downloaded to gs://${bucketName}/${objectPath}`);
}

// BigQuery LOAD job that reads directly from GCS (no local fs)
async function loadCsvGzToBQ(gcsUri) {
  const location = 'US'; // adjust if your dataset is in another location
  const jobConfig = {
    location,
    configuration: {
      load: {
        destinationTable: {
          projectId: BQ_PROJECT,
          datasetId: BQ_DATASET,
          tableId:   BQ_FINAL
        },
        sourceUris: [ gcsUri ],    // GCS URI(s)
        sourceFormat: 'CSV',
        autodetect: true,
        fieldDelimiter: ',',
        skipLeadingRows: 1,
        writeDisposition: 'WRITE_APPEND'
      }
    }
  };

  const [job] = await bq.createJob(jobConfig);
  console.log(`[debug] started BQ load job: ${job.id}`);
  const [meta] = await job.getMetadata();

  if (meta.status?.errorResult) {
    throw new Error(`BQ load error: ${JSON.stringify(meta.status)}`);
  }
  const out = Number(meta.statistics?.load?.outputRows || 0);
  console.log(`[debug] loaded ${out} rows into ${BQ_DATASET}.${BQ_FINAL}`);
  return out;
}

/* ========== MAIN ========== */
async function main() {
  // Guardrails
  if (!AUTH_KEY || !BQ_PROJECT || !BQ_DATASET || !BQ_FINAL || !BUCKET_NAME) {
    throw new Error(`Missing env: ${
      [
        !AUTH_KEY    && 'S1_AUTH_KEY',
        !BQ_PROJECT  && 'BQ_PROJECT',
        !BQ_DATASET  && 'BQ_DATASET',
        !BQ_FINAL    && 'BQ_TABLE',
        !BUCKET_NAME && 'GCS_BUCKET/GCS_BUCKET_NAME'
      ].filter(Boolean).join(', ')
    }`);
  }

  const ymd = targetYMD();
  console.log(`Starting S1 pull for ${REPORT_TYPE} ymd=${ymd}`);

  // 1) Request report
  const reportId = await requestReport(AUTH_KEY);
  console.log(`report_id=${reportId}`);

  // 2) Poll status → content_url
  const contentUrl = await pollStatus(reportId, AUTH_KEY);
  console.log(`content_url=${contentUrl}`);

  // 3) Compose bucket/object path deterministically
  const objectPath = `${GCS_PREFIX}/${REPORT_TYPE}/${SITE_NAME}/${ymd}.csv.gz`;
  console.log(`[debug] bucketName=${BUCKET_NAME} objectPath=${objectPath}`);

  // 4) Download → GCS
  await downloadToGCS(contentUrl, BUCKET_NAME, objectPath);

  // 5) Load → BigQuery (from GCS)
  const gcsUri = `gs://${BUCKET_NAME}/${objectPath}`;
  await loadCsvGzToBQ(gcsUri);

  console.log('Done.');
}

main().catch(e => {
  console.error(e?.message || e);
  process.exitCode = 1;
});
