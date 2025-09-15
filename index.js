import fetch from 'node-fetch';
import { BigQuery } from '@google-cloud/bigquery';
import { Storage } from '@google-cloud/storage';

// ---- env ----
const AUTH_KEY = process.env.S1_AUTH_KEY;               // required
const REPORT_TYPE = process.env.S1_REPORT_TYPE || 'syndication_rsoc_online_ad_widget_daily';
const DAYS = process.env.S1_DAYS || '1';                // e.g. 1 day ending on date
const DATE = process.env.S1_DATE || '';                 // YYYY-MM-DD or blank (current)
const REPORTS_HOST = process.env.S1_HOST || 'https://reports.system1.com';
const GCS_BUCKET = process.env.GCS_BUCKET;              // gs://bucket-name
const GCS_PREFIX = process.env.GCS_PREFIX || 's1';
const BQ_PROJECT = process.env.BQ_PROJECT;
const BQ_DATASET = process.env.BQ_DATASET;
const BQ_TABLE   = process.env.BQ_TABLE;

// ---- clients ----
const bq = new BigQuery({ projectId: BQ_PROJECT });
const storage = new Storage();

function sleep(ms){ return new Promise(r=>setTimeout(r, ms)) }

async function requestReport() {
  const qs = new URLSearchParams({
    report_type: REPORT_TYPE,
    days: String(DAYS),
    auth_key: AUTH_KEY
  });
  if (DATE) qs.set('date', DATE);
  const url = `${REPORTS_HOST}/partner/v1/report?${qs}`;

  const res = await fetch(url, { method: 'POST' });
  if (!res.ok) throw new Error(`Report request failed ${res.status}`);
  const j = await res.json();
  if (!j.report_id) throw new Error(`No report_id: ${JSON.stringify(j)}`);
  return j.report_id;
}

async function pollStatus(reportId) {
  const url = `${REPORTS_HOST}/partner/v1/report/${encodeURIComponent(reportId)}/status?auth_key=${encodeURIComponent(AUTH_KEY)}`;
  let attempts = 0;
  while (attempts < 40) {         // ~20 min @ 30s interval
    const res = await fetch(url);
    if (res.status === 429) {
      const ra = Number(res.headers.get('Retry-After') || 30);
      await sleep((ra + 1) * 1000);
      continue;
    }
    if (!res.ok) throw new Error(`Status failed ${res.status}`);
    const j = await res.json();

    if (j.report_status === 'SUCCESS' && j.content_url) {
      // content_url may already include ?auth_key=... ; if not, append it
      const hasAuth = j.content_url.includes('auth_key=');
      const full = hasAuth ? j.content_url : `${j.content_url}${j.content_url.includes('?')?'&':'?'}auth_key=${encodeURIComponent(AUTH_KEY)}`;
      return full.startsWith('http') ? full : `${REPORTS_HOST}${full}`;
    }
    if (j.report_status === 'FAILED') throw new Error('Report FAILED (no content)');
    if (j.report_status === 'RUNNING') {
      await sleep(30 * 1000); // per S1 guidance
      attempts++;
      continue;
    }
    // NO CONTENT?
    throw new Error(`Unexpected status: ${JSON.stringify(j)}`);
  }
  throw new Error('Timed out waiting for SUCCESS');
}

async function downloadToGCS(contentUrl, gcsUri) {
  const res = await fetch(contentUrl, { redirect: 'follow' });
  if (!res.ok) {
    // sometimes content endpoint returns HTML redirect with link—follow handled by redirect:'follow'
    throw new Error(`Download failed ${res.status}`);
  }
  const [ , bucketName, ...pathParts ] = gcsUri.replace('gs://','').split('/');
  const dest = storage.bucket(bucketName).file(pathParts.join('/'));
  const stream = dest.createWriteStream({ resumable: false }); // we write gz as-is
  await new Promise((resolve, reject) => {
    res.body.pipe(stream)
      .on('finish', resolve)
      .on('error', reject);
  });
}

async function loadCsvFromGCS(gcsUri) {
  // Load job options: assume header row exists
  // Optionally you can define schema explicitly to avoid autodetect surprises
  const [ job ] = await bq.dataset(BQ_DATASET).table(BQ_TABLE).load(gcsUri, {
    sourceFormat: 'CSV',
    autodetect: true,          // or provide schema
    fieldDelimiter: ',',
    skipLeadingRows: 1,
    writeDisposition: 'WRITE_APPEND'
  });
  const [ metadata ] = await job.getMetadata();
  if (metadata.status?.errorResult) {
    throw new Error(`BQ load error: ${JSON.stringify(metadata.status)}`);
  }
  return metadata;
}

function yesterdayYMD() {
  const d = DATE ? new Date(`${DATE}T00:00:00Z`) : new Date();
  if (!DATE) d.setUTCDate(d.getUTCDate() - 1);
  return d.toISOString().slice(0,10);
}

async function main() {
  if (!AUTH_KEY || !BQ_PROJECT || !BQ_DATASET || !BQ_TABLE || !GCS_BUCKET) {
    throw new Error('Missing env: S1_AUTH_KEY, BQ_PROJECT, BQ_DATASET, BQ_TABLE, GCS_BUCKET');
  }
  const ymd = yesterdayYMD();
  console.log(`Starting S1 pull for ${REPORT_TYPE} ymd=${ymd}`);

  const reportId = await requestReport();
  console.log(`report_id=${reportId}`);

  const contentUrl = await pollStatus(reportId);
  console.log(`content_url=${contentUrl}`);

  const objectName = `${GCS_PREFIX}/${REPORT_TYPE}/${ymd}.csv.gz`;
  const gcsUri = `${GCS_BUCKET}/${objectName}`;
  await downloadToGCS(contentUrl, gcsUri);
  console.log(`Downloaded to ${gcsUri}`);

  const meta = await loadCsvFromGCS(gcsUri);
  console.log(`Loaded to BQ: ${meta.statistics?.load?.outputRows || 0} rows`);
}

main().catch(err => {
  console.error(err.message || err);
  process.exitCode = 1;
});
