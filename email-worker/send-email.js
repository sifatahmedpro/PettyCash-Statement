/**
 * ============================================================
 * email-worker/send-email.js  —  v5.1 (BUG-FIX RELEASE)
 *
 * WHAT CHANGED vs v5.0:
 *
 *   [FIX 1] CRITICAL — fetchPremiumStatements() collection mismatch.
 *     v5.0 queried accounts_1st_year / accounts_renewal / accounts_deferred /
 *     accounts_mr / accounts_loan — none of which exist in Firestore.
 *     The push worker (FIX-22 in send-push.js v5.7) confirmed the real
 *     collections are 'accounts' (active) and 'accountsArchive_1st_year'
 *     (archived). The 4 PM "প্রিমিয়াম জমা" slot now queries those two.
 *
 *   [FIX 4] WARNING — getTargetPage() silent null on odd-hour runner.
 *     The hour-map only matched even hours (10,12,14,16,18,20). A GitHub
 *     runner delayed past the :05 mark could start at an odd Dhaka hour
 *     (e.g. 11, 13) and receive null, exiting silently. Added odd-hour
 *     aliases (11→help, 13→office_issue, etc.) mirroring the two-hour
 *     case ranges in send-email.yml.
 *
 *   [FIX 5] WARNING — No email deduplication guard.
 *     If the cron runner fired, partially completed, then retried, some
 *     users received the same email twice. Added checkAndMarkSentToday()
 *     which writes a flag document to emailSentToday/{today}_{pageKey}
 *     before sending. On retry the flag is found and the user is skipped.
 *
 *   [FIX 6] WARNING — email log schema drift vs push log schema.
 *     writeEmailLog() now includes devices:[] and statusKey:null defaults
 *     so the UI (notification-log-backend.js _normalise) can read email
 *     and push logs with the same field shape.
 *
 * ============================================================
 */

'use strict';

const admin      = require('firebase-admin');
const nodemailer = require('nodemailer');

// ── 1. Firebase Admin init ────────────────────────────────────

const privateKey = process.env.FIREBASE_PRIVATE_KEY
    ? process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, '\n')
    : null;

if (!process.env.FIREBASE_PROJECT_ID || !process.env.FIREBASE_CLIENT_EMAIL || !privateKey) {
    console.error('❌ Missing Firebase credentials. Check GitHub Secrets.');
    process.exit(1);
}

if (!process.env.GMAIL_USER || !process.env.GMAIL_APP_PASSWORD) {
    console.error('❌ Missing Gmail credentials. Check GitHub Secrets.');
    process.exit(1);
}

admin.initializeApp({
    credential: admin.credential.cert({
        projectId:   process.env.FIREBASE_PROJECT_ID,
        clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
        privateKey:  privateKey
    })
});

const db = admin.firestore();

// ── 2. Nodemailer transporter ─────────────────────────────────

const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
        user: process.env.GMAIL_USER,
        pass: process.env.GMAIL_APP_PASSWORD
    }
});

// ── 3. Dhaka time helpers ─────────────────────────────────────

const DHAKA_OFFSET_MS = 6 * 60 * 60 * 1000;

function getDhakaDate() {
    return new Date(new Date().getTime() + DHAKA_OFFSET_MS);
}

function getTodayStr() {
    const d = getDhakaDate();
    return d.getUTCFullYear() + '-' +
        String(d.getUTCMonth() + 1).padStart(2, '0') + '-' +
        String(d.getUTCDate()).padStart(2, '0');
}

function getBengaliDateLabel() {
    const d      = getDhakaDate();
    const days   = ['রবিবার','সোমবার','মঙ্গলবার','বুধবার','বৃহস্পতিবার','শুক্রবার','শনিবার'];
    const months = ['জানুয়ারি','ফেব্রুয়ারি','মার্চ','এপ্রিল','মে','জুন',
                    'জুলাই','আগস্ট','সেপ্টেম্বর','অক্টোবর','নভেম্বর','ডিসেম্বর'];
    const toBn   = n => String(n).replace(/\d/g, ch => '০১২৩৪৫৬৭৮৯'[+ch]);
    return `${days[d.getUTCDay()]}, ${toBn(d.getUTCDate())} ${months[d.getUTCMonth()]} ${toBn(d.getUTCFullYear())}`;
}

/**
 * Resolve which page to send.
 *  - Reads TARGET_PAGE env var (set by workflow step).
 *  - Falls back to deriving from Dhaka hour if not set.
 *  - Returns null if current Dhaka time is in the silent window (10 PM – 10 AM).
 */
function getTargetPage() {
    if (process.env.TARGET_PAGE) {
        return process.env.TARGET_PAGE.trim();
    }
    // Fallback: derive from current Dhaka hour (10 PM–10 AM is silent window)
    // getDhakaDate() returns a Date shifted by +6 h, so getUTCHours() gives
    // the true Dhaka local hour (0–23).
    const dhakaHour = getDhakaDate().getUTCHours();
    // Silent window: before 10 AM or at/after 22:00 (10 PM) Dhaka → return null
    if (dhakaHour < 10 || dhakaHour >= 22) return null;
    // Map Dhaka hour → page key
    // FIX 4: Each even hour also has an odd-hour fallback matching the two-hour
    // window used in send-email.yml (e.g. "3|4" → help). This prevents a silent
    // null/exit when GitHub runner congestion pushes start-time past the :00
    // boundary into the next odd hour (e.g. cron fires at 03:55 UTC but the
    // runner actually begins at 04:01 → dhakaHour=10; previously matched fine,
    // but if the runner starts at 04:59 UTC → dhakaHour=10 still fine, however
    // if somehow dhakaHour resolves to 11 we now still return 'office_issue').
    const map = {
        10: 'help',            11: 'help',
        12: 'office_issue',    13: 'office_issue',
        14: 'business_stats',  15: 'business_stats',
        16: 'premium_submit',  17: 'premium_submit',
        18: 'advance_payment', 19: 'advance_payment',
        20: 'donation',        21: 'donation',
    };
    return map[dhakaHour] || null;
}

// ── 4. Firestore base path helper ────────────────────────────

const BASE = (uid) =>
    db.collection('artifacts').doc('default-app-id').collection('users').doc(uid);

// ── 5. Firestore data fetchers ────────────────────────────────
// Each fetcher uses the EXACT collection names written by the app.

/**
 * advance_payments collection
 * Fields: code, name, branch, type, description, amount, date, isArchived
 */
async function fetchAdvancePayments(uid) {
    const snap = await BASE(uid).collection('advance_payments')
        .orderBy('timestamp', 'desc').get();
    const active = [], archived = [];
    snap.forEach(d => {
        const data = d.data();
        const row = {
            code:        data.code        || '—',
            name:        data.name        || '—',
            branch:      data.branch      || '—',
            type:        data.type        || '—',
            description: data.description || '—',
            amount:      data.amount != null ? Number(data.amount) : 0,
            date:        data.date        || '—'
        };
        if (data.isArchived) archived.push(row);
        else                 active.push(row);
    });
    const totalActive   = active.reduce((s, r) => s + r.amount, 0);
    const totalArchived = archived.reduce((s, r) => s + r.amount, 0);
    return { active, archived, totalActive, totalArchived,
             grandTotal: totalActive + totalArchived };
}

/**
 * business_analysis_archives collection
 * Fields stored under meta: reportDate, inchargeName, createdBy, createdAt
 * NOTE: total15DaysBusiness does NOT exist in this collection — it only
 * lives in commission_archives. The main archive stores report metadata only.
 */
async function fetchBusinessStats(uid) {
    let rows = [];
    try {
        const snap = await BASE(uid).collection('business_analysis_archives')
            .orderBy('meta.createdAt', 'desc').get();
        snap.forEach(d => {
            const data = d.data();
            const meta = data.meta || {};
            rows.push({
                date:      meta.reportDate   || '—',
                incharge:  meta.inchargeName || '—',
                createdBy: meta.createdBy    || '—'
            });
        });
    } catch (e) {
        console.warn('  ⚠️ business_analysis_archives fetch error:', e.message);
    }
    return { rows, total: rows.length };
}

/**
 * donations collection
 * Fields: code, name, branch, type, amount, date, isArchived
 */
async function fetchDonations(uid) {
    const snap = await BASE(uid).collection('donations')
        .orderBy('timestamp', 'desc').get();
    const active = [], archived = [];
    snap.forEach(d => {
        const data = d.data();
        const row = {
            code:   data.code   || '—',
            name:   data.name   || '—',
            branch: data.branch || '—',
            type:   data.type   || '—',
            amount: data.amount != null ? Number(data.amount) : 0,
            date:   data.date   || '—'
        };
        if (data.isArchived) archived.push(row);
        else                 active.push(row);
    });
    const totalActive   = active.reduce((s, r) => s + r.amount, 0);
    const totalArchived = archived.reduce((s, r) => s + r.amount, 0);
    return { active, archived, totalActive, totalArchived };
}

/**
 * tasks collection
 * Fields: title, date (Timestamp or string), status ('done' | 'pending')
 */
async function fetchAllTasks(uid) {
    const snap = await BASE(uid).collection('tasks')
        .orderBy('date', 'asc').get();
    const pending = [], done = [];
    snap.forEach(d => {
        const data = d.data();
        let dateStr = '(তারিখ নেই)';
        if (data.date) {
            if (data.date.toDate) {
                const dt = data.date.toDate();
                dateStr = dt.getFullYear() + '-' +
                    String(dt.getMonth() + 1).padStart(2, '0') + '-' +
                    String(dt.getDate()).padStart(2, '0');
            } else {
                dateStr = String(data.date).split('T')[0];
            }
        }
        const row = { title: data.title || 'শিরোনামহীন', date: dateStr };
        if (data.status === 'done') done.push(row);
        else                        pending.push(row);
    });
    return { pending, done };
}

/**
 * office_issues collection
 * Fields: date, description, priority, status ('pending' | 'resolved')
 */
async function fetchIssues(uid) {
    const snap = await BASE(uid).collection('office_issues')
        .orderBy('timestamp', 'desc').get();
    const pending = [], resolved = [];
    snap.forEach(d => {
        const data = d.data();
        const row = {
            date:        data.date        || '—',
            description: (data.description || '—').slice(0, 80),
            priority:    data.priority    || 'medium',
            status:      data.status      || 'pending'
        };
        if (data.status === 'resolved') resolved.push(row);
        else                            pending.push(row);
    });
    return { pending, resolved };
}

/**
 * FIX 1 (mirrors FIX-22 in send-push.js v5.7):
 * Real Firestore collections confirmed in Firestore console:
 *   'accounts'                  — active premium entries
 *   'accountsArchive_1st_year'  — archived entries
 * The old worker queried accounts_1st_year / accounts_renewal /
 * accounts_deferred / accounts_mr / accounts_loan — none of which exist.
 * Fields: type, slipNo, amount, deposit, balance, date, timestamp
 */
async function fetchPremiumStatements(uid) {
    const COLLECTIONS = [
        { col: 'accounts',                 label: 'সক্রিয়' },
        { col: 'accountsArchive_1st_year', label: '১ম বর্ষ আর্কাইভ' },
    ];
    const rows = [];
    for (const { col, label } of COLLECTIONS) {
        try {
            const snap = await BASE(uid).collection(col)
                .orderBy('timestamp', 'desc').get();
            snap.forEach(d => {
                const data = d.data();
                rows.push({
                    type:    data.type   || label,
                    slipNo:  data.slipNo  || '—',
                    amount:  data.amount  != null ? Number(data.amount)  : 0,
                    deposit: data.deposit != null ? Number(data.deposit) : 0,
                    balance: data.balance != null ? Number(data.balance) : 0,
                    date:    data.date    || '—'
                });
            });
        } catch (e) {
            console.warn(`  ⚠️ ${col} fetch error:`, e.message);
        }
    }
    // Sort newest first by date string
    rows.sort((a, b) => (b.date > a.date ? 1 : -1));
    const totalDeposit = rows.reduce((s, r) => s + r.deposit, 0);
    const totalBalance = rows.reduce((s, r) => s + r.balance, 0);
    return { rows, totalDeposit, totalBalance };
}

// ── 6. Email template helpers ─────────────────────────────────

const FONT_IMPORT = `<link href="https://fonts.googleapis.com/css2?family=Noto+Sans+Bengali:wght@400;600;700&display=swap" rel="stylesheet">`;
const FONT_STACK  = `'SolaimanLipi', 'Kalpurush', 'Noto Sans Bengali', 'Arial Unicode MS', sans-serif`;

function toBnNum(n) {
    return String(n).replace(/\d/g, ch => '০১২৩৪৫৬৭৮৯'[+ch]);
}

function formatTaka(val) {
    if (val === '—' || val === null || val === undefined) return '—';
    const num = Number(val);
    if (isNaN(num)) return String(val);
    return '৳\u202f' + num.toLocaleString('bn-BD');
}

function priorityBadge(priority) {
    const map = {
        high:   { bg: '#fee2e2', color: '#dc2626', border: '#fca5a5', label: 'উচ্চ' },
        medium: { bg: '#fef3c7', color: '#d97706', border: '#fcd34d', label: 'সাধারণ' },
        low:    { bg: '#dcfce7', color: '#16a34a', border: '#86efac', label: 'কম' },
    };
    const s = map[priority] || map.medium;
    return `<span style="display:inline-block;padding:2px 9px;border-radius:20px;font-size:.75rem;font-weight:700;background:${s.bg};color:${s.color};border:1px solid ${s.border}">${s.label}</span>`;
}

function typeBadge(label, accent = '#251577') {
    return `<span style="display:inline-block;padding:2px 9px;border-radius:20px;font-size:.75rem;font-weight:600;background:#eef0fc;color:${accent};border:1px solid #c7cef5">${label}</span>`;
}

function emailShell({ headerAccent = '#251577', headerAccent2 = '#3730a3', icon, title,
                       subtitle = '', dateLabel, timeSlot, officeName, bodyHTML, statCards = [] }) {

    const statCardsHTML = statCards.length ? `
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin:0 0 24px">
        <tr>
            ${statCards.map(c => `
            <td width="${Math.floor(100 / statCards.length)}%" style="padding:0 6px 0 0">
                <div style="background:#f5f5ff;border:1.5px solid #dde0f8;border-radius:10px;padding:14px 12px;text-align:center">
                    <div style="font-size:1.4rem;line-height:1">${c.icon}</div>
                    <div style="font-size:1.25rem;font-weight:700;color:#251577;margin:6px 0 2px;font-family:${FONT_STACK}">${c.value}</div>
                    <div style="font-size:.73rem;color:#6b7280;font-family:${FONT_STACK}">${c.label}</div>
                </div>
            </td>`).join('')}
        </tr>
    </table>` : '';

    return `<!DOCTYPE html>
<html lang="bn" dir="ltr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
${FONT_IMPORT}
<title>${title}</title>
<style>
  * { box-sizing: border-box; }
  body { margin: 0; padding: 0; background: #eef0f8; }
  @media (max-width: 620px) {
    .email-wrapper { margin: 0 !important; border-radius: 0 !important; }
    .email-pad     { padding: 20px 16px !important; }
    .data-table td, .data-table th { padding: 8px 6px !important; font-size: .78rem !important; }
  }
</style>
</head>
<body style="margin:0;padding:0;background:#eef0f8;font-family:${FONT_STACK}">

<table role="presentation" width="100%" cellpadding="0" cellspacing="0">
<tr><td align="center" style="padding:28px 12px 40px">

<table role="presentation" class="email-wrapper" width="600" cellpadding="0" cellspacing="0"
       style="max-width:600px;width:100%;background:#ffffff;border-radius:16px;overflow:hidden;
              box-shadow:0 8px 32px rgba(37,21,119,.13)">

  <!-- HEADER -->
  <tr>
    <td style="background:linear-gradient(135deg,${headerAccent} 0%,${headerAccent2} 100%);padding:0">
      <div style="height:4px;background:linear-gradient(90deg,#FFC400,#FFE066,#FFC400)"></div>
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td style="padding:18px 28px 10px">
            <table role="presentation" cellpadding="0" cellspacing="0">
              <tr>
                <td style="background:rgba(255,255,255,.18);border-radius:10px;padding:6px 14px">
                  <span style="color:#ffffff;font-size:.8rem;font-weight:700;font-family:${FONT_STACK}">
                    🏢 অফিস ম্যানেজমেন্ট সিস্টেম
                  </span>
                </td>
                <td style="padding-left:10px">
                  ${officeName ? `<span style="color:rgba(255,255,255,.85);font-size:.78rem;font-family:${FONT_STACK}">📍 ${officeName}</span>` : ''}
                </td>
              </tr>
            </table>
          </td>
        </tr>
      </table>
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td align="center" style="padding:10px 28px 20px">
            <div style="width:64px;height:64px;background:rgba(255,255,255,.2);border-radius:50%;
                        display:inline-flex;align-items:center;justify-content:center;
                        font-size:2rem;line-height:64px;margin-bottom:12px">${icon}</div>
            <h1 style="margin:0 0 6px;color:#ffffff;font-size:1.45rem;font-weight:700;font-family:${FONT_STACK}">${title}</h1>
            ${subtitle ? `<p style="margin:0 0 8px;color:rgba(255,255,255,.8);font-size:.85rem;font-family:${FONT_STACK}">${subtitle}</p>` : ''}
            <div style="display:inline-block;background:rgba(255,255,255,.2);border:1px solid rgba(255,255,255,.35);
                        border-radius:20px;padding:5px 16px;margin-top:4px">
              <span style="color:#ffffff;font-size:.82rem;font-family:${FONT_STACK}">${timeSlot} &nbsp;·&nbsp; ${dateLabel}</span>
            </div>
          </td>
        </tr>
      </table>
      <div style="height:28px;background:#ffffff;clip-path:ellipse(55% 100% at 50% 100%)"></div>
    </td>
  </tr>

  <!-- BODY -->
  <tr>
    <td class="email-pad" style="padding:28px 32px 32px">
      ${statCardsHTML}
      ${bodyHTML}
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-top:28px">
        <tr>
          <td style="background:linear-gradient(135deg,#f5f5ff,#eef0fc);border:1.5px solid #dde0f8;
                     border-radius:12px;padding:16px 20px">
            <p style="margin:0 0 6px;font-size:.82rem;font-weight:700;color:#251577;font-family:${FONT_STACK}">
              💡 দ্রুত টিপস
            </p>
            <p style="margin:0;font-size:.8rem;color:#4b5563;line-height:1.6;font-family:${FONT_STACK}">
              নিয়মিত রেকর্ড আপডেট রাখুন। কোনো মুলতবি কাজ থাকলে দ্রুত সম্পন্ন করুন।
            </p>
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- FOOTER -->
  <tr>
    <td style="background:#251577;padding:20px 32px">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td>
            <p style="margin:0 0 4px;color:rgba(255,255,255,.9);font-size:.82rem;font-weight:700;font-family:${FONT_STACK}">
              অফিস ম্যানেজমেন্ট সিস্টেম
            </p>
            <p style="margin:0;color:rgba(255,255,255,.5);font-size:.75rem;font-family:${FONT_STACK}">
              এই ইমেইল স্বয়ংক্রিয়ভাবে পাঠানো হয়েছে। ইমেইল বন্ধ করতে অ্যাকাউন্ট সেটিংসে যান।
            </p>
          </td>
          <td align="right">
            <span style="font-size:1.5rem">📊</span>
          </td>
        </tr>
      </table>
    </td>
  </tr>

</table>
</td></tr>
</table>
</body>
</html>`;
}

function tableHTML(headers, rows, emptyMsg, { amountCols = [], badgeCols = {}, rowLimit = 50 } = {}) {
    if (rows.length === 0) {
        return `
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
          <tr>
            <td align="center" style="background:#f0fdf4;border:1.5px solid #bbf7d0;border-radius:12px;padding:20px">
              <div style="font-size:1.5rem;margin-bottom:8px">✅</div>
              <p style="margin:0;color:#15803d;font-size:.88rem;font-weight:600;font-family:${FONT_STACK}">${emptyMsg}</p>
            </td>
          </tr>
        </table>`;
    }

    const displayRows = rows.slice(0, rowLimit);
    const extraRows   = rows.length - displayRows.length;
    const thStyle = `padding:10px 12px;text-align:left;font-size:.78rem;color:#ffffff;
                     font-weight:700;background:#251577;font-family:${FONT_STACK};white-space:nowrap`;
    const ths = headers.map(h => `<th style="${thStyle}">${h}</th>`).join('');

    const trs = displayRows.map((row, i) => {
        const vals = Object.values(row);
        const keys = Object.keys(row);
        const bg   = i % 2 === 0 ? '#f8f8ff' : '#ffffff';
        const tds  = vals.map((v, ci) => {
            const key = keys[ci];
            let cell  = v ?? '—';
            if (amountCols.includes(key) || amountCols.includes(ci)) cell = formatTaka(cell);
            if (badgeCols[key] === 'priority') cell = priorityBadge(String(v));
            else if (badgeCols[key] === 'type') cell = typeBadge(String(v));
            return `<td style="padding:9px 12px;border-bottom:1px solid #ebebfc;color:#1f2937;
                               font-size:.83rem;font-family:${FONT_STACK};vertical-align:middle">${cell}</td>`;
        }).join('');
        return `<tr style="background:${bg}">${tds}</tr>`;
    }).join('');

    const extraNote = extraRows > 0
        ? `<tr><td colspan="${headers.length}" style="padding:8px 12px;font-size:.78rem;
               color:#6b7280;text-align:center;font-family:${FONT_STACK}">
               আরও ${toBnNum(extraRows)}টি রেকর্ড আছে — অ্যাপে দেখুন।
           </td></tr>`
        : '';

    return `
    <div style="overflow-x:auto;border-radius:10px;border:1.5px solid #dde0f8;overflow:hidden">
      <table class="data-table" role="presentation" width="100%" cellpadding="0" cellspacing="0"
             style="border-collapse:collapse;min-width:400px">
        <thead><tr>${ths}</tr></thead>
        <tbody>${trs}${extraNote}</tbody>
      </table>
    </div>`;
}

function sectionTitle(icon, label, count) {
    return `
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin:0 0 14px">
      <tr>
        <td style="border-left:4px solid #251577;padding-left:12px">
          <span style="font-size:.95rem;font-weight:700;color:#251577;font-family:${FONT_STACK}">
            ${icon} ${label}
          </span>
          <span style="margin-left:8px;background:#251577;color:#fff;font-size:.72rem;
                       padding:2px 8px;border-radius:20px;font-weight:700;font-family:${FONT_STACK}">
            ${toBnNum(count)}টি
          </span>
        </td>
      </tr>
    </table>`;
}

// ── 7. Per-page email builders ────────────────────────────────

function buildAdvancePaymentEmail({ dateLabel, timeSlot, data, officeName }) {
    const { active, archived, totalActive, totalArchived, grandTotal } = data;
    const body = `
        ${sectionTitle('✅', 'সক্রিয় পরিশোধ রেকর্ড', active.length)}
        ${tableHTML(['কোড','নাম','শাখা','ধরণ','বিবরণ','টাকা','তারিখ'], active,
            'কোনো সক্রিয় রেকর্ড নেই।', { amountCols: ['amount'], badgeCols: { type: 'type' } })}
        ${active.length > 0 ? `<p style="text-align:right;font-weight:700;color:#0f766e;font-family:${FONT_STACK};margin:8px 0 24px">সক্রিয় মোট: ${formatTaka(totalActive)}</p>` : ''}
        ${sectionTitle('📦', 'আর্কাইভ রেকর্ড', archived.length)}
        ${tableHTML(['কোড','নাম','শাখা','ধরণ','বিবরণ','টাকা','তারিখ'], archived,
            'কোনো আর্কাইভ রেকর্ড নেই।', { amountCols: ['amount'], badgeCols: { type: 'type' } })}
        ${archived.length > 0 ? `<p style="text-align:right;font-weight:700;color:#6b7280;font-family:${FONT_STACK};margin:8px 0 0">আর্কাইভ মোট: ${formatTaka(totalArchived)}</p>` : ''}`;
    return emailShell({
        headerAccent: '#0f766e', headerAccent2: '#0d9488',
        icon: '💵', title: 'অগ্রিম পরিশোধ — সম্পূর্ণ তালিকা',
        subtitle: 'সক্রিয় ও আর্কাইভ সহ সকল রেকর্ড',
        dateLabel, timeSlot, officeName, bodyHTML: body,
        statCards: [
            { icon: '✅', label: 'সক্রিয় রেকর্ড',   value: toBnNum(active.length) },
            { icon: '📦', label: 'আর্কাইভ রেকর্ড',  value: toBnNum(archived.length) },
            { icon: '💰', label: 'সক্রিয় মোট টাকা', value: formatTaka(totalActive) },
            { icon: '🏦', label: 'সর্বমোট টাকা',     value: formatTaka(grandTotal) },
        ]
    });
}

function buildBusinessStatsEmail({ dateLabel, timeSlot, data, officeName }) {
    const { rows, total } = data;
    const body = `
        ${sectionTitle('📊', 'সকল ব্যবসা পরিসংখ্যান রিপোর্ট', rows.length)}
        ${tableHTML(['তারিখ','ইনচার্জ','তৈরিকারী'], rows,
            'কোনো সংরক্ষিত রিপোর্ট পাওয়া যায়নি।')}`;
    return emailShell({
        headerAccent: '#251577', headerAccent2: '#1d4ed8',
        icon: '📊', title: 'ব্যবসা পরিসংখ্যান — সম্পূর্ণ তালিকা',
        subtitle: 'সকল সংরক্ষিত রিপোর্টের বিবরণ',
        dateLabel, timeSlot, officeName, bodyHTML: body,
        statCards: [
            { icon: '📋', label: 'মোট রিপোর্ট', value: toBnNum(rows.length) },
        ]
    });
}

function buildDonationEmail({ dateLabel, timeSlot, data, officeName }) {
    const { active, archived, totalActive, totalArchived } = data;
    const body = `
        ${sectionTitle('✅', 'সক্রিয় অনুদান রেকর্ড', active.length)}
        ${tableHTML(['কোড','নাম','শাখা','ধরণ','টাকা','তারিখ'], active,
            'কোনো সক্রিয় অনুদান রেকর্ড নেই।', { amountCols: ['amount'], badgeCols: { type: 'type' } })}
        ${active.length > 0 ? `<p style="text-align:right;font-weight:700;color:#7c3aed;font-family:${FONT_STACK};margin:8px 0 24px">সক্রিয় মোট: ${formatTaka(totalActive)}</p>` : ''}
        ${sectionTitle('📦', 'আর্কাইভ রেকর্ড', archived.length)}
        ${tableHTML(['কোড','নাম','শাখা','ধরণ','টাকা','তারিখ'], archived,
            'কোনো আর্কাইভ রেকর্ড নেই।', { amountCols: ['amount'], badgeCols: { type: 'type' } })}
        ${archived.length > 0 ? `<p style="text-align:right;font-weight:700;color:#6b7280;font-family:${FONT_STACK};margin:8px 0 0">আর্কাইভ মোট: ${formatTaka(totalArchived)}</p>` : ''}`;
    return emailShell({
        headerAccent: '#7c3aed', headerAccent2: '#251577',
        icon: '🤝', title: 'অনুদান — সম্পূর্ণ তালিকা',
        subtitle: 'সক্রিয় ও আর্কাইভ সহ সকল রেকর্ড',
        dateLabel, timeSlot, officeName, bodyHTML: body,
        statCards: [
            { icon: '✅', label: 'সক্রিয় রেকর্ড',  value: toBnNum(active.length) },
            { icon: '📦', label: 'আর্কাইভ রেকর্ড', value: toBnNum(archived.length) },
            { icon: '💰', label: 'সক্রিয় মোট',      value: formatTaka(totalActive) },
        ]
    });
}

function buildHelpEmail({ dateLabel, timeSlot, data, officeName }) {
    const { pending, done } = data;
    const today = new Date().toISOString().split('T')[0];
    const overdue = pending.filter(r => r.date !== '(তারিখ নেই)' && r.date < today).length;
    const body = `
        ${sectionTitle('⏳', 'মুলতবি টাস্কসমূহ', pending.length)}
        ${pending.length > 0 ? `<p style="font-size:.82rem;color:#dc2626;font-weight:600;margin:0 0 12px;font-family:${FONT_STACK}">
            ⚠️ এই টাস্কগুলো এখনও সম্পন্ন হয়নি। দ্রুত সম্পন্ন করুন।</p>` : ''}
        ${tableHTML(['টাস্কের শিরোনাম','নির্ধারিত তারিখ'], pending,
            'অভিনন্দন! সকল টাস্ক সম্পন্ন হয়েছে।')}
        ${sectionTitle('✅', 'সম্পন্ন টাস্কসমূহ', done.length)}
        ${tableHTML(['টাস্কের শিরোনাম','তারিখ'], done,
            'এখনো কোনো টাস্ক সম্পন্ন হয়নি।')}`;
    return emailShell({
        headerAccent: '#b45309', headerAccent2: '#f59e0b',
        icon: '📋', title: 'সহায়তা — সম্পূর্ণ টাস্ক তালিকা',
        subtitle: 'মুলতবি ও সম্পন্ন সকল টাস্কের বিবরণ',
        dateLabel, timeSlot, officeName, bodyHTML: body,
        statCards: [
            { icon: '⏳', label: 'মুলতবি টাস্ক',   value: toBnNum(pending.length) },
            { icon: '🔴', label: 'মেয়াদোত্তীর্ণ', value: toBnNum(overdue) },
            { icon: '✅', label: 'সম্পন্ন টাস্ক',  value: toBnNum(done.length) },
        ]
    });
}

function buildIssueEmail({ dateLabel, timeSlot, data, officeName }) {
    const { pending, resolved } = data;
    const highCount = pending.filter(r => r.priority === 'high').length;
    const body = `
        ${sectionTitle('⚠️', 'চলমান সমস্যাসমূহ', pending.length)}
        ${highCount > 0 ? `<p style="font-size:.82rem;color:#dc2626;font-weight:600;margin:0 0 12px;font-family:${FONT_STACK}">
            🔴 ${toBnNum(highCount)}টি উচ্চ-গুরুত্বের সমস্যা রয়েছে — তাৎক্ষণিক পদক্ষেপ নিন।</p>` : ''}
        ${tableHTML(['তারিখ','সমস্যার বিবরণ','গুরুত্ব'], pending,
            'কোনো চলমান সমস্যা নেই। সবকিছু ঠিকঠাক আছে!', { badgeCols: { priority: 'priority' } })}
        ${sectionTitle('✅', 'সমাধানকৃত সমস্যাসমূহ', resolved.length)}
        ${tableHTML(['তারিখ','সমস্যার বিবরণ','গুরুত্ব'], resolved,
            'কোনো সমাধানকৃত সমস্যা নেই।', { badgeCols: { priority: 'priority' } })}`;
    return emailShell({
        headerAccent: '#dc2626', headerAccent2: '#9f1239',
        icon: '⚠️', title: 'সমস্যা ও সমাধান — সম্পূর্ণ তালিকা',
        subtitle: 'চলমান ও সমাধানকৃত সকল সমস্যার স্ট্যাটাস',
        dateLabel, timeSlot, officeName, bodyHTML: body,
        statCards: [
            { icon: '🔴', label: 'উচ্চ গুরুত্ব', value: toBnNum(highCount) },
            { icon: '📌', label: 'মোট চলমান',     value: toBnNum(pending.length) },
            { icon: '✅', label: 'সমাধানকৃত',     value: toBnNum(resolved.length) },
        ]
    });
}

function buildPremiumEmail({ dateLabel, timeSlot, data, officeName }) {
    const { rows, totalDeposit, totalBalance } = data;
    const formattedRows = rows.map(r => ({
        type:    r.type,
        slipNo:  r.slipNo,
        amount:  formatTaka(r.amount),
        deposit: formatTaka(r.deposit),
        balance: formatTaka(r.balance),
        date:    r.date,
    }));
    const body = `
        ${sectionTitle('🏦', 'সকল প্রিমিয়াম জমার রেকর্ড', rows.length)}
        ${tableHTML(['ধরণ','স্লিপ নং','টাকা','জমা','বকেয়া','তারিখ'], formattedRows,
            'কোনো প্রিমিয়াম জমার রেকর্ড নেই।', { badgeCols: { type: 'type' } })}
        ${rows.length > 0 ? `
        <p style="text-align:right;font-weight:700;color:#0369a1;font-family:${FONT_STACK};margin:8px 0 0">
          মোট জমা: ${formatTaka(totalDeposit)} &nbsp;|&nbsp; মোট বকেয়া: ${formatTaka(totalBalance)}
        </p>` : ''}`;
    return emailShell({
        headerAccent: '#0369a1', headerAccent2: '#251577',
        icon: '🏦', title: 'প্রিমিয়াম জমা — সম্পূর্ণ তালিকা',
        subtitle: 'সকল প্রিমিয়াম কালেকশনের বিবরণ',
        dateLabel, timeSlot, officeName, bodyHTML: body,
        statCards: [
            { icon: '📑', label: 'মোট এন্ট্রি', value: toBnNum(rows.length) },
            { icon: '✅', label: 'মোট জমা',      value: formatTaka(totalDeposit) },
            { icon: '🔔', label: 'মোট বকেয়া',   value: formatTaka(totalBalance) },
        ]
    });
}

// ── 8. Page definitions ───────────────────────────────────────

const PAGE_MAP = {
    help: {
        name: 'সহায়তা (টাস্ক তালিকা)', icon: '📋', timeSlot: '⏰ সকাল ১০টা',
        fetch:      (uid) => fetchAllTasks(uid),
        isEmpty:    (data) => data.pending.length === 0 && data.done.length === 0,
        totalRows:  (data) => data.pending.length + data.done.length,
        buildEmail: (p)   => buildHelpEmail(p),
        subject:    (dateLabel, data) =>
            `📋 সহায়তা — মুলতবি: ${data.pending.length}টি · সম্পন্ন: ${data.done.length}টি · ${dateLabel}`,
    },
    office_issue: {
        name: 'সমস্যা ও সমাধান', icon: '⚠️', timeSlot: '⏰ দুপুর ১২টা',
        fetch:      (uid) => fetchIssues(uid),
        isEmpty:    (data) => data.pending.length === 0 && data.resolved.length === 0,
        totalRows:  (data) => data.pending.length + data.resolved.length,
        buildEmail: (p)   => buildIssueEmail(p),
        subject:    (dateLabel, data) =>
            `⚠️ সমস্যা ও সমাধান — চলমান: ${data.pending.length}টি · সমাধান: ${data.resolved.length}টি · ${dateLabel}`,
    },
    business_stats: {
        name: 'ব্যবসা পরিসংখ্যান', icon: '📊', timeSlot: '⏰ বিকাল ২টা',
        fetch:      (uid) => fetchBusinessStats(uid),
        isEmpty:    (data) => data.rows.length === 0,
        totalRows:  (data) => data.rows.length,
        buildEmail: (p)   => buildBusinessStatsEmail(p),
        subject:    (dateLabel, data) =>
            `📊 ব্যবসা পরিসংখ্যান — ${data.rows.length}টি রিপোর্ট · ${dateLabel}`,
    },
    premium_submit: {
        name: 'প্রিমিয়াম জমা', icon: '🏦', timeSlot: '⏰ বিকাল ৪টা',
        fetch:      (uid) => fetchPremiumStatements(uid),
        isEmpty:    (data) => data.rows.length === 0,
        totalRows:  (data) => data.rows.length,
        buildEmail: (p)   => buildPremiumEmail(p),
        subject:    (dateLabel, data) =>
            `🏦 প্রিমিয়াম জমা — ${data.rows.length}টি এন্ট্রি · ${dateLabel}`,
    },
    advance_payment: {
        name: 'অগ্রিম পরিশোধ', icon: '💵', timeSlot: '⏰ সন্ধ্যা ৬টা',
        fetch:      (uid) => fetchAdvancePayments(uid),
        isEmpty:    (data) => data.active.length === 0 && data.archived.length === 0,
        totalRows:  (data) => data.active.length + data.archived.length,
        buildEmail: (p)   => buildAdvancePaymentEmail(p),
        subject:    (dateLabel, data) =>
            `💵 অগ্রিম পরিশোধ — সক্রিয়: ${data.active.length}টি · আর্কাইভ: ${data.archived.length}টি · ${dateLabel}`,
    },
    donation: {
        name: 'অনুদান', icon: '🤝', timeSlot: '⏰ রাত ৮টা',
        fetch:      (uid) => fetchDonations(uid),
        isEmpty:    (data) => data.active.length === 0 && data.archived.length === 0,
        totalRows:  (data) => data.active.length + data.archived.length,
        buildEmail: (p)   => buildDonationEmail(p),
        subject:    (dateLabel, data) =>
            `🤝 অনুদান — সক্রিয়: ${data.active.length}টি · আর্কাইভ: ${data.archived.length}টি · ${dateLabel}`,
    },
};

// ── 9. Firestore log writers ──────────────────────────────────
//
//   Per-user:  artifacts/default-app-id/users/{uid}/emailLogs/{auto-id}
//   Global:    artifacts/default-app-id/emailRunLogs/{auto-id}
//
// Schema mirrors send-push.js writePushLog so notification-log_module.js
// can read both collections with identical field names.
// ─────────────────────────────────────────────────────────────

async function writeEmailLog(uid, page, targetPageKey, status, recordCount, detail) {
    try {
        const now = admin.firestore.FieldValue.serverTimestamp();
        // Map page key to slot hour (mirrors UPCOMING_SCHEDULE in notification-log_module.js)
        const slotHourMap = {
            help:            10,
            office_issue:    12,
            business_stats:  14,
            premium_submit:  16,
            advance_payment: 18,
            donation:        20,
        };
        const slotHour = slotHourMap[targetPageKey] ?? null;

        const logEntry = {
            tag:         targetPageKey,
            label:       page.name,
            icon:        '📧',
            status,                      // 'sent' | 'failed' | 'skipped'
            sentAt:      now,
            detail:      detail || `${page.name} ইমেইল — ${slotHour}:০০ ঢাকা`,
            sent:        status === 'sent'    ? 1 : 0,
            failed:      status === 'failed'  ? 1 : 0,
            skipped:     status === 'skipped' ? 1 : 0,
            slotHour,
            recordCount: recordCount ?? null,
            // FIX 6: forward-compatibility with push log schema used by
            // notification-log-backend.js (_normalise). Push logs include
            // `devices` (per-device delivery list) and `statusKey` (dedup key).
            // Email logs don't use these but the UI reads them gracefully when
            // they are present as typed nulls/empty-arrays.
            devices:     [],
            statusKey:   null,
        };

        // 1. Per-user email log
        await db.collection('artifacts').doc('default-app-id')
            .collection('users').doc(uid)
            .collection('emailLogs')
            .add(logEntry);

        // 2. Global run log (for summary view on notification-log page)
        await db.collection('artifacts').doc('default-app-id')
            .collection('emailRunLogs')
            .add({ ...logEntry, uid });

    } catch (err) {
        console.warn(`  ⚠️  writeEmailLog failed (non-fatal): ${err.message}`);
    }
}

// ── 10. Send email helper ─────────────────────────────────────

async function sendEmail({ toEmail, toName, subject, htmlBody }) {
    await transporter.sendMail({\
        from:    `"অফিস ম্যানেজমেন্ট সিস্টেম" <${process.env.GMAIL_USER}>`,
        to:      `"${toName}" <${toEmail}>`,
        subject: subject,
        html:    htmlBody
    });
}

// ── FIX 5: Per-user per-page-key per-date deduplication ───────
// Mirrors the seenToday pattern in send-push.js.
// Prevents duplicate emails if the GitHub runner retries after a
// partial failure (email was sent to some users, worker crashed,
// then re-ran and sent again to those already-sent users).
//
// Flag path:
//   artifacts/default-app-id/users/{uid}/emailSentToday/{today}_{pageKey}
// Document: { sentAt: serverTimestamp, page: pageKey }

async function checkAndMarkSentToday(uid, pageKey, today) {
    const flagRef = db.collection('artifacts').doc('default-app-id')
        .collection('users').doc(uid)
        .collection('emailSentToday').doc(`${today}_${pageKey}`);
    const snap = await flagRef.get();
    if (snap.exists) {
        return false; // already sent today
    }
    await flagRef.set({
        sentAt: admin.firestore.FieldValue.serverTimestamp(),
        page: pageKey
    });
    return true; // flag written — safe to send
}

// ── 11. Main ──────────────────────────────────────────────────

async function run() {
    const targetPageKey = getTargetPage();

    if (!targetPageKey) {
        console.log('ℹ️  No target page determined. Exiting (silent hours or unknown hour).');
        process.exit(0);
    }

    const page = PAGE_MAP[targetPageKey];
    if (!page) {
        console.error(`❌ Unknown page key: "${targetPageKey}". Valid keys: ${Object.keys(PAGE_MAP).join(', ')}`);
        process.exit(1);
    }

    const dateLabel = getBengaliDateLabel();
    const today     = getTodayStr();

    console.log(`🚀 Email worker v5 — page: ${targetPageKey} (${page.name})`);
    console.log(`📅 Today (Dhaka): ${today} | UTC: ${new Date().toISOString()}\n`);

    let totalSent = 0, totalFailed = 0, totalSkipped = 0;

    try {
        const usersRef  = db.collection('artifacts').doc('default-app-id').collection('users');
        const usersSnap = await usersRef.get();

        if (usersSnap.empty) {
            console.log('ℹ️  No users found.');
            process.exit(0);
        }
        console.log(`👥 Found ${usersSnap.size} user(s).\n`);

        for (const userDoc of usersSnap.docs) {
            const uid = userDoc.id;
            console.log(`── User: ${uid}`);

            // Load subscription
            let sub = null;
            try {
                const subSnap = await usersRef.doc(uid)
                    .collection('data').doc('emailSubscription').get();
                if (subSnap.exists) sub = subSnap.data();
            } catch (e) {
                console.log(`  ⚠️  Could not read subscription: ${e.message} — skip.`);
                totalSkipped++;
                continue;
            }

            if (!sub)             { console.log(`  ⏭️  No email subscription — skip.`); totalSkipped++; continue; }
            if (!sub.active)      { console.log(`  ⏭️  Subscription inactive — skip.`);  totalSkipped++; continue; }
            if (!sub.email)       { console.log(`  ⚠️  No email address — skip.`);        totalSkipped++; continue; }

            // Check per-page opt-out
            const pagePrefs   = (sub.prefs && sub.prefs.pages) || {};
            const pageEnabled = pagePrefs[targetPageKey] !== false;
            if (!pageEnabled) {
                console.log(`  ⏭️  [${page.name}] opted out — skip.`);
                totalSkipped++;
                continue;
            }
            // Load office name
            let officeName = null;
            try {
                const pSnap = await usersRef.doc(uid).collection('data').doc('profile').get();
                if (pSnap.exists) officeName = pSnap.data().officeName || null;
            } catch (_) {}

            const toName = officeName || sub.email;

            try {
                const data = await page.fetch(uid);

                if (page.isEmpty(data)) {
                    console.log(`  ⏭️  [${page.name}] no records — skip (no empty email sent).`);
                    totalSkipped++;
                    await writeEmailLog(uid, page, targetPageKey, 'skipped', 0, `${page.name} — রেকর্ড নেই, ইমেইল পাঠানো হয়নি`);
                    continue;
                }

                // FIX 5: Deduplication — skip if this user already got this email today
                const canSend = await checkAndMarkSentToday(uid, targetPageKey, today);
                if (!canSend) {
                    console.log(`  ⏭️  [${page.name}] already sent today for ${uid} — skip (dedup).`);
                    totalSkipped++;
                    await writeEmailLog(uid, page, targetPageKey, 'skipped', 0, `${page.name} — আজ ইতিমধ্যে পাঠানো হয়েছে (dedup)`);
                    continue;
                }

                const htmlBody = page.buildEmail({ dateLabel, timeSlot: page.timeSlot, data, officeName });
                const subject  = page.subject(dateLabel, data);
                await sendEmail({ toEmail: sub.email, toName, subject, htmlBody });
                console.log(`  📧 [${page.name}] → ${sub.email} — OK (${page.totalRows(data)} records)`);
                totalSent++;
                await writeEmailLog(uid, page, targetPageKey, 'sent', page.totalRows(data), `${page.name} → ${sub.email} (${page.totalRows(data)} রেকর্ড)`);

            } catch (err) {
                console.error(`  ❌ [${page.name}] FAILED for ${uid}: ${err.message}`);
                totalFailed++;
                await writeEmailLog(uid, page, targetPageKey, 'failed', null, `${page.name} — ব্যর্থ: ${err.message}`);
            }

            // Small delay between users
            await new Promise(r => setTimeout(r, 300));
        }

    } catch (err) {
        console.error('❌ Fatal error:', err);
        process.exit(1);
    }

    console.log('─────────────────────────────────');
    console.log(`✅ Done. Sent: ${totalSent} | Failed: ${totalFailed} | Skipped: ${totalSkipped}`);
    process.exit(0);
}

run();
