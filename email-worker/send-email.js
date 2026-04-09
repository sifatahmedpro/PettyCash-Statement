/**
 * ============================================================
 * email-worker/send-email.js  —  v4.0 (FULL LIST STATUS EMAILS)
 *
 * WHAT CHANGED vs v2:
 *   Complete visual redesign of all 6 email templates:
 *   - Brand colour #251577 used throughout
 *   - SolaimanLipi / Kalpurush Bengali fonts declared
 *   - Rich header with logo bar, title, office & date strip
 *   - Summary stat cards (total records / amount at a glance)
 *   - Zebra-stripe data tables with column headers in brand blue
 *   - Status badges (priority, payment type, issue status)
 *   - Motivational footer with quick-tips section
 *   - Responsive layout (600 px max on desktop, 100% on mobile)
 *
 * All data fetching, Firestore paths, GitHub Actions wiring,
 * page definitions and send logic are UNCHANGED from v2.
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

function getDigestSlot() {
    return getDhakaDate().getUTCHours() < 14 ? 'morning' : 'evening';
}

// ── 4. Firestore data fetchers (v4 — full list, active + archived) ──

const BASE = (uid) =>
    db.collection('artifacts').doc('default-app-id').collection('users').doc(uid);

// Returns { active: [], archived: [], totalActive, totalArchived, grandTotal }
async function fetchAdvancePayments(uid) {
    const snap = await BASE(uid).collection('payments')
        .orderBy('date', 'desc').get();
    const active = [], archived = [];
    snap.forEach(d => {
        const data = d.data();
        const row = {
            code:        data.code        || '—',
            name:        data.name        || '—',
            branch:      data.branch      || '—',
            type:        data.type        || '—',
            description: data.description || '—',
            amount:      data.amount != null ? data.amount : 0,
            date:        data.date        || '—'
        };
        if (data.isArchived) archived.push(row);
        else                 active.push(row);
    });
    const totalActive   = active.reduce((s, r) => s + (Number(r.amount) || 0), 0);
    const totalArchived = archived.reduce((s, r) => s + (Number(r.amount) || 0), 0);
    return { active, archived, totalActive, totalArchived,
             grandTotal: totalActive + totalArchived };
}

// Returns { rows: [], total }
async function fetchBusinessStats(uid) {
    const rows = [];
    const snap = await BASE(uid).collection('businessStats').get();
    snap.forEach(d => {
        if (d.id === 'archive') return;
        const data = d.data();
        rows.push({ date: data.reportDate || data.date || '—',
                    incharge: data.inchargeName || '—',
                    business: data.businessYear || '—',
                    amount: data.businessAmount != null ? data.businessAmount : 0 });
    });
    try {
        const archSnap = await BASE(uid).collection('businessStats')
            .doc('archive').collection('reports').orderBy('reportDate', 'desc').get();
        archSnap.forEach(d => {
            const data = d.data();
            rows.push({ date: data.reportDate || '—',
                        incharge: data.inchargeName || '—',
                        business: data.businessYear || '—',
                        amount: data.businessAmount != null ? data.businessAmount : 0 });
        });
    } catch (_) {}
    rows.sort((a, b) => (b.date > a.date ? 1 : -1));
    const total = rows.reduce((s, r) => s + (Number(r.amount) || 0), 0);
    return { rows, total };
}

// Returns { active: [], archived: [], totalActive, totalArchived }
async function fetchDonations(uid) {
    const snap = await BASE(uid).collection('donations')
        .orderBy('date', 'desc').get();
    const active = [], archived = [];
    snap.forEach(d => {
        const data = d.data();
        const row = {
            code:   data.code   || '—',
            name:   data.name   || '—',
            branch: data.branch || '—',
            type:   data.type   || '—',
            amount: data.amount != null ? data.amount : 0,
            date:   data.date   || '—'
        };
        if (data.isArchived) archived.push(row);
        else                 active.push(row);
    });
    const totalActive   = active.reduce((s, r) => s + (Number(r.amount) || 0), 0);
    const totalArchived = archived.reduce((s, r) => s + (Number(r.amount) || 0), 0);
    return { active, archived, totalActive, totalArchived };
}

// Returns { pending: [], done: [] } — all tasks
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
        const row = { title: data.title || 'শিরোনামহীন', date: dateStr,
                      status: data.status || 'pending' };
        if (data.status === 'done') done.push(row);
        else                        pending.push(row);
    });
    return { pending, done };
}

// Returns { pending: [], resolved: [] }
async function fetchIssues(uid) {
    const snap = await BASE(uid).collection('issues')
        .orderBy('date', 'desc').get();
    const pending = [], resolved = [];
    snap.forEach(d => {
        const data = d.data();
        const row = {
            date:        data.date        || '—',
            description: (data.description || '—').slice(0, 80),
            priority:    data.priority    || '—',
            status:      data.status      || 'pending'
        };
        if (data.status === 'pending') pending.push(row);
        else                           resolved.push(row);
    });
    return { pending, resolved };
}

// Returns { rows: [], totalDeposit, totalBalance }
async function fetchPremiumStatements(uid) {
    const types = ['1st_year', 'renewal', 'deferred', 'mr', 'loan'];
    const typeLabels = { '1st_year': '১ম বর্ষ', renewal: 'নবায়ন',
                         deferred: 'ডেফার্ড', mr: 'এম আর', loan: 'ঋণ' };
    const rows = [];
    for (const type of types) {
        try {
            const snap = await BASE(uid).collection('premiumStatements')
                .where('type', '==', type).orderBy('date', 'desc').get();
            snap.forEach(d => {
                const data = d.data();
                rows.push({
                    type:    typeLabels[type] || type,
                    slipNo:  data.slipNo  || '—',
                    amount:  data.amount  != null ? data.amount  : 0,
                    deposit: data.deposit != null ? data.deposit : 0,
                    balance: data.balance != null ? data.balance : 0,
                    date:    data.date    || '—'
                });
            });
        } catch (_) {}
    }
    rows.sort((a, b) => (b.date > a.date ? 1 : -1));
    const totalDeposit = rows.reduce((s, r) => s + (Number(r.deposit) || 0), 0);
    const totalBalance = rows.reduce((s, r) => s + (Number(r.balance) || 0), 0);
    return { rows, totalDeposit, totalBalance };
}

// ── 5. Professional Email HTML builders ──────────────────────

/**
 * Google Fonts CDN link for SolaimanLipi-compatible Bengali web font.
 * Since SolaimanLipi is a desktop font, we use Noto Sans Bengali as
 * the closest available web font for email, with SolaimanLipi as
 * the first-choice local fallback.
 */
const FONT_IMPORT = `<link href="https://fonts.googleapis.com/css2?family=Noto+Sans+Bengali:wght@400;600;700&display=swap" rel="stylesheet">`;

const FONT_STACK = `'SolaimanLipi', 'Kalpurush', 'Noto Sans Bengali', 'Arial Unicode MS', sans-serif`;

/** Number formatting with Bengali digits */
function toBnNum(n) {
    return String(n).replace(/\d/g, ch => '০১২৩৪৫৬৭৮৯'[+ch]);
}

/** Format taka amounts nicely */
function formatTaka(val) {
    if (val === '—' || val === null || val === undefined) return '—';
    const num = Number(val);
    if (isNaN(num)) return val;
    return '৳\u202f' + num.toLocaleString('bn-BD');
}

/** Priority badge HTML */
function priorityBadge(priority) {
    const map = {
        high:   { bg: '#fee2e2', color: '#dc2626', border: '#fca5a5', label: 'উচ্চ' },
        medium: { bg: '#fef3c7', color: '#d97706', border: '#fcd34d', label: 'সাধারণ' },
        low:    { bg: '#dcfce7', color: '#16a34a', border: '#86efac', label: 'কম' },
    };
    const s = map[priority] || { bg: '#f3f4f6', color: '#6b7280', border: '#d1d5db', label: priority };
    return `<span style="display:inline-block;padding:2px 9px;border-radius:20px;font-size:.75rem;font-weight:700;background:${s.bg};color:${s.color};border:1px solid ${s.border}">${s.label}</span>`;
}

/** Type / category badge */
function typeBadge(label, accent = '#251577') {
    return `<span style="display:inline-block;padding:2px 9px;border-radius:20px;font-size:.75rem;font-weight:600;background:#eef0fc;color:${accent};border:1px solid #c7cef5">${label}</span>`;
}

/**
 * Shared professional email shell.
 *
 * @param {object} opts
 *   headerAccent  — left side of header gradient (defaults to brand blue)
 *   headerAccent2 — right side
 *   icon          — emoji icon string
 *   title         — module title
 *   subtitle      — small tagline under the title
 *   dateLabel     — Bengali date string
 *   slot          — 'morning' | 'evening'
 *   officeName    — string or null
 *   bodyHTML      — inner body HTML
 *   statCards     — optional array of {icon, label, value} summary cards
 */
function emailShell({ headerAccent = '#251577', headerAccent2 = '#3730a3', icon, title,
                       subtitle = '', dateLabel, slot, officeName, bodyHTML, statCards = [] }) {

    const slotLabel = slot === 'morning' ? '🌅 সকালের ডাইজেস্ট' : '🌆 সন্ধ্যার ডাইজেস্ট';

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
<meta name="color-scheme" content="light">
${FONT_IMPORT}
<title>${title}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+Bengali:wght@400;600;700&display=swap');
  * { box-sizing: border-box; }
  body { margin: 0; padding: 0; background: #eef0f8; }
  @media (max-width: 620px) {
    .email-wrapper { margin: 0 !important; border-radius: 0 !important; }
    .email-pad     { padding: 20px 16px !important; }
    .stat-td       { display: block !important; width: 100% !important; padding: 0 0 8px !important; }
    .data-table td, .data-table th { padding: 8px 6px !important; font-size: .78rem !important; }
  }
</style>
</head>
<body style="margin:0;padding:0;background:#eef0f8;font-family:${FONT_STACK}">

<!-- Outer wrapper -->
<table role="presentation" width="100%" cellpadding="0" cellspacing="0">
<tr><td align="center" style="padding:28px 12px 40px">

<!-- Email card -->
<table role="presentation" class="email-wrapper" width="600" cellpadding="0" cellspacing="0"
       style="max-width:600px;width:100%;background:#ffffff;border-radius:16px;overflow:hidden;
              box-shadow:0 8px 32px rgba(37,21,119,.13),0 2px 8px rgba(0,0,0,.06)">

  <!-- ═══ HEADER ═══ -->
  <tr>
    <td style="background:linear-gradient(135deg,${headerAccent} 0%,${headerAccent2} 100%);padding:0">

      <!-- Top accent bar -->
      <div style="height:4px;background:linear-gradient(90deg,#FFC400,#FFE066,#FFC400)"></div>

      <!-- Logo + Office row -->
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td style="padding:18px 28px 10px">
            <table role="presentation" cellpadding="0" cellspacing="0">
              <tr>
                <td style="background:rgba(255,255,255,.18);border-radius:10px;padding:6px 14px">
                  <span style="color:#ffffff;font-size:.8rem;font-weight:700;letter-spacing:.04em;font-family:${FONT_STACK}">
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

      <!-- Central icon + title -->
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td align="center" style="padding:10px 28px 20px">
            <div style="width:64px;height:64px;background:rgba(255,255,255,.2);border-radius:50%;
                        display:inline-flex;align-items:center;justify-content:center;
                        font-size:2rem;line-height:64px;margin-bottom:12px">${icon}</div>
            <h1 style="margin:0 0 6px;color:#ffffff;font-size:1.45rem;font-weight:700;
                       letter-spacing:.01em;font-family:${FONT_STACK}">${title}</h1>
            ${subtitle ? `<p style="margin:0 0 8px;color:rgba(255,255,255,.8);font-size:.85rem;font-family:${FONT_STACK}">${subtitle}</p>` : ''}
            <!-- Date + slot pill -->
            <div style="display:inline-block;background:rgba(255,255,255,.2);border:1px solid rgba(255,255,255,.35);
                        border-radius:20px;padding:5px 16px;margin-top:4px">
              <span style="color:#ffffff;font-size:.82rem;font-family:${FONT_STACK}">${slotLabel} &nbsp;·&nbsp; ${dateLabel}</span>
            </div>
          </td>
        </tr>
      </table>

      <!-- Bottom wave divider -->
      <div style="height:28px;background:#ffffff;clip-path:ellipse(55% 100% at 50% 100%)"></div>
    </td>
  </tr>

  <!-- ═══ BODY ═══ -->
  <tr>
    <td class="email-pad" style="padding:28px 32px 32px">

      ${statCardsHTML}
      ${bodyHTML}

      <!-- ─── Tips / Footer note ─── -->
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-top:28px">
        <tr>
          <td style="background:linear-gradient(135deg,#f5f5ff,#eef0fc);border:1.5px solid #dde0f8;
                     border-radius:12px;padding:16px 20px">
            <p style="margin:0 0 6px;font-size:.82rem;font-weight:700;color:#251577;font-family:${FONT_STACK}">
              💡 দ্রুত টিপস
            </p>
            <p style="margin:0;font-size:.8rem;color:#4b5563;line-height:1.6;font-family:${FONT_STACK}">
              নিয়মিত রেকর্ড আপডেট রাখুন। কোনো মুলতবি কাজ থাকলে দ্রুত সম্পন্ন করুন।
              পরবর্তী ডাইজেস্ট ${slot === 'morning' ? 'সন্ধ্যা ৮টায়' : 'আগামীকাল সকাল ৮টায়'} পাঠানো হবে।
            </p>
          </td>
        </tr>
      </table>

    </td>
  </tr>

  <!-- ═══ FOOTER ═══ -->
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
            <span style="font-size:1.5rem" title="Office Management System">📊</span>
          </td>
        </tr>
      </table>
    </td>
  </tr>

</table>
<!-- /Email card -->

</td></tr>
</table>
<!-- /Outer wrapper -->

</body>
</html>`;
}

// ─────────────────────────────────────────────────────────────
/** Professional table builder */
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
                     font-weight:700;background:#251577;font-family:${FONT_STACK};
                     white-space:nowrap;border-bottom:2px solid #1e0f60`;

    const ths = headers.map(h => `<th style="${thStyle}">${h}</th>`).join('');

    const trs = displayRows.map((row, i) => {
        const vals = Object.values(row);
        const keys = Object.keys(row);
        const bg   = i % 2 === 0 ? '#f8f8ff' : '#ffffff';

        const tds = vals.map((v, ci) => {
            const key = keys[ci];
            let cell  = v ?? '—';

            // Amount columns — format as taka
            if (amountCols.includes(key) || amountCols.includes(ci)) {
                cell = formatTaka(cell);
            }
            // Badge columns — render as styled badge
            if (badgeCols[key] === 'priority') {
                cell = priorityBadge(String(v));
            } else if (badgeCols[key] === 'type') {
                cell = typeBadge(String(v));
            }

            return `<td style="padding:9px 12px;border-bottom:1px solid #ebebfc;color:#1f2937;
                               font-size:.83rem;font-family:${FONT_STACK};vertical-align:middle">${cell}</td>`;
        }).join('');

        return `<tr style="background:${bg}">${tds}</tr>`;
    }).join('');

    const extraNote = extraRows > 0
        ? `<tr><td colspan="${headers.length}" style="padding:8px 12px;font-size:.78rem;
               color:#6b7280;text-align:center;font-family:${FONT_STACK};background:#f9fafb;
               border-top:1px dashed #d1d5db">
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

// ─────────────────────────────────────────────────────────────
/** Section heading helper */
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

// ─── Per-page email builders ──────────────────────────────────

function buildAdvancePaymentEmail({ slot, dateLabel, data, officeName }) {
    const { active, archived, totalActive, totalArchived, grandTotal } = data;
    const body = `
        ${sectionTitle('✅', 'সক্রিয় পরিশোধ রেকর্ড', active.length)}
        ${tableHTML(
            ['কোড', 'নাম', 'শাখা', 'ধরণ', 'বিবরণ', 'টাকা', 'তারিখ'], active,
            'কোনো সক্রিয় রেকর্ড নেই।',
            { amountCols: ['amount'], badgeCols: { type: 'type' } }
        )}
        ${active.length > 0 ? `<p style="text-align:right;font-weight:700;color:#0f766e;font-family:${FONT_STACK};margin:8px 0 24px">সক্রিয় মোট: ${formatTaka(totalActive)}</p>` : ''}

        ${sectionTitle('📦', 'আর্কাইভ রেকর্ড', archived.length)}
        ${tableHTML(
            ['কোড', 'নাম', 'শাখা', 'ধরণ', 'বিবরণ', 'টাকা', 'তারিখ'], archived,
            'কোনো আর্কাইভ রেকর্ড নেই।',
            { amountCols: ['amount'], badgeCols: { type: 'type' } }
        )}
        ${archived.length > 0 ? `<p style="text-align:right;font-weight:700;color:#6b7280;font-family:${FONT_STACK};margin:8px 0 0">আর্কাইভ মোট: ${formatTaka(totalArchived)}</p>` : ''}`;
    return emailShell({
        headerAccent: '#0f766e', headerAccent2: '#0d9488',
        icon: '💵', title: 'অগ্রিম পরিশোধ — সম্পূর্ণ তালিকা',
        subtitle: 'সক্রিয় ও আর্কাইভ সহ সকল রেকর্ড',
        dateLabel, slot, officeName, bodyHTML: body,
        statCards: [
            { icon: '✅', label: 'সক্রিয় রেকর্ড',  value: toBnNum(active.length) },
            { icon: '📦', label: 'আর্কাইভ রেকর্ড', value: toBnNum(archived.length) },
            { icon: '💰', label: 'সক্রিয় মোট টাকা', value: formatTaka(totalActive) },
            { icon: '🏦', label: 'সর্বমোট টাকা',    value: formatTaka(grandTotal) },
        ]
    });
}

function buildBusinessStatsEmail({ slot, dateLabel, data, officeName }) {
    const { rows, total } = data;
    const body = `
        ${sectionTitle('📊', 'সকল ব্যবসা পরিসংখ্যান রিপোর্ট', rows.length)}
        ${tableHTML(
            ['তারিখ', 'ইনচার্জ', 'ব্যবসা সাল', 'পরিমাণ'], rows,
            'কোনো সংরক্ষিত রিপোর্ট পাওয়া যায়নি।',
            { amountCols: ['amount'] }
        )}
        ${rows.length > 0 ? `<p style="text-align:right;font-weight:700;color:#251577;font-family:${FONT_STACK};margin:8px 0 0">মোট: ${formatTaka(total)}</p>` : ''}`;
    return emailShell({
        headerAccent: '#251577', headerAccent2: '#1d4ed8',
        icon: '📊', title: 'ব্যবসা পরিসংখ্যান — সম্পূর্ণ তালিকা',
        subtitle: 'সকল সংরক্ষিত রিপোর্টের বিবরণ',
        dateLabel, slot, officeName, bodyHTML: body,
        statCards: [
            { icon: '📋', label: 'মোট রিপোর্ট', value: toBnNum(rows.length) },
            { icon: '💰', label: 'মোট পরিমাণ',  value: formatTaka(total) },
        ]
    });
}

function buildDonationEmail({ slot, dateLabel, data, officeName }) {
    const { active, archived, totalActive, totalArchived } = data;
    const body = `
        ${sectionTitle('✅', 'সক্রিয় অনুদান রেকর্ড', active.length)}
        ${tableHTML(
            ['কোড', 'নাম', 'শাখা', 'ধরণ', 'টাকা', 'তারিখ'], active,
            'কোনো সক্রিয় অনুদান রেকর্ড নেই।',
            { amountCols: ['amount'], badgeCols: { type: 'type' } }
        )}
        ${active.length > 0 ? `<p style="text-align:right;font-weight:700;color:#7c3aed;font-family:${FONT_STACK};margin:8px 0 24px">সক্রিয় মোট: ${formatTaka(totalActive)}</p>` : ''}

        ${sectionTitle('📦', 'আর্কাইভ রেকর্ড', archived.length)}
        ${tableHTML(
            ['কোড', 'নাম', 'শাখা', 'ধরণ', 'টাকা', 'তারিখ'], archived,
            'কোনো আর্কাইভ রেকর্ড নেই।',
            { amountCols: ['amount'], badgeCols: { type: 'type' } }
        )}
        ${archived.length > 0 ? `<p style="text-align:right;font-weight:700;color:#6b7280;font-family:${FONT_STACK};margin:8px 0 0">আর্কাইভ মোট: ${formatTaka(totalArchived)}</p>` : ''}`;
    return emailShell({
        headerAccent: '#7c3aed', headerAccent2: '#251577',
        icon: '🤝', title: 'অনুদান — সম্পূর্ণ তালিকা',
        subtitle: 'সক্রিয় ও আর্কাইভ সহ সকল রেকর্ড',
        dateLabel, slot, officeName, bodyHTML: body,
        statCards: [
            { icon: '✅', label: 'সক্রিয় রেকর্ড',  value: toBnNum(active.length) },
            { icon: '📦', label: 'আর্কাইভ রেকর্ড', value: toBnNum(archived.length) },
            { icon: '💰', label: 'সক্রিয় মোট',     value: formatTaka(totalActive) },
        ]
    });
}

function buildHelpEmail({ slot, dateLabel, data, officeName }) {
    const { pending, done } = data;
    const today = new Date().toISOString().split('T')[0];
    const overdue = pending.filter(r => r.date !== '(তারিখ নেই)' && r.date < today).length;
    const body = `
        ${sectionTitle('⏳', 'মুলতবি টাস্কসমূহ', pending.length)}
        ${pending.length > 0 ? `<p style="font-size:.82rem;color:#dc2626;font-weight:600;margin:0 0 12px;font-family:${FONT_STACK}">
            ⚠️ এই টাস্কগুলো এখনও সম্পন্ন হয়নি। দ্রুত সম্পন্ন করুন।
           </p>` : ''}
        ${tableHTML(
            ['টাস্কের শিরোনাম', 'নির্ধারিত তারিখ'], pending,
            'অভিনন্দন! সকল টাস্ক সম্পন্ন হয়েছে। দারুণ কাজ!'
        )}

        ${sectionTitle('✅', 'সম্পন্ন টাস্কসমূহ', done.length)}
        ${tableHTML(
            ['টাস্কের শিরোনাম', 'তারিখ'], done,
            'এখনো কোনো টাস্ক সম্পন্ন হয়নি।'
        )}`;
    return emailShell({
        headerAccent: '#b45309', headerAccent2: '#f59e0b',
        icon: '📋', title: 'সহায়তা — সম্পূর্ণ টাস্ক তালিকা',
        subtitle: 'মুলতবি ও সম্পন্ন সকল টাস্কের বিবরণ',
        dateLabel, slot, officeName, bodyHTML: body,
        statCards: [
            { icon: '⏳', label: 'মুলতবি টাস্ক',    value: toBnNum(pending.length) },
            { icon: '🔴', label: 'মেয়াদোত্তীর্ণ',  value: toBnNum(overdue) },
            { icon: '✅', label: 'সম্পন্ন টাস্ক',   value: toBnNum(done.length) },
        ]
    });
}

function buildIssueEmail({ slot, dateLabel, data, officeName }) {
    const { pending, resolved } = data;
    const highCount = pending.filter(r => r.priority === 'high').length;
    const body = `
        ${sectionTitle('⚠️', 'চলমান সমস্যাসমূহ', pending.length)}
        ${highCount > 0 ? `<p style="font-size:.82rem;color:#dc2626;font-weight:600;margin:0 0 12px;font-family:${FONT_STACK}">
            🔴 ${toBnNum(highCount)}টি উচ্চ-গুরুত্বের সমস্যা রয়েছে — তাৎক্ষণিক পদক্ষেপ নিন।
           </p>` : ''}
        ${tableHTML(
            ['তারিখ', 'সমস্যার বিবরণ', 'গুরুত্ব'], pending,
            'কোনো চলমান সমস্যা নেই। সবকিছু ঠিকঠাক আছে!',
            { badgeCols: { priority: 'priority' } }
        )}

        ${sectionTitle('✅', 'সমাধানকৃত সমস্যাসমূহ', resolved.length)}
        ${tableHTML(
            ['তারিখ', 'সমস্যার বিবরণ', 'গুরুত্ব'], resolved,
            'কোনো সমাধানকৃত সমস্যা নেই।',
            { badgeCols: { priority: 'priority' } }
        )}`;
    return emailShell({
        headerAccent: '#dc2626', headerAccent2: '#9f1239',
        icon: '⚠️', title: 'সমস্যা ও সমাধান — সম্পূর্ণ তালিকা',
        subtitle: 'চলমান ও সমাধানকৃত সকল সমস্যার স্ট্যাটাস',
        dateLabel, slot, officeName, bodyHTML: body,
        statCards: [
            { icon: '🔴', label: 'উচ্চ গুরুত্ব',      value: toBnNum(highCount) },
            { icon: '📌', label: 'মোট চলমান',          value: toBnNum(pending.length) },
            { icon: '✅', label: 'সমাধানকৃত',          value: toBnNum(resolved.length) },
        ]
    });
}

function buildPremiumEmail({ slot, dateLabel, data, officeName }) {
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
        ${tableHTML(
            ['ধরণ', 'স্লিপ নং', 'টাকা', 'জমা', 'বকেয়া', 'তারিখ'], formattedRows,
            'কোনো প্রিমিয়াম জমার রেকর্ড নেই।',
            { badgeCols: { type: 'type' } }
        )}
        ${rows.length > 0 ? `
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-top:10px">
          <tr>
            <td style="text-align:right;font-weight:700;color:#0369a1;font-family:${FONT_STACK};padding:4px 0">
              মোট জমা: ${formatTaka(totalDeposit)} &nbsp;|&nbsp; মোট বকেয়া: ${formatTaka(totalBalance)}
            </td>
          </tr>
        </table>` : ''}`;
    return emailShell({
        headerAccent: '#0369a1', headerAccent2: '#251577',
        icon: '🏦', title: 'প্রিমিয়াম জমা — সম্পূর্ণ তালিকা',
        subtitle: 'সকল প্রিমিয়াম কালেকশনের বিবরণ',
        dateLabel, slot, officeName, bodyHTML: body,
        statCards: [
            { icon: '📑', label: 'মোট এন্ট্রি',  value: toBnNum(rows.length) },
            { icon: '✅', label: 'মোট জমা',       value: formatTaka(totalDeposit) },
            { icon: '🔔', label: 'মোট বকেয়া',    value: formatTaka(totalBalance) },
        ]
    });
}

// ── 6. All 6 page definitions ─────────────────────────────────

function getPageDefinitions(slot) {
    return [
        {
            key: 'advance_payment', name: 'অগ্রিম পরিশোধ', icon: '💵', prefKey: 'advance_payment',
            fetch:      (uid) => fetchAdvancePayments(uid),
            isEmpty:    (data) => data.active.length === 0 && data.archived.length === 0,
            totalRows:  (data) => data.active.length + data.archived.length,
            buildEmail: (params) => buildAdvancePaymentEmail(params),
            subject:    (slot, dateLabel, data) =>
                `💵 অগ্রিম পরিশোধ ${slot === 'morning' ? '🌅 সকাল' : '🌆 সন্ধ্যা'} — সক্রিয়: ${data.active.length}টি · আর্কাইভ: ${data.archived.length}টি · ${dateLabel}`,
        },
        {
            key: 'business_stats', name: 'ব্যবসা পরিসংখ্যান', icon: '📊', prefKey: 'business_stats',
            fetch:      (uid) => fetchBusinessStats(uid),
            isEmpty:    (data) => data.rows.length === 0,
            totalRows:  (data) => data.rows.length,
            buildEmail: (params) => buildBusinessStatsEmail(params),
            subject:    (slot, dateLabel, data) =>
                `📊 ব্যবসা পরিসংখ্যান ${slot === 'morning' ? '🌅 সকাল' : '🌆 সন্ধ্যা'} — ${data.rows.length}টি রিপোর্ট · ${dateLabel}`,
        },
        {
            key: 'donation', name: 'অনুদান', icon: '🤝', prefKey: 'donation',
            fetch:      (uid) => fetchDonations(uid),
            isEmpty:    (data) => data.active.length === 0 && data.archived.length === 0,
            totalRows:  (data) => data.active.length + data.archived.length,
            buildEmail: (params) => buildDonationEmail(params),
            subject:    (slot, dateLabel, data) =>
                `🤝 অনুদান ${slot === 'morning' ? '🌅 সকাল' : '🌆 সন্ধ্যা'} — সক্রিয়: ${data.active.length}টি · আর্কাইভ: ${data.archived.length}টি · ${dateLabel}`,
        },
        {
            key: 'help', name: 'সহায়তা', icon: '📋', prefKey: 'help',
            fetch:      (uid) => fetchAllTasks(uid),
            isEmpty:    (data) => data.pending.length === 0 && data.done.length === 0,
            totalRows:  (data) => data.pending.length + data.done.length,
            buildEmail: (params) => buildHelpEmail(params),
            subject:    (slot, dateLabel, data) =>
                `📋 সহায়তা ${slot === 'morning' ? '🌅 সকাল' : '🌆 সন্ধ্যা'} — মুলতবি: ${data.pending.length}টি · সম্পন্ন: ${data.done.length}টি · ${dateLabel}`,
        },
        {
            key: 'office_issue', name: 'সমস্যা ও সমাধান', icon: '⚠️', prefKey: 'office_issue',
            fetch:      (uid) => fetchIssues(uid),
            isEmpty:    (data) => data.pending.length === 0 && data.resolved.length === 0,
            totalRows:  (data) => data.pending.length + data.resolved.length,
            buildEmail: (params) => buildIssueEmail(params),
            subject:    (slot, dateLabel, data) =>
                `⚠️ সমস্যা ও সমাধান ${slot === 'morning' ? '🌅 সকাল' : '🌆 সন্ধ্যা'} — চলমান: ${data.pending.length}টি · সমাধান: ${data.resolved.length}টি · ${dateLabel}`,
        },
        {
            key: 'premium_submit', name: 'প্রিমিয়াম জমা', icon: '🏦', prefKey: 'premium_submit',
            fetch:      (uid) => fetchPremiumStatements(uid),
            isEmpty:    (data) => data.rows.length === 0,
            totalRows:  (data) => data.rows.length,
            buildEmail: (params) => buildPremiumEmail(params),
            subject:    (slot, dateLabel, data) =>
                `🏦 প্রিমিয়াম জমা ${slot === 'morning' ? '🌅 সকাল' : '🌆 সন্ধ্যা'} — ${data.rows.length}টি এন্ট্রি · ${dateLabel}`,
        },
    ];
}

// ── 7. Send email ─────────────────────────────────────────────

async function sendEmail({ toEmail, toName, subject, htmlBody }) {
    await transporter.sendMail({
        from:    `"অফিস ম্যানেজমেন্ট সিস্টেম" <${process.env.GMAIL_USER}>`,
        to:      `"${toName}" <${toEmail}>`,
        subject: subject,
        html:    htmlBody
    });
}

// ── 8. Main ───────────────────────────────────────────────────

async function run() {
    const slot      = getDigestSlot();
    const today     = getTodayStr();
    const dateLabel = getBengaliDateLabel();
    const pages     = getPageDefinitions(slot);

    console.log(`🚀 Email worker v3 started — slot: ${slot}`);
    console.log(`📅 Today (Dhaka): ${today} | UTC: ${new Date().toISOString()}\n`);

    let totalSent = 0, totalFailed = 0, totalSkipped = 0;

    try {
        const usersRef  = db.collection('artifacts').doc('default-app-id').collection('users');
        const usersSnap = await usersRef.get();

        if (usersSnap.empty) { console.log('ℹ️  No users found.'); process.exit(0); }
        console.log(`👥 Found ${usersSnap.size} user(s).\n`);

        for (const userDoc of usersSnap.docs) {
            const uid = userDoc.id;
            console.log(`── User: ${uid}`);

            const subRef  = usersRef.doc(uid).collection('data').doc('emailSubscription');
            const subSnap = await subRef.get();

            if (!subSnap.exists)     { console.log(`  ⏭️  No email subscription — skip.`); totalSkipped++; continue; }
            const sub = subSnap.data();
            if (!sub.active)         { console.log(`  ⏭️  Subscription inactive — skip.`); totalSkipped++; continue; }
            if (!sub.email)          { console.log(`  ⚠️  No email address — skip.`); totalSkipped++; continue; }
            if (!sub.prefs?.[slot])  { console.log(`  ⏭️  Opted out of ${slot} digest — skip.`); totalSkipped++; continue; }

            let officeName = null;
            try {
                const pSnap = await usersRef.doc(uid).collection('data').doc('profile').get();
                if (pSnap.exists) officeName = pSnap.data().officeName || null;
            } catch (_) {}

            const toName = officeName || sub.email;

            for (const page of pages) {
                const pagePrefs   = (sub.prefs && sub.prefs.pages) || {};
                const pageEnabled = pagePrefs[page.key] !== false;

                if (!pageEnabled) {
                    console.log(`  ⏭️  [${page.name}] opted out — skip.`);
                    totalSkipped++;
                    continue;
                }

                try {
                    const data = await page.fetch(uid);

                    // Skip sending if module has no records at all
                    if (page.isEmpty(data)) {
                        console.log(`  ⏭️  [${page.name}] no records — skip (no empty email sent).`);
                        totalSkipped++;
                        continue;
                    }

                    const htmlBody = page.buildEmail({ slot, dateLabel, data, officeName });
                    const subject  = page.subject(slot, dateLabel, data);
                    await sendEmail({ toEmail: sub.email, toName, subject, htmlBody });
                    console.log(`  📧 [${page.name}] → ${sub.email} — OK (${page.totalRows(data)} records)`);
                    totalSent++;
                } catch (err) {
                    console.error(`  ❌ [${page.name}] FAILED: ${err.message}`);
                    totalFailed++;
                }

                await new Promise(r => setTimeout(r, 400));
            }
            console.log('');
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
