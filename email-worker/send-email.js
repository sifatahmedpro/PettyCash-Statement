/**
 * ============================================================
 * email-worker/send-email.js  —  v2.0 (PER-PAGE STATUS EMAILS)
 *
 * WHAT CHANGED vs v1:
 *   Instead of one combined digest, each of the 6 app pages
 *   now gets its own dedicated status email — sent separately
 *   so the user receives 6 emails per digest slot (morning /
 *   evening), one for each module:
 *
 *   1. অগ্রিম পরিশোধ   — advance-payment
 *   2. ব্যবসা পরিসংখ্যান — business-statistics
 *   3. অনুদান           — donation
 *   4. সহায়তা           — help  (pending tasks only)
 *   5. সমস্যা ও সমাধান  — office-issue-solution
 *   6. প্রিমিয়াম জমা    — premium-submit
 *
 * Each email has its own subject line, colour header, and
 * data table matched to that module's Firestore collection.
 *
 * Firestore paths read:
 *   users/{uid}/payments             → advance-payment
 *   users/{uid}/businessStats/archive → business-statistics
 *   users/{uid}/donations            → donation
 *   users/{uid}/tasks                → help (pending tasks)
 *   users/{uid}/issues               → office-issue-solution
 *   users/{uid}/premiumStatements    → premium-submit
 *
 * GitHub Secrets required (unchanged):
 *   FIREBASE_PROJECT_ID, FIREBASE_CLIENT_EMAIL, FIREBASE_PRIVATE_KEY
 *   GMAIL_USER, GMAIL_APP_PASSWORD
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

// ── 4. Firestore data fetchers (one per page) ─────────────────

const BASE = (uid) =>
    db.collection('artifacts').doc('default-app-id').collection('users').doc(uid);

/** 1. অগ্রিম পরিশোধ — recent payments (today) */
async function fetchAdvancePayments(uid, today) {
    const snap = await BASE(uid).collection('payments')
        .where('date', '>=', today)
        .orderBy('date', 'desc')
        .get();
    const rows = [];
    snap.forEach(d => {
        const data = d.data();
        rows.push({
            name:   data.name   || '—',
            type:   data.type   || '—',
            amount: data.amount != null ? data.amount : '—',
            date:   data.date   || '—',
        });
    });
    return rows;
}

/** 2. ব্যবসা পরিসংখ্যান — archived reports */
async function fetchBusinessStats(uid) {
    const snap = await BASE(uid).collection('businessStats').get();
    const rows = [];
    snap.forEach(d => {
        if (d.id === 'archive') return; // skip meta doc
        const data = d.data();
        rows.push({
            date:     data.reportDate    || data.date || '—',
            incharge: data.inchargeName  || '—',
            business: data.businessYear  || '—',
            amount:   data.businessAmount != null ? data.businessAmount : '—',
        });
    });
    // Also check archive sub-docs
    try {
        const archSnap = await BASE(uid).collection('businessStats')
            .doc('archive').collection('reports')
            .orderBy('reportDate', 'desc').limit(5).get();
        archSnap.forEach(d => {
            const data = d.data();
            rows.push({
                date:     data.reportDate   || '—',
                incharge: data.inchargeName || '—',
                business: data.businessYear || '—',
                amount:   data.businessAmount != null ? data.businessAmount : '—',
            });
        });
    } catch (_) {}
    return rows;
}

/** 3. অনুদান — recent donations */
async function fetchDonations(uid, today) {
    const snap = await BASE(uid).collection('donations')
        .where('date', '>=', today)
        .orderBy('date', 'desc')
        .get();
    const rows = [];
    snap.forEach(d => {
        const data = d.data();
        rows.push({
            name:   data.name   || '—',
            type:   data.type   || '—',
            amount: data.amount != null ? data.amount : '—',
            date:   data.date   || '—',
        });
    });
    return rows;
}

/** 4. সহায়তা — pending tasks (reuse existing logic) */
async function fetchPendingTasks(uid, today) {
    const snap = await BASE(uid).collection('tasks')
        .where('status', '!=', 'done').get();
    const rows = [];
    snap.forEach(d => {
        const data = d.data();
        if (!data.date) {
            rows.push({ title: data.title || 'শিরোনামহীন', date: '(তারিখ নেই)' });
            return;
        }
        let dateStr;
        if (data.date.toDate) {
            const dt = data.date.toDate();
            dateStr = dt.getFullYear() + '-' +
                String(dt.getMonth() + 1).padStart(2, '0') + '-' +
                String(dt.getDate()).padStart(2, '0');
        } else {
            dateStr = String(data.date).split('T')[0];
        }
        if (dateStr <= today) {
            rows.push({ title: data.title || 'শিরোনামহীন', date: dateStr });
        }
    });
    return rows;
}

/** 5. সমস্যা ও সমাধান — pending issues */
async function fetchIssues(uid) {
    const snap = await BASE(uid).collection('issues')
        .where('status', '==', 'pending').get();
    const rows = [];
    snap.forEach(d => {
        const data = d.data();
        rows.push({
            date:        data.date        || '—',
            description: (data.description || '—').slice(0, 60),
            priority:    data.priority    || '—',
        });
    });
    return rows;
}

/** 6. প্রিমিয়াম জমা — today's premium statements */
async function fetchPremiumStatements(uid, today) {
    const types   = ['1st_year', 'renewal', 'deferred', 'mr', 'loan'];
    const typeLabels = {
        '1st_year': '১ম বর্ষ', renewal: 'নবায়ন',
        deferred:   'ডেফার্ড', mr:      'এম আর', loan: 'ঋণ'
    };
    const rows = [];
    for (const type of types) {
        try {
            const snap = await BASE(uid).collection('premiumStatements')
                .where('type', '==', type)
                .where('date', '>=', today)
                .get();
            snap.forEach(d => {
                const data = d.data();
                rows.push({
                    type:    typeLabels[type] || type,
                    slipNo:  data.slipNo  || '—',
                    amount:  data.amount  != null ? data.amount : '—',
                    deposit: data.deposit != null ? data.deposit : '—',
                    balance: data.balance != null ? data.balance : '—',
                    date:    data.date    || '—',
                });
            });
        } catch (_) {}
    }
    return rows;
}

// ── 5. Email HTML builders (one per page) ────────────────────

/** Shared header/footer shell */
function emailShell({ headerColor, icon, title, dateLabel, slot, officeName, bodyHTML }) {
    const slotLabel = slot === 'morning' ? '🌅 সকালের ডাইজেস্ট' : '🌆 সন্ধ্যার ডাইজেস্ট';
    return `<!DOCTYPE html>
<html lang="bn">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:'Segoe UI',Tahoma,sans-serif">
<div style="max-width:600px;margin:32px auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 4px 16px rgba(0,0,0,.08)">
    <div style="background:${headerColor};padding:24px 30px;text-align:center">
        <div style="font-size:2rem">${icon}</div>
        <h1 style="margin:6px 0 2px;color:#fff;font-size:1.2rem">${title}</h1>
        <p style="margin:0;color:rgba(255,255,255,.8);font-size:.82rem">${slotLabel} · ${dateLabel}</p>
        ${officeName ? `<p style="margin:6px 0 0;color:rgba(255,255,255,.85);font-size:.82rem">🏢 ${officeName}</p>` : ''}
    </div>
    <div style="padding:24px 30px">
        ${bodyHTML}
        <p style="color:#9ca3af;font-size:.75rem;margin:24px 0 0;text-align:center">
            এই ইমেইল স্বয়ংক্রিয়ভাবে পাঠানো হয়েছে। ইমেইল বন্ধ করতে অ্যাকাউন্ট সেটিংসে যান।
        </p>
    </div>
</div>
</body></html>`;
}

/** Generic table builder */
function tableHTML(headers, rows, emptyMsg) {
    if (rows.length === 0) {
        return `<p style="color:#16a34a;background:#f0fdf4;padding:10px 14px;border-radius:8px;font-size:.88rem;text-align:center">✅ ${emptyMsg}</p>`;
    }
    const ths = headers.map(h =>
        `<th style="padding:8px 10px;text-align:left;font-size:.82rem;color:#6b7280;font-weight:600;background:#f3f4f6">${h}</th>`
    ).join('');
    const trs = rows.map((row, i) => {
        const tds = Object.values(row).map(v =>
            `<td style="padding:8px 10px;border-bottom:1px solid #e5e7eb;color:#374151;font-size:.85rem">${v}</td>`
        ).join('');
        return `<tr style="background:${i % 2 === 0 ? '#f9fafb' : '#fff'}">${tds}</tr>`;
    }).join('');
    return `<table style="width:100%;border-collapse:collapse;border:1px solid #e5e7eb;border-radius:8px;overflow:hidden">
        <thead><tr>${ths}</tr></thead><tbody>${trs}</tbody>
    </table>`;
}

// ── Per-page email builders ───────────────────────────────────

function buildAdvancePaymentEmail({ slot, dateLabel, rows, officeName }) {
    const body = `
        <p style="color:#374151;margin:0 0 14px">আজকের অগ্রিম পরিশোধের রেকর্ড (${rows.length}টি এন্ট্রি)</p>
        ${tableHTML(['নাম', 'পরিশোধের ধরণ', 'টাকা', 'তারিখ'], rows, 'আজকে কোনো পরিশোধ নেই।')}`;
    return emailShell({
        headerColor: 'linear-gradient(135deg,#0f766e,#0d9488)',
        icon: '💵', title: 'অগ্রিম পরিশোধ স্ট্যাটাস',
        dateLabel, slot, officeName, bodyHTML: body
    });
}

function buildBusinessStatsEmail({ slot, dateLabel, rows, officeName }) {
    const body = `
        <p style="color:#374151;margin:0 0 14px">সংরক্ষিত ব্যবসা পরিসংখ্যান রিপোর্ট (${rows.length}টি)</p>
        ${tableHTML(['তারিখ', 'ইনচার্জ', 'সাল', 'পরিমাণ'], rows, 'কোনো সংরক্ষিত রিপোর্ট নেই।')}`;
    return emailShell({
        headerColor: 'linear-gradient(135deg,#1d4ed8,#3b82f6)',
        icon: '📊', title: 'ব্যবসা পরিসংখ্যান স্ট্যাটাস',
        dateLabel, slot, officeName, bodyHTML: body
    });
}

function buildDonationEmail({ slot, dateLabel, rows, officeName }) {
    const body = `
        <p style="color:#374151;margin:0 0 14px">আজকের অনুদানের রেকর্ড (${rows.length}টি এন্ট্রি)</p>
        ${tableHTML(['নাম', 'দানের ধরণ', 'টাকা', 'তারিখ'], rows, 'আজকে কোনো অনুদান নেই।')}`;
    return emailShell({
        headerColor: 'linear-gradient(135deg,#7c3aed,#a855f7)',
        icon: '🤝', title: 'অনুদান স্ট্যাটাস',
        dateLabel, slot, officeName, bodyHTML: body
    });
}

function buildHelpEmail({ slot, dateLabel, rows, officeName }) {
    const body = `
        <p style="color:#374151;margin:0 0 14px">মুলতবি টাস্ক (${rows.length}টি)</p>
        ${tableHTML(['টাস্ক শিরোনাম', 'তারিখ'], rows, 'কোনো মুলতবি টাস্ক নেই।')}`;
    return emailShell({
        headerColor: 'linear-gradient(135deg,#b45309,#f59e0b)',
        icon: '📋', title: 'সহায়তা — মুলতবি টাস্ক',
        dateLabel, slot, officeName, bodyHTML: body
    });
}

function buildIssueEmail({ slot, dateLabel, rows, officeName }) {
    const priorityLabel = { high: 'উচ্চ', medium: 'সাধারণ', low: 'কম' };
    const mapped = rows.map(r => ({
        date:        r.date,
        description: r.description,
        priority:    priorityLabel[r.priority] || r.priority,
    }));
    const body = `
        <p style="color:#374151;margin:0 0 14px">চলমান সমস্যা (${rows.length}টি)</p>
        ${tableHTML(['তারিখ', 'বিবরণ', 'গুরুত্ব'], mapped, 'কোনো চলমান সমস্যা নেই।')}`;
    return emailShell({
        headerColor: 'linear-gradient(135deg,#dc2626,#ef4444)',
        icon: '⚠️', title: 'সমস্যা ও সমাধান স্ট্যাটাস',
        dateLabel, slot, officeName, bodyHTML: body
    });
}

function buildPremiumEmail({ slot, dateLabel, rows, officeName }) {
    const body = `
        <p style="color:#374151;margin:0 0 14px">আজকের প্রিমিয়াম জমার রেকর্ড (${rows.length}টি এন্ট্রি)</p>
        ${tableHTML(['ধরণ', 'স্লিপ নং', 'টাকা', 'জমা', 'বকেয়া', 'তারিখ'], rows, 'আজকে কোনো প্রিমিয়াম জমা নেই।')}`;
    return emailShell({
        headerColor: 'linear-gradient(135deg,#0369a1,#0ea5e9)',
        icon: '🏦', title: 'প্রিমিয়াম জমা স্ট্যাটাস',
        dateLabel, slot, officeName, bodyHTML: body
    });
}

// ── 6. All 6 page definitions ─────────────────────────────────

function getPageDefinitions(slot) {
    return [
        {
            key:         'advance_payment',
            name:        'অগ্রিম পরিশোধ',
            icon:        '💵',
            prefKey:     'advance_payment',
            fetch:       (uid, today) => fetchAdvancePayments(uid, today),
            buildEmail:  (params)     => buildAdvancePaymentEmail(params),
            subject:     (slot, dateLabel, rows) =>
                `💵 অগ্রিম পরিশোধ ${slot === 'morning' ? '🌅 সকাল' : '🌆 সন্ধ্যা'} — ${rows.length}টি রেকর্ড · ${dateLabel}`,
        },
        {
            key:         'business_stats',
            name:        'ব্যবসা পরিসংখ্যান',
            icon:        '📊',
            prefKey:     'business_stats',
            fetch:       (uid, _today) => fetchBusinessStats(uid),
            buildEmail:  (params)      => buildBusinessStatsEmail(params),
            subject:     (slot, dateLabel, rows) =>
                `📊 ব্যবসা পরিসংখ্যান ${slot === 'morning' ? '🌅 সকাল' : '🌆 সন্ধ্যা'} — ${rows.length}টি রিপোর্ট · ${dateLabel}`,
        },
        {
            key:         'donation',
            name:        'অনুদান',
            icon:        '🤝',
            prefKey:     'donation',
            fetch:       (uid, today) => fetchDonations(uid, today),
            buildEmail:  (params)     => buildDonationEmail(params),
            subject:     (slot, dateLabel, rows) =>
                `🤝 অনুদান ${slot === 'morning' ? '🌅 সকাল' : '🌆 সন্ধ্যা'} — ${rows.length}টি রেকর্ড · ${dateLabel}`,
        },
        {
            key:         'help',
            name:        'সহায়তা',
            icon:        '📋',
            prefKey:     'help',
            fetch:       (uid, today) => fetchPendingTasks(uid, today),
            buildEmail:  (params)     => buildHelpEmail(params),
            subject:     (slot, dateLabel, rows) =>
                `📋 সহায়তা ${slot === 'morning' ? '🌅 সকাল' : '🌆 সন্ধ্যা'} — ${rows.length}টি মুলতবি টাস্ক · ${dateLabel}`,
        },
        {
            key:         'office_issue',
            name:        'সমস্যা ও সমাধান',
            icon:        '⚠️',
            prefKey:     'office_issue',
            fetch:       (uid, _today) => fetchIssues(uid),
            buildEmail:  (params)      => buildIssueEmail(params),
            subject:     (slot, dateLabel, rows) =>
                `⚠️ সমস্যা ও সমাধান ${slot === 'morning' ? '🌅 সকাল' : '🌆 সন্ধ্যা'} — ${rows.length}টি চলমান · ${dateLabel}`,
        },
        {
            key:         'premium_submit',
            name:        'প্রিমিয়াম জমা',
            icon:        '🏦',
            prefKey:     'premium_submit',
            fetch:       (uid, today) => fetchPremiumStatements(uid, today),
            buildEmail:  (params)     => buildPremiumEmail(params),
            subject:     (slot, dateLabel, rows) =>
                `🏦 প্রিমিয়াম জমা ${slot === 'morning' ? '🌅 সকাল' : '🌆 সন্ধ্যা'} — ${rows.length}টি এন্ট্রি · ${dateLabel}`,
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

    console.log(`🚀 Email worker v2 started — slot: ${slot}`);
    console.log(`📅 Today (Dhaka): ${today} | UTC: ${new Date().toISOString()}\n`);

    let totalSent    = 0;
    let totalFailed  = 0;
    let totalSkipped = 0;

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

            // Load email subscription
            const subRef  = usersRef.doc(uid).collection('data').doc('emailSubscription');
            const subSnap = await subRef.get();

            if (!subSnap.exists) {
                console.log(`  ⏭️  No email subscription — skip.`);
                totalSkipped++;
                continue;
            }

            const sub = subSnap.data();

            if (!sub.active) {
                console.log(`  ⏭️  Subscription inactive — skip.`);
                totalSkipped++;
                continue;
            }

            if (!sub.email) {
                console.log(`  ⚠️  No email address — skip.`);
                totalSkipped++;
                continue;
            }

            // Check global morning/evening slot preference
            if (!sub.prefs || !sub.prefs[slot]) {
                console.log(`  ⏭️  Opted out of ${slot} digest — skip.`);
                totalSkipped++;
                continue;
            }

            // Load office name from profile
            let officeName = null;
            try {
                const profileSnap = await usersRef.doc(uid).collection('data').doc('profile').get();
                if (profileSnap.exists) officeName = profileSnap.data().officeName || null;
            } catch (_) {}

            const toName = officeName || sub.email;

            // Send one email per page
            for (const page of pages) {
                // Per-page preference check: sub.prefs.pages.{key} (optional granular control)
                // Falls back to true (send) if not set — backward compatible.
                const pagePrefs  = (sub.prefs && sub.prefs.pages) || {};
                const pageEnabled = pagePrefs[page.key] !== false; // default ON

                if (!pageEnabled) {
                    console.log(`  ⏭️  [${page.name}] opted out — skip.`);
                    totalSkipped++;
                    continue;
                }

                try {
                    const rows     = await page.fetch(uid, today);
                    const htmlBody = page.buildEmail({ slot, dateLabel, rows, officeName });
                    const subject  = page.subject(slot, dateLabel, rows);

                    await sendEmail({ toEmail: sub.email, toName, subject, htmlBody });
                    console.log(`  📧 [${page.name}] → ${sub.email} — OK (${rows.length} rows)`);
                    totalSent++;
                } catch (err) {
                    console.error(`  ❌ [${page.name}] FAILED: ${err.message}`);
                    totalFailed++;
                }

                // Small delay between emails to avoid Gmail rate limits
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
