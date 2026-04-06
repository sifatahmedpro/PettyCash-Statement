/**
 * ============================================================
 * email-worker/send-email.js
 *
 * GitHub Actions worker — fires at 8 AM and 8 PM Dhaka time.
 * (GitHub Actions runs on UTC; Dhaka = UTC+6)
 *
 * Steps:
 *   1. Connect to Firestore via Firebase Admin SDK
 *   2. Read ALL users who have active email subscriptions
 *   3. For each user, read their pending tasks (if prefs.tasks)
 *   4. Build a rich HTML digest email
 *   5. Send via EmailJS REST API (free plan, no server needed)
 *
 * EMAILJS FREE PLAN LIMITS (as of 2024):
 *   - 200 emails/month
 *   - No server SDK needed — use the REST API directly
 *   - Template variables are passed as JSON
 *
 * REQUIRED GITHUB SECRETS:
 *   FIREBASE_PROJECT_ID
 *   FIREBASE_CLIENT_EMAIL
 *   FIREBASE_PRIVATE_KEY
 *   EMAILJS_SERVICE_ID      ← Settings > Email Services in EmailJS
 *   EMAILJS_TEMPLATE_ID     ← Email Templates in EmailJS
 *   EMAILJS_PUBLIC_KEY      ← Account > API Keys in EmailJS
 *   EMAILJS_PRIVATE_KEY     ← Account > API Keys in EmailJS (for REST)
 *
 * NO npm install needed for EmailJS — pure HTTPS fetch to their API.
 * Only firebase-admin is required.
 * ============================================================
 */

'use strict';

const admin = require('firebase-admin');

// ── 1. Firebase Admin init ────────────────────────────────────

const privateKey = process.env.FIREBASE_PRIVATE_KEY
    ? process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, '\n')
    : null;

if (!process.env.FIREBASE_PROJECT_ID || !process.env.FIREBASE_CLIENT_EMAIL || !privateKey) {
    console.error('❌ Missing Firebase credentials. Check GitHub Secrets.');
    process.exit(1);
}

const EMAILJS_SERVICE_ID  = process.env.EMAILJS_SERVICE_ID;
const EMAILJS_TEMPLATE_ID = process.env.EMAILJS_TEMPLATE_ID;
const EMAILJS_PUBLIC_KEY  = process.env.EMAILJS_PUBLIC_KEY;
const EMAILJS_PRIVATE_KEY = process.env.EMAILJS_PRIVATE_KEY;

if (!EMAILJS_SERVICE_ID || !EMAILJS_TEMPLATE_ID || !EMAILJS_PUBLIC_KEY || !EMAILJS_PRIVATE_KEY) {
    console.error('❌ Missing EmailJS credentials. Check GitHub Secrets.');
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

// ── 2. Dhaka time helpers ─────────────────────────────────────

const DHAKA_OFFSET_MS = 6 * 60 * 60 * 1000;   // UTC+6

function getDhakaDate() {
    const now   = new Date();
    return new Date(now.getTime() + DHAKA_OFFSET_MS);
}

function getTodayStr() {
    const d = getDhakaDate();
    return d.getUTCFullYear() + '-' +
        String(d.getUTCMonth() + 1).padStart(2, '0') + '-' +
        String(d.getUTCDate()).padStart(2, '0');
}

/** "রবিবার, ০৬ এপ্রিল ২০২৫" */
function getBengaliDateLabel() {
    const d     = getDhakaDate();
    const days  = ['রবিবার','সোমবার','মঙ্গলবার','বুধবার','বৃহস্পতিবার','শুক্রবার','শনিবার'];
    const months= ['জানুয়ারি','ফেব্রুয়ারি','মার্চ','এপ্রিল','মে','জুন',
                   'জুলাই','আগস্ট','সেপ্টেম্বর','অক্টোবর','নভেম্বর','ডিসেম্বর'];
    const toBn  = n => String(n).replace(/\d/g, d => '০১২৩৪৫৬৭৮৯'[+d]);
    return `${days[d.getUTCDay()]}, ${toBn(d.getUTCDate())} ${months[d.getUTCMonth()]} ${toBn(d.getUTCFullYear())}`;
}

/** Returns "morning" or "evening" based on Dhaka hour */
function getDigestSlot() {
    const dhakaHour = getDhakaDate().getUTCHours();
    return dhakaHour < 14 ? 'morning' : 'evening';  // 8 AM run = morning, 8 PM (14 UTC) = evening
}

// ── 3. Fetch pending tasks for a user ─────────────────────────

async function getPendingTasks(uid, today) {
    const tasksRef  = db.collection('artifacts').doc('default-app-id')
                       .collection('users').doc(uid).collection('tasks');
    const snap      = await tasksRef.where('status', '!=', 'done').get();
    const pending   = [];

    snap.forEach(docSnap => {
        const data = docSnap.data();
        if (!data.date) {
            pending.push({ title: data.title || 'শিরোনামহীন টাস্ক', date: '(তারিখ নেই)' });
            return;
        }
        let taskDateStr;
        if (data.date.toDate && typeof data.date.toDate === 'function') {
            const d = data.date.toDate();
            taskDateStr = d.getFullYear() + '-' +
                String(d.getMonth() + 1).padStart(2, '0') + '-' +
                String(d.getDate()).padStart(2, '0');
        } else {
            taskDateStr = String(data.date).split('T')[0];
        }
        if (taskDateStr <= today) {
            pending.push({ title: data.title || 'শিরোনামহীন টাস্ক', date: taskDateStr });
        }
    });

    return pending;
}

// ── 4. Build HTML email body ──────────────────────────────────

function buildEmailHTML({ slot, dateLabel, tasks, officeName }) {
    const slotLabel = slot === 'morning' ? '🌅 সকালের ডাইজেস্ট' : '🌆 সন্ধ্যার ডাইজেস্ট';
    const greeting  = slot === 'morning' ? 'সুপ্রভাত! আজকের কাজের তালিকা নিচে দেওয়া হয়েছে।'
                                         : 'শুভ সন্ধ্যা! দিনের শেষে মুলতবি কাজের একটি সারসংক্ষেপ।';

    const taskRows = tasks.length === 0
        ? `<tr><td colspan="2" style="padding:10px 0;color:#16a34a;text-align:center">✅ সব কাজ সম্পন্ন! দারুণ কাজ।</td></tr>`
        : tasks.map((t, i) => `
<tr style="background:${i % 2 === 0 ? '#f9fafb' : '#fff'}">
    <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;color:#374151">${t.title}</td>
    <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;color:#6b7280;white-space:nowrap">${t.date}</td>
</tr>`).join('');

    const taskSection = `
<h2 style="font-size:1rem;color:#374151;margin:20px 0 8px">📋 মুলতবি টাস্কসমূহ (${tasks.length}টি)</h2>
<table style="width:100%;border-collapse:collapse;border:1px solid #e5e7eb;border-radius:8px;overflow:hidden">
    <thead>
        <tr style="background:#f3f4f6">
            <th style="padding:9px 12px;text-align:left;font-size:.85rem;color:#6b7280;font-weight:600">টাস্ক</th>
            <th style="padding:9px 12px;text-align:left;font-size:.85rem;color:#6b7280;font-weight:600">তারিখ</th>
        </tr>
    </thead>
    <tbody>${taskRows}</tbody>
</table>`;

    return `<!DOCTYPE html>
<html lang="bn">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:'Segoe UI',Tahoma,sans-serif">
<div style="max-width:560px;margin:32px auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 4px 16px rgba(0,0,0,.08)">

    <!-- Header -->
    <div style="background:linear-gradient(135deg,#4f46e5,#7c3aed);padding:28px 30px;text-align:center">
        <p style="margin:0;color:rgba(255,255,255,.7);font-size:.85rem">${dateLabel}</p>
        <h1 style="margin:6px 0 0;color:#fff;font-size:1.3rem">${slotLabel}</h1>
        ${officeName ? `<p style="margin:6px 0 0;color:rgba(255,255,255,.85);font-size:.85rem">🏢 ${officeName}</p>` : ''}
    </div>

    <!-- Body -->
    <div style="padding:26px 30px">
        <p style="color:#374151;margin:0 0 16px">${greeting}</p>

        ${taskSection}

        <p style="color:#9ca3af;font-size:.78rem;margin:24px 0 0;text-align:center">
            এই ইমেইল স্বয়ংক্রিয়ভাবে পাঠানো হয়েছে।<br>
            ইমেইল বন্ধ করতে আপনার অ্যাকাউন্ট সেটিংসে যান।
        </p>
    </div>

</div>
</body>
</html>`;
}

// ── 5. Send via EmailJS REST API ──────────────────────────────

async function sendEmail({ toEmail, toName, subject, htmlBody }) {
    const payload = {
        service_id:  EMAILJS_SERVICE_ID,
        template_id: EMAILJS_TEMPLATE_ID,
        user_id:     EMAILJS_PUBLIC_KEY,
        accessToken: EMAILJS_PRIVATE_KEY,
        template_params: {
            to_email:  toEmail,
            to_name:   toName || toEmail,
            subject:   subject,
            html_body: htmlBody,
            // Plain-text fallback for EmailJS templates that use {{message}}
            message:   `আপনার ডাইজেস্ট ইমেইল পাঠানো হয়েছে। HTML সাপোর্ট না থাকলে এই টেক্সট দেখুন।`
        }
    };

    const res = await fetch('https://api.emailjs.com/api/v1.0/email/send', {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify(payload)
    });

    if (!res.ok) {
        const body = await res.text();
        throw new Error(`EmailJS error ${res.status}: ${body}`);
    }
    return true;
}

// ── 6. Main ───────────────────────────────────────────────────

async function run() {
    const slot      = getDigestSlot();
    const today     = getTodayStr();
    const dateLabel = getBengaliDateLabel();

    console.log(`🚀 Email worker started — slot: ${slot}`);
    console.log(`📅 Today (Dhaka): ${today}`);
    console.log(`🕐 UTC time:      ${new Date().toISOString()}\n`);

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

            // Read email subscription
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

            if (!sub.prefs || !sub.prefs[slot]) {
                console.log(`  ⏭️  User opted out of ${slot} digest — skip.`);
                totalSkipped++;
                continue;
            }

            if (!sub.email) {
                console.log(`  ⚠️  No email address saved — skip.`);
                totalSkipped++;
                continue;
            }

            // Fetch profile for officeName
            let officeName = null;
            try {
                const profileRef  = usersRef.doc(uid).collection('data').doc('profile');
                const profileSnap = await profileRef.get();
                if (profileSnap.exists) officeName = profileSnap.data().officeName || null;
            } catch (_) {}

            // Pending tasks
            let tasks = [];
            if (sub.prefs.tasks) {
                tasks = await getPendingTasks(uid, today);
            }

            // Build + send
            const htmlBody = buildEmailHTML({ slot, dateLabel, tasks, officeName });
            const taskCountLabel = tasks.length > 0 ? ` (${tasks.length}টি মুলতবি)` : ' (সব শেষ!)';
            const subject = slot === 'morning'
                ? `🌅 সকালের ডাইজেস্ট${taskCountLabel} — ${dateLabel}`
                : `🌆 সন্ধ্যার ডাইজেস্ট${taskCountLabel} — ${dateLabel}`;

            try {
                await sendEmail({
                    toEmail:  sub.email,
                    toName:   officeName || sub.email,
                    subject:  subject,
                    htmlBody: htmlBody
                });
                console.log(`  📧 Sent to ${sub.email} — OK`);
                totalSent++;
            } catch (err) {
                console.error(`  ❌ Failed for ${sub.email}: ${err.message}`);
                totalFailed++;
            }

            // Small delay to avoid rate limits
            await new Promise(r => setTimeout(r, 300));
        }

    } catch (err) {
        console.error('❌ Fatal error:', err);
        process.exit(1);
    }

    console.log('\n─────────────────────────────────');
    console.log(`✅ Done. Sent: ${totalSent} | Failed: ${totalFailed} | Skipped: ${totalSkipped}`);
    process.exit(0);
}

run();
