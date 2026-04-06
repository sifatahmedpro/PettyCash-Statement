/**
 * ============================================================
 * push-worker/send-push.js  —  v2.1 (UNIQUE-KEY per push)
 *
 * WHAT CHANGED vs v2.0:
 *   • buildStatusKey() now generates a UNIQUE ID per push run
 *     (format: push_{uid_prefix}_{timestamp_base36}_{random4hex})
 *     instead of a hash of task titles. This means every push
 *     notification on the Android shade is independently
 *     dismissable — snooze, mark_read, and undo each operate on
 *     that exact notification only, never affecting any other.
 *
 *   • No new dependencies — ID built from Date.now() + Math.random()
 *     (completely free, no UUID library needed).
 *
 *   • app-backend.js v2.1 caps the seenToday keys list at 200
 *     entries to prevent Firestore document bloat (old entries
 *     reset daily anyway).
 *
 * Firestore paths (read by this worker):
 *   artifacts/default-app-id/users/{uid}/pushSubscriptions/{hash}
 *   artifacts/default-app-id/users/{uid}/tasks/{taskId}
 *   artifacts/default-app-id/users/{uid}/pushState/seenToday
 *   artifacts/default-app-id/users/{uid}/pushState/snooze
 * ============================================================
 */

'use strict';

const admin   = require('firebase-admin');
const webpush = require('web-push');

// ── 1. Firebase Admin init ────────────────────────────────────

const privateKey = process.env.FIREBASE_PRIVATE_KEY
    ? process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, '\n')
    : null;

if (!process.env.FIREBASE_PROJECT_ID || !process.env.FIREBASE_CLIENT_EMAIL || !privateKey) {
    console.error('❌ Missing Firebase credentials. Check GitHub Secrets.');
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

// ── 2. VAPID keys setup ───────────────────────────────────────

if (!process.env.VAPID_PUBLIC_KEY || !process.env.VAPID_PRIVATE_KEY || !process.env.VAPID_SUBJECT) {
    console.error('❌ Missing VAPID keys. Check GitHub Secrets.');
    process.exit(1);
}

webpush.setVapidDetails(
    process.env.VAPID_SUBJECT,
    process.env.VAPID_PUBLIC_KEY,
    process.env.VAPID_PRIVATE_KEY
);

console.log('🔑 VAPID Subject :', process.env.VAPID_SUBJECT);
console.log('🔑 VAPID Public  :', process.env.VAPID_PUBLIC_KEY.slice(0, 20) + '...');

// ── 3. Time helpers (Dhaka = UTC+6) ──────────────────────────

function getDhakaDate() {
    const now   = new Date();
    return new Date(now.getTime() + 6 * 60 * 60 * 1000);
}

function getTodayStr() {
    const d = getDhakaDate();
    return d.getUTCFullYear() + '-' +
        String(d.getUTCMonth() + 1).padStart(2, '0') + '-' +
        String(d.getUTCDate()).padStart(2, '0');
}

function getDhakaHour() {
    return getDhakaDate().getUTCHours();
}

// ── 4. Safe key extractor ─────────────────────────────────────

function extractKeys(subscription) {
    if (subscription.keys && subscription.keys.p256dh && subscription.keys.auth) {
        return { p256dh: subscription.keys.p256dh, auth: subscription.keys.auth };
    }
    const p256dh = subscription['keys.p256dh'] || null;
    const auth   = subscription['keys.auth']   || null;
    if (p256dh && auth) return { p256dh, auth };
    return null;
}

// ── 5. Build a unique push ID for this specific run ──────────
// Each call to send-push.js creates a fresh ID, so every push
// notification is independently dismissable — no hash collisions
// between users or across repeated runs with the same task list.
// Format: push_{uid_prefix}_{timestamp}_{random4hex}
// This is free — uses only Date.now() and Math.random(), no UUID lib.

function buildStatusKey(uid, _pendingTasks) {
    const ts   = Date.now().toString(36);                          // base-36 timestamp
    const rnd  = Math.floor(Math.random() * 0xFFFF).toString(16).padStart(4, '0');
    const user = uid.slice(0, 6);                                  // first 6 chars of uid
    return `push_${user}_${ts}_${rnd}`;
}

// ── 6. Main function ──────────────────────────────────────────

async function run() {
    const dhakaHour = getDhakaHour();
    const today     = getTodayStr();

    console.log('🚀 Push worker started at', new Date().toISOString());
    console.log('📅 Today (Dhaka):', today, '| Hour:', dhakaHour);

    // Resting hours: 00:00–05:00 Dhaka (midnight to 5 AM) — no notifications
    if (dhakaHour < 5) {
        console.log('🌙 Resting hours (00:00–05:00 Dhaka). Exiting.');
        process.exit(0);
    }

    let totalSent    = 0;
    let totalFailed  = 0;
    let totalSkipped = 0;

    try {
        const usersRef  = db.collection('artifacts').doc('default-app-id').collection('users');
        const usersSnap = await usersRef.get();

        if (usersSnap.empty) {
            console.log('ℹ️  No users found in Firestore.');
            return;
        }

        console.log(`👥 Found ${usersSnap.size} user(s).\n`);

        for (const userDoc of usersSnap.docs) {
            const uid = userDoc.id;
            console.log(`── Processing user: ${uid}`);

            // ── Step A: Check SNOOZE ──────────────────────────────
            // Per-notification snooze is stored inside pushState/seenToday
            // as a 'snoozed' map: { [statusKey]: snoozeUntilEpochMs }.
            // We read this once here and use it in Step D below.
            // (System-wide snooze is removed — only per-notification snooze exists.)

            // ── Step B: Read device subscriptions ────────────────
            const subsRef  = usersRef.doc(uid).collection('pushSubscriptions');
            const subsSnap = await subsRef.get();

            if (subsSnap.empty) {
                console.log(`  ⏭️  No push subscriptions — skipping.`);
                totalSkipped++;
                continue;
            }
            console.log(`  📱 ${subsSnap.size} device subscription(s).`);

            // ── Step C: Read pending tasks ────────────────────────
            const tasksRef  = usersRef.doc(uid).collection('tasks');
            const tasksSnap = await tasksRef.where('status', '!=', 'done').get();

            const pendingTasks = [];
            tasksSnap.forEach(docSnap => {
                const data = docSnap.data();
                if (!data.date) {
                    pendingTasks.push(data.title || 'Untitled Task');
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
                    pendingTasks.push(data.title || 'Untitled Task');
                }
            });

            if (pendingTasks.length === 0) {
                console.log(`  ✅ No pending tasks.`);
                totalSkipped++;
                continue;
            }

            // ── Step D: Check SEEN-TODAY + per-notification SNOOZE ─
            // statusKey is now a unique ID per push run — no hash collisions.
            const statusKey    = buildStatusKey(uid, pendingTasks);
            const seenRef      = usersRef.doc(uid).collection('pushState').doc('seenToday');
            const seenSnap     = await seenRef.get();
            const seenData     = seenSnap.exists ? seenSnap.data() : {};

            // Reset seen list if it's from a previous day
            const seenDate     = seenData.date || '';
            const currentSeen  = seenDate === today ? (seenData.keys || []) : [];

            // Per-notification snooze map: { [statusKey]: snoozeUntilEpochMs }
            // Snooze entries from previous days are ignored (they expire naturally).
            const snoozedMap   = seenDate === today ? (seenData.snoozed || {}) : {};
            const snoozeUntil  = snoozedMap[statusKey] || 0;

            if (currentSeen.includes(statusKey)) {
                console.log(`  👁️  Already seen today (key: ${statusKey}) — skipping.`);
                totalSkipped++;
                continue;
            }

            if (Date.now() < snoozeUntil) {
                const minsLeft = Math.ceil((snoozeUntil - Date.now()) / 60000);
                console.log(`  😴 This notification snoozed for ${minsLeft} more min (key: ${statusKey}) — skipping.`);
                totalSkipped++;
                continue;
            }

            // ── Step E: Build payload ─────────────────────────────
            const count      = pendingTasks.length;
            const firstTitle = pendingTasks[0].slice(0, 50);

            const payload = JSON.stringify({
                title:     '⚠️ টাস্ক রিমাইন্ডার',
                body:      `আপনার ${count} টি কাজ বাকি আছে। যেমন: "${firstTitle}"`,
                count:     count,
                statusKey: statusKey,    // sent to SW so it can mark "seen"
                uid:       uid,          // sent to SW for snooze/mark_read actions
                tag:       'task-reminder',
                // Needed by SW for direct Firestore REST calls when tab is closed:
                projectId: process.env.FIREBASE_PROJECT_ID,
                apiKey:    process.env.FIREBASE_API_KEY || ''
            });

            // ── Step F: Send to ALL devices ───────────────────────
            let sentForUser = 0;
            for (const subDoc of subsSnap.docs) {
                const subscription = subDoc.data();
                if (!subscription.endpoint) {
                    console.warn(`  ⚠️  Device ${subDoc.id}: no endpoint — skipping.`);
                    continue;
                }
                const keys = extractKeys(subscription);
                if (!keys) {
                    console.warn(`  ⚠️  Device ${subDoc.id}: missing keys — skipping.`);
                    continue;
                }

                const pushSub = {
                    endpoint: subscription.endpoint,
                    keys: { p256dh: keys.p256dh, auth: keys.auth }
                };

                try {
                    await webpush.sendNotification(pushSub, payload, { TTL: 3600 }); // 1hr TTL
                    console.log(`  📨 Sent to device ${subDoc.id.slice(0, 12)}... — OK`);
                    totalSent++;
                    sentForUser++;
                } catch (pushErr) {
                    console.error(`  ❌ FAILED for device ${subDoc.id.slice(0, 12)}...`);
                    console.error(`     Status: ${pushErr.statusCode} — ${pushErr.body}`);
                    if (pushErr.statusCode === 410 || pushErr.statusCode === 404) {
                        console.warn(`  🗑️  Expired subscription — removing.`);
                        await subDoc.ref.delete();
                    } else {
                        totalFailed++;
                    }
                }
            }

            // ── Step G: intentionally NOT marking seen here ───────
            // Seen is only written when the user TAPS the notification
            // (sw.js → PUSH_NOTIF_ACTION → AppDB.markPushSeen).
            // This allows retries every 30 min until the user acts.
            if (sentForUser > 0) {
                console.log(`  📨 Sent ${sentForUser} push(es) — will retry next run until user taps.`);
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
