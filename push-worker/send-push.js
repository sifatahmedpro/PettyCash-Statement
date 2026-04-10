/**
 * ============================================================
 * push-worker/send-push.js  —  v3.0 (resting 23:00–06:00 + per-module pushes)
 *
 * ROOT CAUSE FIX (v2.2 → v2.3):
 *   The previous query for module notifications used:
 *     .where('pushed', '!=', true)
 *     .orderBy('pushed')
 *     .orderBy('timestamp', 'asc')
 *   This requires a Firestore composite index that was never
 *   created in the Firebase console. When the index is missing,
 *   Firestore returns an EMPTY result silently in Node.js —
 *   no error is thrown — so module notifications were NEVER sent.
 *
 *   Additionally, documents written by notify.js before this
 *   feature existed have NO 'pushed' field at all. Firestore's
 *   inequality filter (.where('pushed', '!=', true)) excludes
 *   documents where the field is absent, so those old
 *   notifications were also silently skipped.
 *
 * THE FIX:
 *   Replace the fragile composite-index query with a simple
 *   fetch-all-then-filter approach:
 *     1. Fetch the latest 50 notifications ordered by timestamp desc
 *        (single-field index — always exists, no setup needed).
 *     2. Filter in-memory: keep only docs where pushed !== true.
 *        This correctly catches docs with pushed=false, pushed=null,
 *        pushed=undefined, or the field missing entirely.
 *     3. Process oldest-first (reverse) so notifications arrive
 *        in chronological order.
 *
 * WHAT IS UNCHANGED vs v2.2:
 *   • PART 1 — Task reminders: identical logic.
 *   • PART 2 — Module notifications: same mark-pushed-true
 *     behaviour after sending. Only the query strategy changed.
 *   • sendToAllDevices(), extractKeys(), VAPID setup — unchanged.
 *   • Time helpers and resting-hours guard — unchanged.
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
    const now = new Date();
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

// ── 5. Build a deterministic, per-user task-reminder key ─────
//
// FIX (v3.2): The previous implementation encoded Date.now() +
// random bytes, so the key was DIFFERENT on every GitHub Actions
// run. The client's snooze lookup (snoozedMap[statusKey]) would
// never match because the key written to Firestore on a previous
// run is gone — only the new random key exists this run.
//
// Correct approach: use a stable key that is the SAME for every
// run on the same calendar day. The user's device stores the
// snooze against this key; the next run (60 min later, same day)
// produces the identical key and the lookup succeeds.
//
// Key format: "tasks-reminder-{uid}-{YYYY-MM-DD}"
//   • Unique per user — prevents cross-user collisions.
//   • Unique per calendar day — resets at midnight Dhaka so the
//     "already seen today" set is also naturally scoped.
//   • Deterministic within a day — snooze lookups always work.

function buildStatusKey(uid) {
    return `tasks-reminder-${uid}-${getTodayStr()}`;
}

// ── 6. Send one push to all devices for a user ───────────────
/**
 * Sends a single push payload to every registered device for uid.
 * Returns { sent, failed } counts.
 * Cleans up expired subscriptions (410/404) automatically.
 */
async function sendToAllDevices(subsSnap, payload, label) {
    let sent = 0, failed = 0;
    for (const subDoc of subsSnap.docs) {
        const subscription = subDoc.data();
        if (!subscription.endpoint) {
            console.warn(`  ⚠️  [${label}] Device ${subDoc.id}: no endpoint — skipping.`);
            continue;
        }
        const keys = extractKeys(subscription);
        if (!keys) {
            console.warn(`  ⚠️  [${label}] Device ${subDoc.id}: missing keys — skipping.`);
            continue;
        }
        const pushSub = {
            endpoint: subscription.endpoint,
            keys: { p256dh: keys.p256dh, auth: keys.auth }
        };
        try {
            await webpush.sendNotification(pushSub, payload, { TTL: 3600 });
            console.log(`  📨 [${label}] Sent to device ${subDoc.id.slice(0, 12)}... — OK`);
            sent++;
        } catch (pushErr) {
            console.error(`  ❌ [${label}] FAILED for device ${subDoc.id.slice(0, 12)}...`);
            console.error(`     Status: ${pushErr.statusCode} — ${pushErr.body}`);
            if (pushErr.statusCode === 410 || pushErr.statusCode === 404 || pushErr.statusCode === 403) {
                console.warn(`  🗑️  Invalid/expired subscription (${pushErr.statusCode}) — removing.`);
                await subDoc.ref.delete();
            } else {
                failed++;
            }
        }
    }
    return { sent, failed };
}

// ── 7. Main function ──────────────────────────────────────────

async function run() {
    const dhakaHour = getDhakaHour();
    const today     = getTodayStr();

    console.log('🚀 Push worker started at', new Date().toISOString());
    console.log('📅 Today (Dhaka):', today, '| Hour:', dhakaHour);

    // Resting hours: 23:00–06:00 Dhaka — no notifications
    if (dhakaHour < 6 || dhakaHour >= 23) {
        console.log('🌙 Resting hours (23:00–06:00 Dhaka). Exiting.');
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

            // ── Step A: Read device subscriptions ────────────────
            const subsRef  = usersRef.doc(uid).collection('pushSubscriptions');
            const subsSnap = await subsRef.get();

            if (subsSnap.empty) {
                console.log(`  ⏭️  No push subscriptions — skipping user.`);
                totalSkipped++;
                continue;
            }
            console.log(`  📱 ${subsSnap.size} device subscription(s).`);

            // ════════════════════════════════════════════════════
            // PART 1 — TASK REMINDERS
            // Unique key per run, retry every 30 min until user taps.
            // ════════════════════════════════════════════════════

            console.log(`  [TASKS] Checking pending tasks...`);

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
                console.log(`  [TASKS] ✅ No pending tasks.`);
            } else {
                const statusKey   = buildStatusKey(uid);
                const seenRef     = usersRef.doc(uid).collection('pushState').doc('seenToday');
                const seenSnap    = await seenRef.get();
                const seenData    = seenSnap.exists ? seenSnap.data() : {};

                const seenDate    = seenData.date || '';
                const currentSeen = seenDate === today ? (seenData.keys    || []) : [];
                const snoozedMap  = seenDate === today ? (seenData.snoozed || {}) : {};
                const snoozeUntil = snoozedMap[statusKey] || 0;

                if (currentSeen.includes(statusKey)) {
                    console.log(`  [TASKS] 👁️  Already seen today — skipping.`);
                    totalSkipped++;
                } else if (Date.now() < snoozeUntil) {
                    const minsLeft = Math.ceil((snoozeUntil - Date.now()) / 60000);
                    console.log(`  [TASKS] 😴 Snoozed for ${minsLeft} more min — skipping.`);
                    totalSkipped++;
                } else {
                    const count      = pendingTasks.length;
                    const firstTitle = pendingTasks[0].slice(0, 50);

                    const payload = JSON.stringify({
                        title:     '⚠️ টাস্ক রিমাইন্ডার',
                        body:      `আপনার ${count} টি কাজ বাকি আছে। যেমন: "${firstTitle}"`,
                        count:     count,
                        statusKey: statusKey,
                        uid:       uid,
                        tag:       'task-reminder',
                        projectId: process.env.FIREBASE_PROJECT_ID,
                        apiKey:    process.env.FIREBASE_API_KEY || ''
                    });

                    const { sent, failed } = await sendToAllDevices(subsSnap, payload, 'TASKS');
                    totalSent   += sent;
                    totalFailed += failed;
                    if (sent > 0) {
                        console.log(`  [TASKS] 📨 Sent ${sent} push(es) — will retry next run until user taps.`);
                    }
                }
            }

            // ════════════════════════════════════════════════════
            // PART 2 — PER-MODULE NOTIFICATIONS  (v3.0)
            //
            // Each module tag (task-manager, office-issue, petty-cash,
            // advance-payment, manpower, etc.) gets its OWN separate
            // push notification so the user knows exactly which module
            // triggered the alert.
            //
            // Strategy:
            //   1. Fetch latest 50 notifications, filter unpushed in-memory.
            //   2. Group by tag.
            //   3. Send ONE push per group — title = module name, body
            //      lists up to 3 items then "+ N more".
            //   4. Mark all docs in the group as pushed:true.
            // ════════════════════════════════════════════════════

            console.log(`  [NOTIFS] Checking unread/unsent notifications (per-module)...`);

            const notifRef = usersRef.doc(uid).collection('notifications');

            // FIX (Watch Point C): The previous single `.limit(50)` query meant
            // any notification beyond the 50th most recent was permanently skipped
            // — it was never fetched, never filtered, and never marked pushed:true,
            // so it accumulated as undeliverable debt forever.
            //
            // Fix: paginate through ALL notifications (oldest-to-newest within each
            // page) and collect every doc where pushed !== true. This correctly
            // catches docs with pushed=false, pushed=null, pushed=undefined, or
            // the field missing entirely — same as the in-memory filter before.
            //
            // Early-exit optimisation: if a full page arrives with zero unpushed
            // docs, all older pages will also be all-pushed (they were processed
            // in earlier runs), so we can stop paging immediately.
            const PAGE_SIZE = 50;
            let allUnsentDocs = [];
            let lastDoc       = null;

            while (true) {
                let pageQuery = notifRef.orderBy('timestamp', 'desc').limit(PAGE_SIZE);
                if (lastDoc) pageQuery = pageQuery.startAfter(lastDoc);

                const pageSnap  = await pageQuery.get();
                if (pageSnap.empty) break;

                const unsentOnPage = pageSnap.docs.filter(d => d.data().pushed !== true);
                allUnsentDocs = allUnsentDocs.concat(unsentOnPage);

                // If this page had no unpushed docs, all older pages are also fully
                // pushed — no need to continue paginating.
                if (unsentOnPage.length === 0) break;

                if (pageSnap.size < PAGE_SIZE) break;   // last page reached
                lastDoc = pageSnap.docs[pageSnap.docs.length - 1];
            }

            // Sort oldest-first so notifications arrive in chronological order
            const unsentDocs = allUnsentDocs.sort((a, b) =>
                (a.data().timestamp?.toMillis?.() ?? 0) - (b.data().timestamp?.toMillis?.() ?? 0)
            );

            if (unsentDocs.length === 0) {
                console.log(`  [NOTIFS] ✅ No unsent notifications.`);
            } else {
                console.log(`  [NOTIFS] 📋 ${unsentDocs.length} unsent notification(s). Grouping by module...`);

                // Group by tag
                const groups = {};
                for (const notifDoc of unsentDocs) {
                    const data = notifDoc.data();
                    const tag  = data.tag || 'general';
                    if (!groups[tag]) groups[tag] = [];
                    groups[tag].push(notifDoc);
                }

                const freshSubsSnap = await subsRef.get();
                if (freshSubsSnap.empty) {
                    console.log(`  [NOTIFS] ⏭️  No device subscriptions left — skipping notifications.`);
                } else {
                    for (const [tag, docs] of Object.entries(groups)) {
                        const count    = docs.length;
                        const latest   = docs[docs.length - 1].data(); // most recent in group
                        const title    = latest.title || '🔔 নতুন নোটিফিকেশন';

                        // Build body: list up to 3 messages, then "+ N more"
                        const preview = docs.slice(-3).map(d => d.data().message || '').filter(Boolean);
                        let body;
                        if (count === 1) {
                            body = preview[0] || '';
                        } else {
                            const shown = preview.slice(0, 3);
                            const extra = count - shown.length;
                            body = shown.join(' • ');
                            if (extra > 0) body += ` (+${extra} আরও)`;
                        }

                        console.log(`  [NOTIFS] → Module [${tag}]: ${count} notification(s)`);

                        const payload = JSON.stringify({
                            title,
                            body,
                            tag:       `notif-${tag}`,
                            notifId:   docs[docs.length - 1].id,
                            uid,
                            projectId: process.env.FIREBASE_PROJECT_ID,
                            apiKey:    process.env.FIREBASE_API_KEY || ''
                        });

                        const { sent, failed } = await sendToAllDevices(
                            freshSubsSnap, payload, `NOTIF:${tag}`
                        );
                        totalSent   += sent;
                        totalFailed += failed;

                        // BUG FIX (v3.1): Only mark pushed:true when at least one device
                        // actually received the push. Previously, marking was unconditional —
                        // if all subscriptions failed (missing keys, expired, etc.) the
                        // notifications were permanently marked pushed:true and never retried.
                        if (sent > 0) {
                            const batch = admin.firestore().batch();
                            for (const notifDoc of docs) {
                                batch.update(notifDoc.ref, {
                                    pushed:     true,
                                    pushSentAt: admin.firestore.FieldValue.serverTimestamp()
                                });
                            }
                            await batch.commit();
                            console.log(`  [NOTIFS] ✔ [${tag}] Pushed & marked ${count} doc(s).`);
                        } else {
                            console.log(`  [NOTIFS] ⚠️  [${tag}] 0 devices received push — NOT marking pushed:true, will retry next run.`);
                        }
                    }
                }
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
