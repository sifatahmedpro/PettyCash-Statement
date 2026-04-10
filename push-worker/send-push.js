/**
 * ============================================================
 * push-worker/send-push.js  —  v4.0
 *
 * ── WHAT WAS BROKEN & WHY ────────────────────────────────────
 *
 * BUG 1 — Module notifications only fired ONCE, then stopped forever.
 *   Root cause: after the first successful delivery, every notification
 *   doc was permanently marked pushed:true. Every subsequent GitHub
 *   Actions run found zero unpushed docs and exited with "No unsent
 *   notifications." Even a manual trigger produced nothing new because
 *   ALL existing docs were already pushed:true. The only way to ever
 *   get another module notification was to take a new action on a page
 *   (which writes a fresh doc with pushed:false). But even then, once
 *   that single new doc was delivered it disappeared forever.
 *
 * BUG 2 — Module notifications had no "hourly repeat" mechanism.
 *   Task reminders repeat every hour (until the user taps) because they
 *   use a per-day statusKey stored in pushState/seenToday. If the key
 *   isn't in seenToday, the push fires. Module notifications had no
 *   equivalent — there was no concept of "send again next hour if the
 *   user hasn't acted yet."
 *
 * BUG 3 — No cleanup. pushed:true docs accumulated forever, growing
 *   the notifications collection indefinitely.
 *
 * ── THE FIX (v4.0) ───────────────────────────────────────────
 *
 * Module notifications now use the SAME daily seenToday pattern as
 * task reminders:
 *
 *   • A per-tag statusKey is built:  "notif-{tag}-{uid}-{YYYY-MM-DD}"
 *   • Before sending, check pushState/seenToday for that key.
 *   • If already seen today → skip (user already received it today).
 *   • If NOT seen → send the push, then ADD the key to seenToday
 *     (so the same tag doesn't fire again this hour/run).
 *   • The seenToday document resets every calendar day automatically
 *     (same mechanism as tasks — the date field is checked).
 *   • pushed:true on individual notification docs is NO LONGER USED
 *     for gating delivery. It is still written for audit purposes
 *     but NOT read as a gate.
 *
 * Cleanup: notification docs older than CLEANUP_DAYS (7 days) are
 *   deleted at the end of each run to keep the collection lean.
 *
 * ── WHAT IS UNCHANGED ────────────────────────────────────────
 *   • PART 1 — Task reminders: identical logic, same statusKey format.
 *   • sendToAllDevices(), extractKeys(), VAPID setup — unchanged.
 *   • Resting hours guard: 00:00–06:00 Dhaka = silent.
 *   • Cron schedule in push-notify.yml — unchanged.
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

// ── 5. Build deterministic status keys ───────────────────────
//
// TASK REMINDER key: same for every run on the same calendar day.
//   Format: "tasks-reminder-{uid}-{YYYY-MM-DD}"
//   The seenToday check prevents re-sending after user taps.
//
// MODULE NOTIFICATION key: per-tag, per-day.
//   Format: "notif-{tag}-{uid}-{YYYY-MM-DD}"
//   Allows one push per tag per day regardless of how many
//   individual notification docs the tag has accumulated.
//   Resets at midnight Dhaka so each new day gets fresh pushes.

function buildTaskStatusKey(uid) {
    return `tasks-reminder-${uid}-${getTodayStr()}`;
}

function buildNotifStatusKey(uid, tag) {
    return `notif-${tag}-${uid}-${getTodayStr()}`;
}

// ── 6. Send one push to all devices for a user ───────────────
/**
 * Sends a single push payload to every registered device for uid.
 * Returns { sent, failed } counts.
 * Cleans up expired subscriptions (410/404/403) automatically.
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

// ── 7. Read seenToday document for a user ────────────────────
/**
 * Returns { currentSeen: string[], snoozedMap: object }
 * Both are scoped to today — stale data from previous days is discarded.
 */
async function readSeenToday(usersRef, uid, today) {
    const seenRef  = usersRef.doc(uid).collection('pushState').doc('seenToday');
    const seenSnap = await seenRef.get();
    const seenData = seenSnap.exists ? seenSnap.data() : {};
    const seenDate = seenData.date || '';
    return {
        seenRef,
        currentSeen: seenDate === today ? (seenData.keys    || []) : [],
        snoozedMap:  seenDate === today ? (seenData.snoozed || {}) : {}
    };
}

// ── 8. Mark a statusKey as seen today ────────────────────────
/**
 * Adds statusKey to the seenToday document.
 * Re-reads the doc to avoid overwriting concurrent writes.
 */
async function markSeenToday(seenRef, statusKey, today) {
    const snap = await seenRef.get();
    const data = snap.exists ? snap.data() : {};
    const existingDate = data.date || '';
    const keys    = existingDate === today ? (data.keys    || []) : [];
    const snoozed = existingDate === today ? (data.snoozed || {}) : {};
    if (!keys.includes(statusKey)) {
        const trimmed = keys.length >= 200 ? keys.slice(-199) : keys;
        await seenRef.set({ date: today, keys: [...trimmed, statusKey], snoozed });
    }
}

// ── 9. Main function ──────────────────────────────────────────

// How many days back to keep notification docs. Older docs are deleted.
const CLEANUP_DAYS = 7;

async function run() {
    const dhakaHour = getDhakaHour();
    const today     = getTodayStr();

    console.log('🚀 Push worker started at', new Date().toISOString());
    console.log('📅 Today (Dhaka):', today, '| Hour:', dhakaHour);

    // ── Resting hours: 00:00–06:00 Dhaka — no notifications ──
    // Silence from midnight (12 AM) to 6 AM Dhaka.
    // Active hours: 06:00–23:59 Dhaka.
    if (dhakaHour < 6) {
        console.log('🌙 Resting hours (00:00–06:00 Dhaka). Exiting silently.');
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

            // ── Step B: Read seenToday once, reuse across PART 1 & 2 ──
            const { seenRef, currentSeen, snoozedMap } = await readSeenToday(usersRef, uid, today);

            // ════════════════════════════════════════════════════
            // PART 1 — TASK REMINDERS
            // Fires every run until user taps "Seen" or snoozes.
            // Uses deterministic daily statusKey so snooze works.
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
                const statusKey   = buildTaskStatusKey(uid);
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
            // PART 2 — PER-MODULE NOTIFICATIONS  (v4.0 rewrite)
            //
            // FIX: The old system marked each notification doc
            // pushed:true permanently — so after the first delivery
            // they NEVER fired again.
            //
            // New approach:
            //   1. Fetch ALL notification docs (not filtered by pushed).
            //   2. Group by tag.
            //   3. For each tag, build a daily statusKey.
            //   4. Check seenToday — if the tag key is there, skip.
            //      If not, send the push for that tag.
            //   5. After sending, add the tag key to seenToday.
            //      This prevents the same tag firing again in the
            //      same day (resets at midnight Dhaka).
            //   6. Mark the individual docs pushed:true for audit,
            //      but this NO LONGER gates delivery.
            //
            // Effect: each module tag fires at most once per day,
            //   every day that new notifications exist for it.
            //   The user gets fresh module pushes each day as long
            //   as activity is happening on those pages.
            // ════════════════════════════════════════════════════

            console.log(`  [NOTIFS] Checking module notifications (per-tag daily push)...`);

            const notifRef = usersRef.doc(uid).collection('notifications');

            // Paginate through ALL notification docs.
            // We no longer filter by pushed:true/false — ALL docs are
            // considered so we can group them by tag and apply the
            // daily seenToday gate instead.
            const PAGE_SIZE = 50;
            let allNotifDocs = [];
            let lastDoc      = null;

            while (true) {
                let pageQuery = notifRef.orderBy('timestamp', 'desc').limit(PAGE_SIZE);
                if (lastDoc) pageQuery = pageQuery.startAfter(lastDoc);

                const pageSnap = await pageQuery.get();
                if (pageSnap.empty) break;

                allNotifDocs = allNotifDocs.concat(pageSnap.docs);

                if (pageSnap.size < PAGE_SIZE) break;
                lastDoc = pageSnap.docs[pageSnap.docs.length - 1];
            }

            if (allNotifDocs.length === 0) {
                console.log(`  [NOTIFS] ✅ No notifications found.`);
            } else {
                console.log(`  [NOTIFS] 📋 ${allNotifDocs.length} total notification(s). Grouping by module tag...`);

                // Group ALL docs by tag (regardless of pushed status)
                const groups = {};
                for (const notifDoc of allNotifDocs) {
                    const data = notifDoc.data();
                    const tag  = data.tag || 'general';
                    if (!groups[tag]) groups[tag] = [];
                    groups[tag].push(notifDoc);
                }

                // Re-read fresh subs before sending
                const freshSubsSnap = await subsRef.get();
                if (freshSubsSnap.empty) {
                    console.log(`  [NOTIFS] ⏭️  No device subscriptions left — skipping notifications.`);
                } else {
                    for (const [tag, docs] of Object.entries(groups)) {
                        // Build the per-tag daily statusKey
                        const notifStatusKey = buildNotifStatusKey(uid, tag);
                        const snoozeUntil    = snoozedMap[notifStatusKey] || 0;

                        if (currentSeen.includes(notifStatusKey)) {
                            console.log(`  [NOTIFS] 👁️  [${tag}] Already sent today — skipping.`);
                            totalSkipped++;
                            continue;
                        }

                        if (Date.now() < snoozeUntil) {
                            const minsLeft = Math.ceil((snoozeUntil - Date.now()) / 60000);
                            console.log(`  [NOTIFS] 😴 [${tag}] Snoozed for ${minsLeft} more min — skipping.`);
                            totalSkipped++;
                            continue;
                        }

                        // Sort docs oldest-first for the preview
                        const sortedDocs = docs.slice().sort((a, b) =>
                            (a.data().timestamp?.toMillis?.() ?? 0) - (b.data().timestamp?.toMillis?.() ?? 0)
                        );

                        const count  = sortedDocs.length;
                        const latest = sortedDocs[sortedDocs.length - 1].data();
                        const title  = latest.title || '🔔 নতুন নোটিফিকেশন';

                        // Build body: show up to 3 recent messages
                        const preview = sortedDocs.slice(-3).map(d => d.data().message || '').filter(Boolean);
                        let body;
                        if (count === 1) {
                            body = preview[0] || '';
                        } else {
                            const shown = preview.slice(0, 3);
                            const extra = count - shown.length;
                            body = shown.join(' • ');
                            if (extra > 0) body += ` (+${extra} আরও)`;
                        }

                        console.log(`  [NOTIFS] → Module [${tag}]: ${count} doc(s) — sending push...`);

                        const payload = JSON.stringify({
                            title,
                            body,
                            tag:       `notif-${tag}`,
                            notifId:   sortedDocs[sortedDocs.length - 1].id,
                            uid,
                            projectId: process.env.FIREBASE_PROJECT_ID,
                            apiKey:    process.env.FIREBASE_API_KEY || ''
                        });

                        const { sent, failed } = await sendToAllDevices(
                            freshSubsSnap, payload, `NOTIF:${tag}`
                        );
                        totalSent   += sent;
                        totalFailed += failed;

                        if (sent > 0) {
                            // Mark this tag as "sent today" in seenToday so the
                            // same module doesn't fire again until tomorrow.
                            await markSeenToday(seenRef, notifStatusKey, today);
                            console.log(`  [NOTIFS] ✔ [${tag}] Sent ${sent} push(es). Marked seen today — resets midnight Dhaka.`);

                            // Also mark individual docs pushed:true for audit trail.
                            // NOTE: this is audit-only — v4.0 no longer reads pushed:true
                            // as a delivery gate.
                            const batch = db.batch();
                            for (const notifDoc of sortedDocs) {
                                if (notifDoc.data().pushed !== true) {
                                    batch.update(notifDoc.ref, {
                                        pushed:     true,
                                        pushSentAt: admin.firestore.FieldValue.serverTimestamp()
                                    });
                                }
                            }
                            await batch.commit();
                        } else {
                            console.log(`  [NOTIFS] ⚠️  [${tag}] 0 devices received push — will retry next run.`);
                        }
                    }
                }
            }

            // ════════════════════════════════════════════════════
            // PART 3 — CLEANUP OLD NOTIFICATION DOCS
            //
            // Delete notification docs older than CLEANUP_DAYS (7 days)
            // to prevent unbounded collection growth.
            // ════════════════════════════════════════════════════

            const cutoffDate = new Date();
            cutoffDate.setDate(cutoffDate.getDate() - CLEANUP_DAYS);
            const cutoffTimestamp = admin.firestore.Timestamp.fromDate(cutoffDate);

            const oldNotifSnap = await notifRef
                .where('timestamp', '<', cutoffTimestamp)
                .limit(200)
                .get();

            if (!oldNotifSnap.empty) {
                const cleanBatch = db.batch();
                oldNotifSnap.docs.forEach(d => cleanBatch.delete(d.ref));
                await cleanBatch.commit();
                console.log(`  [CLEANUP] 🗑️  Deleted ${oldNotifSnap.size} notification doc(s) older than ${CLEANUP_DAYS} days.`);
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
