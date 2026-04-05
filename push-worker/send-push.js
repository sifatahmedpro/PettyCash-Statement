/**
 * ============================================================
 * push-worker/send-push.js
 *
 * Runs inside GitHub Actions every 2 hours (Dhaka business hours).
 * Steps:
 *   1. Connect to Firestore using Firebase Admin SDK
 *   2. Read ALL users who have a saved push subscription
 *   3. For each user, read their pending tasks
 *   4. If they have pending tasks → send a Web Push notification
 *      to their device (phone/desktop) via Web Push protocol
 *
 * Firestore path: artifacts/default-app-id/users/{uid}/...
 *
 * FIXES APPLIED:
 *   - FIX-1: keys.p256dh and keys.auth now read correctly from
 *     Firestore whether stored as nested map OR flat fields.
 *   - FIX-2: Detailed error logging (statusCode + body) so you
 *     can see exactly why a push fails.
 *   - FIX-3: Subscription validity check — skips if keys are missing.
 *   - FIX-4: webpush TTL option added (86400 = 24 hours).
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

// ── 3. Helper: get today's date string (YYYY-MM-DD) in Dhaka time ──

function getTodayStr() {
    // Use Dhaka timezone (UTC+6) so "today" matches the user's local date
    const now = new Date();
    const dhaka = new Date(now.getTime() + 6 * 60 * 60 * 1000);
    return dhaka.getUTCFullYear() + '-' +
        String(dhaka.getUTCMonth() + 1).padStart(2, '0') + '-' +
        String(dhaka.getUTCDate()).padStart(2, '0');
}

// ── 4. FIX-1: Safe key extractor ─────────────────────────────
/**
 * Firestore may store the subscription in two ways:
 *
 *   A) Nested map (standard):
 *      { endpoint, keys: { p256dh, auth } }
 *
 *   B) Flat fields (some older saves):
 *      { endpoint, 'keys.p256dh': '...', 'keys.auth': '...' }
 *
 * This function handles both.
 */
function extractKeys(subscription) {
    // Nested map (normal case)
    if (subscription.keys && subscription.keys.p256dh && subscription.keys.auth) {
        return {
            p256dh: subscription.keys.p256dh,
            auth:   subscription.keys.auth
        };
    }

    // Flat fields fallback
    const p256dh = subscription['keys.p256dh'] || null;
    const auth   = subscription['keys.auth']   || null;

    if (p256dh && auth) {
        return { p256dh, auth };
    }

    return null;  // keys not found at all
}

// ── 5. Main function ──────────────────────────────────────────

async function run() {
    console.log('🚀 Push worker started at', new Date().toISOString());
    console.log('📅 Today (Dhaka):', getTodayStr());

    const today = getTodayStr();
    let totalSent   = 0;
    let totalFailed = 0;
    let totalSkipped = 0;

    try {
        // Firestore path: artifacts > default-app-id > users > {uid}
        const usersRef  = db.collection('artifacts').doc('default-app-id').collection('users');
        const usersSnap = await usersRef.get();

        if (usersSnap.empty) {
            console.log('ℹ️  No users found in Firestore.');
            return;
        }

        console.log(`👥 Found ${usersSnap.size} user(s). Checking subscriptions...\n`);

        for (const userDoc of usersSnap.docs) {
            const uid = userDoc.id;
            console.log(`── Processing user: ${uid}`);

            // ── Step A: Read this user's push subscription ──
            const subRef  = usersRef.doc(uid).collection('data').doc('pushSubscription');
            const subSnap = await subRef.get();

            if (!subSnap.exists) {
                console.log(`  ⏭️  No pushSubscription document — skipping.`);
                totalSkipped++;
                continue;
            }

            const subscription = subSnap.data();

            if (!subscription.endpoint) {
                console.log(`  ⏭️  pushSubscription exists but has no endpoint — skipping.`);
                totalSkipped++;
                continue;
            }

            // FIX-1: Extract keys safely
            const keys = extractKeys(subscription);
            if (!keys) {
                console.warn(`  ⚠️  pushSubscription missing p256dh/auth keys — skipping.`);
                console.warn(`      Raw subscription data:`, JSON.stringify(subscription, null, 2));
                totalSkipped++;
                continue;
            }

            console.log(`  ✅ Subscription found. Endpoint: ${subscription.endpoint.slice(0, 60)}...`);

            // ── Step B: Read this user's pending tasks ──
            const tasksRef  = usersRef.doc(uid).collection('tasks');
            const tasksSnap = await tasksRef.where('status', '!=', 'done').get();

            const pendingTasks = [];
            tasksSnap.forEach(docSnap => {
                const data = docSnap.data();

                // Tasks with no date are always pending
                if (!data.date) {
                    pendingTasks.push(data.title || 'Untitled Task');
                    return;
                }

                // Parse date — handles Firestore Timestamp or string
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
                console.log(`  ✅ No pending tasks — no push needed.`);
                totalSkipped++;
                continue;
            }

            console.log(`  📋 Pending tasks (${pendingTasks.length}):`, pendingTasks.slice(0, 3));

            // ── Step C: Build the push payload ──
            const count      = pendingTasks.length;
            const firstTitle = pendingTasks[0].slice(0, 50);

            const payload = JSON.stringify({
                title: '⚠️ টাস্ক রিমাইন্ডার',
                body:  `আপনার ${count} টি কাজ বাকি আছে। যেমন: "${firstTitle}"`,
                count: count
            });

            // ── Step D: Build the subscription object ──
            const pushSub = {
                endpoint: subscription.endpoint,
                keys: {
                    p256dh: keys.p256dh,
                    auth:   keys.auth
                }
            };

            // ── Step E: Send the Web Push ──
            try {
                await webpush.sendNotification(pushSub, payload, {
                    TTL: 86400   // FIX-4: keep in push service queue for 24 hours
                });
                console.log(`  📨 Push sent successfully — ${count} pending task(s).`);
                totalSent++;

            } catch (pushErr) {
                // FIX-2: Log full error details
                console.error(`  ❌ Push FAILED for user ${uid}`);
                console.error(`     Message    : ${pushErr.message}`);
                console.error(`     Status Code: ${pushErr.statusCode}`);
                console.error(`     Body       : ${pushErr.body}`);
                console.error(`     Endpoint   : ${subscription.endpoint.slice(0, 80)}...`);

                if (pushErr.statusCode === 410 || pushErr.statusCode === 404) {
                    // Subscription is expired or invalid — remove it so we don't retry
                    console.warn(`  🗑️  Subscription expired (${pushErr.statusCode}). Removing from Firestore.`);
                    await subRef.delete();
                } else {
                    totalFailed++;
                }
            }

            console.log(''); // blank line between users
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
