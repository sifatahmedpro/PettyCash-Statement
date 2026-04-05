/**
 * ============================================================
 * push-worker/send-push.js
 *
 * Runs inside GitHub Actions every 2 hours (Dhaka business hours).
 * Steps:
 *   1. Connect to Firestore using Firebase Admin SDK
 *   2. Read ALL users
 *   3. For each user, read ALL their device subscriptions
 *   4. For each user, read their pending tasks
 *   5. If they have pending tasks → send Web Push to ALL devices
 *
 * Firestore path: artifacts/default-app-id/users/{uid}/...
 *
 * MULTI-DEVICE: Subscriptions are stored in a sub-collection:
 *   users/{uid}/pushSubscriptions/{endpointHash}
 * This allows phone AND desktop to both receive notifications.
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
    const now   = new Date();
    const dhaka = new Date(now.getTime() + 6 * 60 * 60 * 1000);
    return dhaka.getUTCFullYear() + '-' +
        String(dhaka.getUTCMonth() + 1).padStart(2, '0') + '-' +
        String(dhaka.getUTCDate()).padStart(2, '0');
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

// ── 5. Main function ──────────────────────────────────────────

async function run() {
    console.log('🚀 Push worker started at', new Date().toISOString());
    console.log('📅 Today (Dhaka):', getTodayStr());

    const today = getTodayStr();
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

        console.log(`👥 Found ${usersSnap.size} user(s). Checking subscriptions...\n`);

        for (const userDoc of usersSnap.docs) {
            const uid = userDoc.id;
            console.log(`── Processing user: ${uid}`);

            // ── Step A: Read ALL device subscriptions for this user ──
            const subsRef  = usersRef.doc(uid).collection('pushSubscriptions');
            const subsSnap = await subsRef.get();

            if (subsSnap.empty) {
                console.log(`  ⏭️  No push subscriptions — skipping.`);
                totalSkipped++;
                continue;
            }

            console.log(`  📱 Found ${subsSnap.size} device subscription(s).`);

            // ── Step B: Read pending tasks ──
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
                console.log(`  ✅ No pending tasks — no push needed.`);
                totalSkipped++;
                continue;
            }

            console.log(`  📋 Pending tasks (${pendingTasks.length}):`, pendingTasks.slice(0, 3));

            // ── Step C: Build payload ──
            const count      = pendingTasks.length;
            const firstTitle = pendingTasks[0].slice(0, 50);

            const payload = JSON.stringify({
                title: '⚠️ টাস্ক রিমাইন্ডার',
                body:  `আপনার ${count} টি কাজ বাকি আছে। যেমন: "${firstTitle}"`,
                count: count
            });

            // ── Step D: Send to ALL devices ──
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
                    await webpush.sendNotification(pushSub, payload, { TTL: 86400 });
                    console.log(`  📨 Sent to device ${subDoc.id.slice(0, 12)}... — OK`);
                    totalSent++;

                } catch (pushErr) {
                    console.error(`  ❌ FAILED for device ${subDoc.id.slice(0, 12)}...`);
                    console.error(`     Status: ${pushErr.statusCode} — ${pushErr.body}`);

                    if (pushErr.statusCode === 410 || pushErr.statusCode === 404) {
                        console.warn(`  🗑️  Subscription expired — removing from Firestore.`);
                        await subDoc.ref.delete();
                    } else {
                        totalFailed++;
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
