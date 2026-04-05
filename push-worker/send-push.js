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
 * This file never needs to change when you add new HTML pages.
 * ============================================================
 */

'use strict';

const admin   = require('firebase-admin');
const webpush = require('web-push');

// ── 1. Firebase Admin init ────────────────────────────────────
// Reads credentials from GitHub Actions secrets (environment variables)

const privateKey = process.env.FIREBASE_PRIVATE_KEY
    ? process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, '\n')   // GitHub escapes newlines
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
// VAPID keys identify your server to browsers (security requirement)

if (!process.env.VAPID_PUBLIC_KEY || !process.env.VAPID_PRIVATE_KEY || !process.env.VAPID_SUBJECT) {
    console.error('❌ Missing VAPID keys. Check GitHub Secrets.');
    process.exit(1);
}

webpush.setVapidDetails(
    process.env.VAPID_SUBJECT,     // e.g. mailto:you@gmail.com
    process.env.VAPID_PUBLIC_KEY,
    process.env.VAPID_PRIVATE_KEY
);

// ── 3. Helper: get today's date string (YYYY-MM-DD) ──────────

function getTodayStr() {
    const d = new Date();
    return d.getFullYear() + '-' +
        String(d.getMonth() + 1).padStart(2, '0') + '-' +
        String(d.getDate()).padStart(2, '0');
}

// ── 4. Main function ──────────────────────────────────────────

async function run() {
    console.log('🚀 Push worker started at', new Date().toISOString());

    const today = getTodayStr();
    let totalSent = 0;
    let totalFailed = 0;

    try {
        // ── Step A: Find all users who have a push subscription saved ──
        // Firestore path: artifacts/default/users/{uid}/data/pushSubscription
        // We query by checking for documents that have an 'endpoint' field.

        const usersRef = db.collection('artifacts').doc('default').collection('users');
        const usersSnap = await usersRef.get();

        if (usersSnap.empty) {
            console.log('ℹ️  No users found in Firestore.');
            return;
        }

        console.log(`👥 Found ${usersSnap.size} user(s). Checking subscriptions...`);

        for (const userDoc of usersSnap.docs) {
            const uid = userDoc.id;

            // ── Step B: Read this user's push subscription ──
            const subRef  = usersRef.doc(uid).collection('data').doc('pushSubscription');
            const subSnap = await subRef.get();

            if (!subSnap.exists || !subSnap.data().endpoint) {
                console.log(`  ⏭️  User ${uid} — no push subscription, skipping.`);
                continue;
            }

            const subscription = subSnap.data();

            // ── Step C: Read this user's pending tasks ──
            const tasksRef  = usersRef.doc(uid).collection('tasks');
            const tasksSnap = await tasksRef.where('status', '!=', 'done').get();

            const pendingTasks = [];
            tasksSnap.forEach(doc => {
                const data = doc.data();
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
                console.log(`  ✅ User ${uid} — no pending tasks, no push needed.`);
                continue;
            }

            // ── Step D: Build the push payload ──
            const count      = pendingTasks.length;
            const firstTitle = pendingTasks[0].slice(0, 50);

            const payload = JSON.stringify({
                title: '⚠️ টাস্ক রিমাইন্ডার',
                body:  `আপনার ${count} টি কাজ বাকি আছে। যেমন: "${firstTitle}"...`,
                count: count
            });

            // ── Step E: Send the Web Push ──
            // Build a proper PushSubscription object for web-push library
            const pushSub = {
                endpoint: subscription.endpoint,
                keys: {
                    p256dh: subscription.keys?.p256dh,
                    auth:   subscription.keys?.auth
                }
            };

            try {
                await webpush.sendNotification(pushSub, payload);
                console.log(`  📨 Sent push to user ${uid} — ${count} pending task(s).`);
                totalSent++;
            } catch (pushErr) {
                // If subscription is expired/invalid, remove it from Firestore
                if (pushErr.statusCode === 410 || pushErr.statusCode === 404) {
                    console.warn(`  ⚠️  Subscription expired for user ${uid}. Removing from Firestore.`);
                    await subRef.delete();
                } else {
                    console.error(`  ❌ Push failed for user ${uid}:`, pushErr.message);
                    totalFailed++;
                }
            }
        }

    } catch (err) {
        console.error('❌ Fatal error:', err);
        process.exit(1);
    }

    console.log(`\n✅ Done. Sent: ${totalSent}, Failed: ${totalFailed}`);
    process.exit(0);
}

run();
