/**
 * ============================================================
 * push-worker/send-push.js  —  v5.0
 * Standalone Push Notification Worker for GitHub Actions
 * Project : অফিস ম্যানেজমেন্ট সিস্টেম
 *
 * WHAT CHANGED in v5.0:
 *
 *   BUG FIXES from v4.0:
 *     [FIX-6] CRITICAL — Manual trigger only sent the nearest
 *             earlier slot (e.g. triggering at 11 AM only fired
 *             the 10:00 slot). Manual triggers now run ALL 9
 *             scheduled slots so every module fires in one run.
 *             To test a single specific slot use force_send=true
 *             instead (which keeps the nearest-earlier-slot logic).
 *
 *     [FIX-7] CRITICAL — seen-today deduplication silently
 *             suppressed all modules after the first manual test
 *             of each day. A module marked "sent" at e.g. 11 AM
 *             would never fire again that calendar day — making
 *             it appear broken. Fixed by:
 *             (a) Auto-enabling RESET_SEEN_TODAY for every
 *                 manual trigger and every force_send run.
 *             (b) Adding a resetSeenTodayForAllUsers() helper
 *                 that deletes the seenToday Firestore document
 *                 for all users before the worker processes any
 *                 slot, so all modules are always eligible.
 *             (c) Exposing a reset_seen_today workflow_dispatch
 *                 input in push-notify.yml so operators can
 *                 reset state from the Actions tab without
 *                 touching Firestore directly.
 *
 *   UNCHANGED from v4.0:
 *     All v4.0 fixes (FIX-1 through FIX-5) are preserved.
 *     Cron schedule, HOUR_SCHEDULE table, payload builders,
 *     retry logic, and subscription cleanup are unchanged.
 *
 * ENVIRONMENT VARIABLES (from GitHub Secrets):
 *   FIREBASE_PROJECT_ID       (required)
 *   FIREBASE_CLIENT_EMAIL     (required)
 *   FIREBASE_PRIVATE_KEY      (required)
 *   FIREBASE_API_KEY          (required)
 *   VAPID_PUBLIC_KEY          (required)
 *   VAPID_PRIVATE_KEY         (required)
 *   VAPID_SUBJECT             (optional)
 *   FORCE_SEND                (optional, workflow_dispatch only)
 *   DRY_RUN                   (optional, workflow_dispatch only)
 *   RESET_SEEN_TODAY          (optional — auto-set for manual/force runs)
 * ============================================================
 */

'use strict';

const admin   = require('firebase-admin');
const webpush = require('web-push');
const fs      = require('fs');
const path    = require('path');

// ══════════════════════════════════════════════════════════════
// CONFIGURATION
// ══════════════════════════════════════════════════════════════

const CONFIG = {
    PROJECT_ID:     process.env.FIREBASE_PROJECT_ID    || '',
    CLIENT_EMAIL:   process.env.FIREBASE_CLIENT_EMAIL  || '',
    PRIVATE_KEY:   (process.env.FIREBASE_PRIVATE_KEY   || '').replace(/\\n/g, '\n'),
    API_KEY:        process.env.FIREBASE_API_KEY        || '',

    VAPID_PUBLIC_KEY:  process.env.VAPID_PUBLIC_KEY  || '',
    VAPID_PRIVATE_KEY: process.env.VAPID_PRIVATE_KEY || '',
    VAPID_SUBJECT:     process.env.VAPID_SUBJECT     || 'mailto:support@officemanagement.app',

    RUN_ID:           process.env.RUN_ID               || Math.random().toString(36).slice(2, 8),
    RUN_NUMBER:       process.env.WORKFLOW_RUN_NUMBER  || 'unknown',
    GITHUB_ACTOR:     process.env.GITHUB_ACTOR         || 'system',
    IS_MANUAL_TRIGGER: process.env.IS_MANUAL_TRIGGER   === 'true',

    FORCE_SEND:        process.env.FORCE_SEND        === 'true',
    DRY_RUN:           process.env.DRY_RUN           === 'true',
    // When true: wipe the seen-today deduplication state before running.
    // Set automatically for manual triggers and force_send runs so that
    // testing never silently skips modules that were already sent today.
    RESET_SEEN_TODAY:  process.env.RESET_SEEN_TODAY  === 'true',

    // FIX-5: was 'Asia/Kolkata' (UTC+5:30) — Dhaka is UTC+6
    TIMEZONE: 'Asia/Dhaka',

    // Active window: 06:00–23:00 Dhaka
    ACTIVE_HOUR_START:   6,
    ACTIVE_HOUR_END:    23,

    MAX_RETRY_ATTEMPTS: 3,
    RETRY_DELAY_MS:     1000,
    BATCH_SIZE:         10,
};

// ══════════════════════════════════════════════════════════════
// SCHEDULE TABLE
// Maps Dhaka hour → array of module descriptors to notify about.
// Each descriptor tells the worker which Firestore collection(s)
// to inspect and what push copy to show.
//
// 'query' is optional. When absent the push is a simple reminder
// with no live record count (e.g. various-calculators).
// When present it must be a function(db, uid) → Promise<number>
// that returns a record count. The push body shows that count.
// ══════════════════════════════════════════════════════════════

const HOUR_SCHEDULE = {
    6: [
        {
            tag:        'advance-payment',
            icon:       '💰',
            title:      'অগ্রিম পরিশোধ',
            body:       'অগ্রিম পরিশোধ রেকর্ড পর্যালোচনা করুন।',
            countLabel: 'টি সক্রিয় রেকর্ড',
            collection: 'advancePayments',
            countField: null,              // count all docs
        },
        {
            tag:        'manpower',
            icon:       '👥',
            title:      'জনবল তথ্য',
            body:       'আজকের জনবল তালিকা পর্যালোচনা করুন।',
            countLabel: 'টি এন্ট্রি',
            collection: 'manpower',
            countField: null,
        },
        {
            tag:        'policy-files',
            icon:       '📂',
            title:      'পলিসি ফাইলসমূহ',
            body:       'পলিসি ফাইল তালিকা আপডেট করুন।',
            countLabel: 'টি পলিসি',
            collection: 'policyFiles',
            countField: null,
        },
    ],

    8: [
        {
            tag:        'fund-archive',
            icon:       '🏦',
            title:      'ফান্ড আর্কাইভ',
            body:       'ফান্ড আর্কাইভ রেকর্ড পর্যালোচনা করুন।',
            countLabel: 'টি ফান্ড এন্ট্রি',
            collection: 'fundArchive',
            countField: null,
        },
        {
            tag:        'hbl-recovery',
            icon:       '🏧',
            title:      'HBL রিকভারি',
            body:       'HBL রিকভারি রেকর্ড পর্যালোচনা করুন।',
            countLabel: 'টি রিকভারি রেকর্ড',
            collection: 'hblRecovery',
            countField: null,
        },
        {
            tag:        'vat-tax',
            icon:       '🧾',
            title:      'ভ্যাট-ট্যাক্স হিসাব',
            body:       'ভ্যাট ও ট্যাক্স হিসাব পর্যালোচনা করুন।',
            countLabel: 'টি পেমেন্ট রেকর্ড',
            collection: 'vatTaxPayments',
            countField: null,
        },
    ],

    10: [
        // Task manager handled separately via the existing pending-task logic
        {
            tag:        'task-manager',
            icon:       '📋',
            title:      'টাস্ক ম্যানেজার',
            body:       null,              // body is built dynamically from pending tasks
            isTaskReminder: true,          // special flag — uses existing task reminder flow
        },
        {
            tag:        'office-issue',
            icon:       '🆕',
            title:      'অফিস সমস্যা',
            body:       'অফিসের সমস্যা সমাধান তালিকা দেখুন।',
            countLabel: 'টি অমীমাংসিত সমস্যা',
            collection: 'officeIssues',
            statusFilter: { field: 'resolved', value: false },
        },
        {
            tag:        'notesheet',
            icon:       '📋',
            title:      'নোটশীট',
            body:       'নোটশীট রিপোর্ট পর্যালোচনা করুন।',
            countLabel: 'টি সক্রিয় নোটশীট',
            collection: 'notesheets',
            countField: null,
        },
    ],

    12: [
        {
            tag:        'lunch-allowance',
            icon:       '🍱',
            title:      'লাঞ্চ ভাতা',
            body:       'লাঞ্চ ভাতার রেকর্ড পর্যালোচনা করুন।',
            countLabel: 'টি এন্ট্রি',
            collection: 'lunchAllowance',
            countField: null,
        },
        {
            tag:        'transport-bill',
            icon:       '🚌',
            title:      'যাতায়াত বিল',
            body:       'যাতায়াত বিল আর্কাইভ পর্যালোচনা করুন।',
            countLabel: 'টি বিল রেকর্ড',
            collection: 'transportBills',
            countField: null,
        },
        {
            tag:        'business-stats',
            icon:       '📊',
            title:      'ব্যবসায়িক পরিসংখ্যান',
            body:       'ব্যবসায়িক পরিসংখ্যান রিপোর্ট দেখুন।',
            countLabel: 'টি রিপোর্ট',
            collection: 'businessStats',
            countField: null,
        },
    ],

    14: [
        {
            tag:        'various-calculators',
            icon:       '🧮',
            title:      'বিভিন্ন ক্যালকুলেটর',
            body:       'আজকের হিসাব-নিকাশের জন্য ক্যালকুলেটর ব্যবহার করুন।',
            staticOnly: true,              // no Firestore collection — reminder only
        },
        {
            tag:        'stationary-item',
            icon:       '✏️',
            title:      'স্টেশনারী আইটেম',
            body:       'স্টেশনারী রিপোর্ট পর্যালোচনা করুন।',
            countLabel: 'টি রিপোর্ট',
            collection: 'stationaryItems',
            countField: null,
        },
        {
            tag:        'medical-bill',
            icon:       '🏥',
            title:      'মেডিকেল বিল',
            body:       'মেডিকেল বিল আর্কাইভ পর্যালোচনা করুন।',
            countLabel: 'টি রেকর্ড',
            collection: 'medicalBills',
            countField: null,
        },
    ],

    16: [
        {
            tag:        'ta-bill',
            icon:       '🚗',
            title:      'ভ্রমণ বিল (TA)',
            body:       'ভ্রমণ বিলের রেকর্ড পর্যালোচনা করুন।',
            countLabel: 'টি বিল',
            collection: 'taBills',
            countField: null,
        },
        {
            tag:        'license-forwarding',
            icon:       '📜',
            title:      'লাইসেন্স ফরওয়ার্ডিং',
            body:       'লাইসেন্স ফরওয়ার্ডিং এন্ট্রি পর্যালোচনা করুন।',
            countLabel: 'টি এন্ট্রি',
            collection: 'licenseForwarding',
            countField: null,
        },
        {
            tag:        'premium-submit',
            icon:       '✅',
            title:      'প্রিমিয়াম জমা',
            body:       'প্রিমিয়াম জমার রেকর্ড পর্যালোচনা করুন।',
            countLabel: 'টি প্রিমিয়াম এন্ট্রি',
            collection: 'premiumSubmit',
            countField: null,
        },
    ],

    18: [
        {
            tag:        'personal-dues',
            icon:       '💳',
            title:      'ব্যক্তিগত বকেয়া',
            body:       'ব্যক্তিগত বকেয়ার তালিকা পর্যালোচনা করুন।',
            countLabel: 'টি বকেয়া এন্ট্রি',
            collection: 'personalDues',
            statusFilter: { field: 'settled', value: false },
        },
        {
            tag:        'personal-expense',
            icon:       '💸',
            title:      'ব্যক্তিগত খরচ',
            body:       'আজকের ব্যক্তিগত খরচের হিসাব দেখুন।',
            countLabel: 'টি খরচ এন্ট্রি',
            collection: 'personalExpenses',
            countField: null,
        },
    ],

    20: [
        {
            tag:        'donation',
            icon:       '🤲',
            title:      'দান/চাঁদা',
            body:       'দান ও চাঁদার রেকর্ড পর্যালোচনা করুন।',
            countLabel: 'টি রেকর্ড',
            collection: 'donations',
            countField: null,
        },
        {
            tag:        'help-requisition',
            icon:       '📝',
            title:      'চাহিদা তালিকা ও ফিক্স নোট',
            body:       'স্টেশনারী চাহিদা ও ফিক্স নোট পর্যালোচনা করুন।',
            countLabel: 'টি আইটেম',
            collection: 'helpRequisition',
            countField: null,
        },
    ],

    22: [
        {
            tag:        'proposal-index',
            icon:       '📑',
            title:      'প্রস্তাবপত্র ইন্ডেক্স',
            body:       'প্রস্তাবপত্র রেজিস্টার পর্যালোচনা করুন।',
            countLabel: 'টি প্রস্তাবপত্র',
            collection: 'proposalIndex',
            countField: null,
        },
        {
            tag:        'fpr-register',
            icon:       '📒',
            title:      'FPR রেজিস্টার',
            body:       'FPR রেজিস্টার পর্যালোচনা করুন।',
            countLabel: 'টি FPR এন্ট্রি',
            collection: 'fprRegister',
            countField: null,
        },
        {
            tag:        'license-archive',
            icon:       '🗃️',
            title:      'লাইসেন্স আর্কাইভ',
            body:       'এজেন্সি নিবন্ধন আর্কাইভ পর্যালোচনা করুন।',
            countLabel: 'টি আর্কাইভ এন্ট্রি',
            collection: 'licenseArchive',
            countField: null,
        },
    ],
};

// ══════════════════════════════════════════════════════════════
// LOGGING SYSTEM
// ══════════════════════════════════════════════════════════════

class Logger {
    constructor(name = 'PUSH-WORKER') {
        this.name    = name;
        this.logsDir = path.join(__dirname, 'logs');
        this._ensureLogsDir();
    }

    _ensureLogsDir() {
        try {
            if (!fs.existsSync(this.logsDir)) {
                fs.mkdirSync(this.logsDir, { recursive: true });
            }
        } catch (err) {
            console.warn('⚠️ Failed to create logs directory:', err.message);
        }
    }

    _write(level, message, data = {}) {
        const entry = {
            timestamp: new Date().toISOString(),
            level,
            name:      this.name,
            message,
            ...data,
        };

        // Console
        const prefix = `[${entry.timestamp}] [${level}] [${this.name}]`;
        if (level === 'ERROR') {
            console.error(prefix, message, data.errorMessage || '');
            if (data.errorStack) console.error('Stack:', data.errorStack);
        } else if (level === 'WARN') {
            console.warn(prefix, message, data);
        } else {
            console.log(prefix, message, Object.keys(data).length > 0 ? data : '');
        }

        // File
        try {
            const date    = new Date().toISOString().split('T')[0];
            const logFile = path.join(this.logsDir, `push-worker-${date}.log`);
            fs.appendFileSync(logFile, JSON.stringify(entry) + '\n', 'utf8');
        } catch (_) {}
    }

    info (message, data)         { this._write('INFO',  message, data); }
    warn (message, data)         { this._write('WARN',  message, data); }
    debug(message, data)         { this._write('DEBUG', message, data); }
    error(message, err, data={}) {
        this._write('ERROR', message, {
            errorMessage: err?.message || String(err),
            errorCode:    err?.code,
            errorStack:   err?.stack,
            ...data,
        });
    }
}

const logger = new Logger('PUSH-WORKER');

// ══════════════════════════════════════════════════════════════
// FIREBASE INITIALIZATION
// FIX-1: db is created INSIDE initializeFirebase() so it is
// only called after admin.initializeApp() has run. A module-
// level `const db = admin.firestore()` crashes immediately
// because no app exists yet at parse time.
// ══════════════════════════════════════════════════════════════

let _db = null;

function getDB() {
    if (!_db) throw new Error('Firebase not initialised. Call initializeFirebase() first.');
    return _db;
}

function initializeFirebase() {
    if (!CONFIG.PROJECT_ID || !CONFIG.CLIENT_EMAIL || !CONFIG.PRIVATE_KEY) {
        logger.error('Firebase credentials incomplete', null, {
            hasProjectId:   !!CONFIG.PROJECT_ID,
            hasClientEmail: !!CONFIG.CLIENT_EMAIL,
            hasPrivateKey:  !!CONFIG.PRIVATE_KEY,
        });
        process.exit(1);
    }

    const serviceAccount = {
        type:                        'service_account',
        project_id:                  CONFIG.PROJECT_ID,
        private_key_id:              '',
        private_key:                 CONFIG.PRIVATE_KEY,
        client_email:                CONFIG.CLIENT_EMAIL,
        client_id:                   '',
        auth_uri:                    'https://accounts.google.com/o/oauth2/auth',
        token_uri:                   'https://oauth2.googleapis.com/token',
        auth_provider_x509_cert_url: 'https://www.googleapis.com/oauth2/v1/certs',
    };

    try {
        admin.initializeApp({
            credential: admin.credential.cert(serviceAccount),
            projectId:  CONFIG.PROJECT_ID,
        });
    } catch (err) {
        if (err.code !== 'app/duplicate-app') {
            logger.error('Firebase initializeApp failed', err);
            process.exit(1);
        }
        logger.warn('Firebase app already initialised — reusing existing app.');
    }

    // FIX-1: assign AFTER initializeApp
    _db = admin.firestore();
    logger.info('✅ Firebase initialised', { projectId: CONFIG.PROJECT_ID });
}

// ══════════════════════════════════════════════════════════════
// WEB PUSH INITIALIZATION
// ══════════════════════════════════════════════════════════════

function initializeWebPush() {
    if (!CONFIG.VAPID_PUBLIC_KEY || !CONFIG.VAPID_PRIVATE_KEY) {
        logger.error('VAPID keys missing', null, {
            hasPublicKey:  !!CONFIG.VAPID_PUBLIC_KEY,
            hasPrivateKey: !!CONFIG.VAPID_PRIVATE_KEY,
        });
        process.exit(1);
    }
    webpush.setVapidDetails(
        CONFIG.VAPID_SUBJECT,
        CONFIG.VAPID_PUBLIC_KEY,
        CONFIG.VAPID_PRIVATE_KEY
    );
    logger.info('✅ Web Push VAPID configured', { subject: CONFIG.VAPID_SUBJECT });
}

// ══════════════════════════════════════════════════════════════
// TIME UTILITIES
// ══════════════════════════════════════════════════════════════

/** Current hour in Dhaka time (0–23). */
function getDhakaHour() {
    const now     = new Date();
    const utcHour = now.getUTCHours();
    return (utcHour + 6) % 24;
}

/** Today's date string in Dhaka time (YYYY-MM-DD). */
function getTodayDhaka() {
    const dhaka = new Date(Date.now() + 6 * 3600 * 1000);
    return dhaka.getUTCFullYear() + '-' +
        String(dhaka.getUTCMonth() + 1).padStart(2, '0') + '-' +
        String(dhaka.getUTCDate()).padStart(2, '0');
}

/** Convert a Firestore Timestamp to a Dhaka date string (YYYY-MM-DD). */
function taskDateToDhaka(firebaseTimestamp) {
    if (!firebaseTimestamp) return null;
    try {
        const d     = firebaseTimestamp.toDate ? firebaseTimestamp.toDate() : new Date(firebaseTimestamp);
        const dhaka = new Date(d.getTime() + 6 * 3600 * 1000);
        return dhaka.getUTCFullYear() + '-' +
            String(dhaka.getUTCMonth() + 1).padStart(2, '0') + '-' +
            String(dhaka.getUTCDate()).padStart(2, '0');
    } catch (err) {
        logger.warn('Failed to parse task date', { error: err.message });
        return null;
    }
}

// ══════════════════════════════════════════════════════════════
// STATUS KEY (deduplication hash)
// Matches the DJB2 hash in app.js _buildStatusKey() exactly.
// ══════════════════════════════════════════════════════════════

function buildStatusKey(tag, pendingTasks) {
    if (tag !== 'task-manager') {
        // For module pushes use a simple daily key so each slot fires
        // once per day maximum regardless of record changes.
        return `module_${tag}_${getTodayDhaka()}`;
    }

    // Task-reminder: hash of sorted task titles (matches app.js)
    if (!pendingTasks || pendingTasks.length === 0) return 'tasks_empty';
    const sorted = [...pendingTasks].map(t => (t.title || t)).sort();
    const joined = sorted.map(t => String(t).slice(0, 30)).join('|');
    let h = 5381;
    for (let i = 0; i < joined.length; i++) {
        h = ((h << 5) + h) ^ joined.charCodeAt(i);
        h = h >>> 0;
    }
    return `tasks_${h}`;
}

// ══════════════════════════════════════════════════════════════
// SEEN-TODAY / SNOOZE STATE
// ══════════════════════════════════════════════════════════════

async function checkSeenOrSnoozed(uid, statusKey) {
    try {
        const ref  = getDB().doc(`artifacts/default-app-id/users/${uid}/pushState/seenToday`);
        const snap = await ref.get();
        if (!snap.exists()) return { skip: false };

        const data  = snap.data();
        const today = getTodayDhaka();
        if (data.date !== today) return { skip: false };

        if (Array.isArray(data.keys) && data.keys.includes(statusKey)) {
            return { skip: true, reason: 'Already sent today' };
        }

        if (data.snoozed && data.snoozed[statusKey]) {
            const until = data.snoozed[statusKey];
            if (Date.now() < until) {
                return { skip: true, reason: `Snoozed until ${new Date(until).toLocaleTimeString()}` };
            }
        }

        return { skip: false };
    } catch (err) {
        logger.warn('checkSeenOrSnoozed failed — proceeding', { uid, statusKey, error: err.message });
        return { skip: false };
    }
}

async function markSeenToday(uid, statusKey) {
    try {
        const today = getTodayDhaka();
        const ref   = getDB().doc(`artifacts/default-app-id/users/${uid}/pushState/seenToday`);
        const snap  = await ref.get();
        const data  = snap.exists() ? snap.data() : {};

        const existingDate = data.date || '';
        const keys         = existingDate === today ? (data.keys || []) : [];

        if (!keys.includes(statusKey)) {
            const trimmed = keys.length >= 200 ? keys.slice(-199) : keys;
            trimmed.push(statusKey);
            await ref.set(
                {
                    date:    today,
                    keys:    trimmed,
                    snoozed: existingDate === today ? (data.snoozed || {}) : {},
                },
                { merge: true }
            );
        }
    } catch (err) {
        logger.warn('markSeenToday failed', { uid, statusKey, error: err.message });
    }
}

// ══════════════════════════════════════════════════════════════
// MARK NOTIFICATIONS AS PUSHED
// FIX-4: old code queried tag == 'task-manager' which matched
// in-app event notifications (taskAdded, etc.), silently marking
// them pushed:true and suppressing in-app sounds. We now only
// mark docs that are ALREADY unread + unpushed and whose
// timestamp is within the last 2 minutes — i.e. the specific
// "task-reminder" notification the push worker just sent.
// For module pushes we do NOT touch the notifications collection
// at all — those are written by user actions, not the scheduler.
// ══════════════════════════════════════════════════════════════

async function markTaskNotificationsAsPushed(uid) {
    try {
        const twoMinutesAgo = new Date(Date.now() - 2 * 60 * 1000);
        const notifsRef = getDB().collection(`artifacts/default-app-id/users/${uid}/notifications`);

        // Only touch unread, unpushed docs written in the last 2 minutes
        const snap = await notifsRef
            .where('pushed', '==', false)
            .where('tag', '==', 'task-manager')
            .get();

        let marked = 0;
        for (const d of snap.docs) {
            const ts = d.data().timestamp;
            const docTime = ts?.toDate ? ts.toDate() : (ts ? new Date(ts) : null);
            if (docTime && docTime >= twoMinutesAgo) {
                await d.ref.update({
                    pushed:   true,
                    pushedAt: admin.firestore.FieldValue.serverTimestamp(),
                }).catch(() => {});
                marked++;
            }
        }

        if (marked > 0) logger.debug(`Marked ${marked} task notifications as pushed`, { uid });
        return marked;
    } catch (err) {
        logger.warn('markTaskNotificationsAsPushed failed', { uid, error: err.message });
        return 0;
    }
}

// ══════════════════════════════════════════════════════════════
// COLLECTION RECORD COUNT
// Fetches document count from a user sub-collection with an
// optional equality filter (e.g. resolved: false).
// ══════════════════════════════════════════════════════════════

async function getCollectionCount(uid, collectionName, statusFilter) {
    try {
        let ref = getDB().collection(`artifacts/default-app-id/users/${uid}/${collectionName}`);
        let q   = ref;

        if (statusFilter) {
            q = ref.where(statusFilter.field, '==', statusFilter.value);
        }

        // Use count() aggregation when available (firebase-admin >= 11.10)
        // Fall back to getDocs() size for older SDK versions.
        if (typeof q.count === 'function') {
            const agg = await q.count().get();
            return agg.data().count;
        }

        const snap = await q.get();
        return snap.size;
    } catch (err) {
        logger.warn('getCollectionCount failed', { uid, collectionName, error: err.message });
        return null; // null = unknown, show static body instead
    }
}

// ══════════════════════════════════════════════════════════════
// PUSH PAYLOAD BUILDERS
// ══════════════════════════════════════════════════════════════

/**
 * Build payload for a TASK REMINDER push (10 AM slot).
 */
function buildTaskReminderPayload(statusKey, pendingTasks, uid, deviceName) {
    const firstTitle  = pendingTasks[0]?.title || 'কোনো শিরোনাম নেই';
    const remaining   = Math.max(0, pendingTasks.length - 1);
    const remainText  = remaining > 0 ? ` এবং আরও ${remaining} টি` : '';

    return {
        title:              `📋 ${pendingTasks.length} টি টাস্ক অপেক্ষমান`,
        body:               `"${firstTitle.slice(0, 50)}"${remainText}`,
        icon:               'https://raw.githubusercontent.com/sifatahmedpro/PettyCash-Statement/main/logo.png',
        badge:              'https://raw.githubusercontent.com/sifatahmedpro/PettyCash-Statement/main/logo.png',
        tag:                `task-reminder-${statusKey}`,
        requireInteraction: true,
        vibrate:            [200, 100, 200],
        timestamp:          Date.now(),
        data: {
            statusKey,
            uid,
            projectId:  CONFIG.PROJECT_ID,
            apiKey:     CONFIG.API_KEY,
            deviceName: deviceName || '',
            type:       'task-reminder',
            taskCount:  pendingTasks.length,
            firstTask:  firstTitle,
            url:        'https://officemanagement.app/dashboard.html',
        },
        actions: [
            { action: 'open',      title: '📋 দেখুন'        },
            { action: 'snooze',    title: '⏰ ১ ঘণ্টা পরে'  },
            { action: 'mark_read', title: '✅ দেখা হয়েছে'   },
            { action: 'dismiss',   title: '✕ বন্ধ করুন'     },
        ],
    };
}

/**
 * Build payload for a MODULE REMINDER push (all other slots).
 * If count is null, omit the count from the body.
 */
function buildModulePayload(module, statusKey, uid, count, deviceName) {
    let body = module.body;

    if (!module.staticOnly && count !== null && module.countLabel) {
        body = `${count} ${module.countLabel} আছে। পর্যালোচনা করুন।`;
    }

    return {
        title:              `${module.icon} ${module.title}`,
        body:               body || `${module.title} পর্যালোচনা করুন।`,
        icon:               'https://raw.githubusercontent.com/sifatahmedpro/PettyCash-Statement/main/logo.png',
        badge:              'https://raw.githubusercontent.com/sifatahmedpro/PettyCash-Statement/main/logo.png',
        tag:                `notif-${module.tag}-${statusKey}`,
        requireInteraction: false,         // module reminders auto-dismiss
        vibrate:            [100, 50, 100],
        timestamp:          Date.now(),
        data: {
            statusKey,
            uid,
            notifId:    null,
            projectId:  CONFIG.PROJECT_ID,
            apiKey:     CONFIG.API_KEY,
            deviceName: deviceName || '',
            type:       'module-notif',
            tag:        module.tag,
            url:        'https://officemanagement.app/dashboard.html',
        },
        // No action buttons for module notifications — tap to open
    };
}

// ══════════════════════════════════════════════════════════════
// SEND WITH RETRY (exponential back-off)
// ══════════════════════════════════════════════════════════════

async function sendWebPushWithRetry(subscription, payload, maxAttempts = CONFIG.MAX_RETRY_ATTEMPTS) {
    const subId      = (subscription.endpoint || '').slice(0, 25) + '…';
    const deviceName = subscription.deviceName || 'Unknown Device';

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
            await webpush.sendNotification(subscription, JSON.stringify(payload));
            logger.info('✅ Push sent', { device: deviceName, subId, attempt });
            return { success: true };
        } catch (err) {
            if (err.statusCode === 410 || err.statusCode === 404) {
                logger.warn('Subscription expired (410/404)', { device: deviceName, subId });
                return { success: false, expired: true, endpoint: subscription.endpoint };
            }
            if (err.statusCode === 401) {
                logger.error('VAPID auth failed (401)', err, { device: deviceName });
                return { success: false, expired: false };
            }
            if (attempt < maxAttempts) {
                const delayMs = CONFIG.RETRY_DELAY_MS * Math.pow(2, attempt - 1);
                logger.warn(`Push failed (attempt ${attempt}), retrying in ${delayMs}ms`, {
                    device: deviceName, error: err.message,
                });
                await new Promise(r => setTimeout(r, delayMs));
            } else {
                logger.error(`Push failed after ${maxAttempts} attempts`, err, { device: deviceName, subId });
                return { success: false, expired: false };
            }
        }
    }
    return { success: false, expired: false };
}

// ══════════════════════════════════════════════════════════════
// CLEANUP EXPIRED SUBSCRIPTIONS
// ══════════════════════════════════════════════════════════════

async function cleanupExpiredSubscriptions(uid, expiredEndpoints) {
    if (!expiredEndpoints.length) return 0;
    try {
        const colRef = getDB().collection(`artifacts/default-app-id/users/${uid}/pushSubscriptions`);
        const snap   = await colRef.get();
        let deleted  = 0;
        for (const d of snap.docs) {
            if (expiredEndpoints.includes(d.data().endpoint)) {
                await d.ref.delete();
                deleted++;
                logger.info('Deleted expired subscription', { uid, subId: d.id });
            }
        }
        return deleted;
    } catch (err) {
        logger.warn('cleanupExpiredSubscriptions failed', { uid, error: err.message });
        return 0;
    }
}

// ══════════════════════════════════════════════════════════════
// RESET SEEN-TODAY STATE FOR ALL USERS
// Called when RESET_SEEN_TODAY=true (manual / force_send runs).
// Deletes the seenToday document for every user so that all
// modules are eligible to fire again regardless of whether they
// were already sent earlier today.
// ══════════════════════════════════════════════════════════════

async function resetSeenTodayForAllUsers(userDocs) {
    let reset = 0;
    for (const userDoc of userDocs) {
        try {
            const ref = getDB().doc(
                `artifacts/default-app-id/users/${userDoc.id}/pushState/seenToday`
            );
            await ref.delete();
            reset++;
        } catch (err) {
            logger.warn('resetSeenToday failed for user', {
                uid: userDoc.id, error: err.message,
            });
        }
    }
    logger.info(`🔄 Seen-today state cleared for ${reset} user(s).`);
    return reset;
}

// ══════════════════════════════════════════════════════════════
// FETCH ALL PUSH SUBSCRIPTIONS FOR A USER
// ══════════════════════════════════════════════════════════════

async function getUserSubscriptions(uid) {
    try {
        const snap = await getDB()
            .collection(`artifacts/default-app-id/users/${uid}/pushSubscriptions`)
            .get();
        return snap.docs.map(d => d.data()).filter(s => s.endpoint && s.keys);
    } catch (err) {
        logger.warn('getUserSubscriptions failed', { uid, error: err.message });
        return [];
    }
}

// ══════════════════════════════════════════════════════════════
// SEND ONE MODULE NOTIFICATION TO ALL SUBSCRIPTIONS OF A USER
// Returns { pushed, skipped, errors, expiredEndpoints }
// ══════════════════════════════════════════════════════════════

async function sendModuleNotificationToUser(uid, module, subscriptions) {
    const result = { pushed: 0, skipped: 0, errors: 0, expiredEndpoints: [] };

    const statusKey  = buildStatusKey(module.tag, null);
    const seenCheck  = await checkSeenOrSnoozed(uid, statusKey);

    if (seenCheck.skip) {
        logger.debug(`Module ${module.tag} skipped for ${uid}: ${seenCheck.reason}`);
        result.skipped++;
        return result;
    }

    // Count records (skip for static-only modules)
    let count = null;
    if (!module.staticOnly && module.collection) {
        count = await getCollectionCount(uid, module.collection, module.statusFilter || null);
    }

    // Send to each subscription
    for (const sub of subscriptions) {
        const payload = buildModulePayload(module, statusKey, uid, count, sub.deviceName);

        if (CONFIG.DRY_RUN) {
            logger.info(`🧪 DRY RUN: would send [${module.tag}]`, {
                uid, device: sub.deviceName, count,
            });
            result.pushed++;
            continue;
        }

        const res = await sendWebPushWithRetry(sub, payload);
        if (res.success) {
            result.pushed++;
        } else if (res.expired) {
            result.expiredEndpoints.push(res.endpoint);
            result.errors++;
        } else {
            result.errors++;
        }
    }

    if (result.pushed > 0 && !CONFIG.DRY_RUN) {
        await markSeenToday(uid, statusKey);
    }

    return result;
}

// ══════════════════════════════════════════════════════════════
// SEND TASK REMINDER TO ONE USER (10 AM slot)
// Returns { pushed, skipped, errors, expiredEndpoints }
// ══════════════════════════════════════════════════════════════

async function sendTaskReminderToUser(uid, today, subscriptions) {
    const result = { pushed: 0, skipped: 0, errors: 0, expiredEndpoints: [] };

    // Fetch pending tasks
    let pendingTasks = [];
    try {
        const tasksSnap = await getDB()
            .collection(`artifacts/default-app-id/users/${uid}/tasks`)
            .where('status', '!=', 'done')
            .get();

        tasksSnap.forEach(d => {
            const task = d.data();
            if (!task.date) {
                pendingTasks.push({ title: task.title || 'Untitled Task' });
                return;
            }
            const taskDate = taskDateToDhaka(task.date);
            if (taskDate && taskDate <= today) {
                pendingTasks.push({ title: task.title || 'Untitled Task' });
            }
        });
    } catch (err) {
        logger.error('Failed to fetch tasks', err, { uid });
        result.errors++;
        return result;
    }

    if (pendingTasks.length === 0) {
        logger.debug('No pending tasks for user', { uid });
        result.skipped++;
        return result;
    }

    const statusKey = buildStatusKey('task-manager', pendingTasks);
    const seenCheck = await checkSeenOrSnoozed(uid, statusKey);

    if (seenCheck.skip) {
        logger.debug(`Task reminder skipped for ${uid}: ${seenCheck.reason}`);
        result.skipped++;
        return result;
    }

    for (const sub of subscriptions) {
        const payload = buildTaskReminderPayload(statusKey, pendingTasks, uid, sub.deviceName);

        if (CONFIG.DRY_RUN) {
            logger.info('🧪 DRY RUN: would send task reminder', {
                uid, device: sub.deviceName, taskCount: pendingTasks.length,
            });
            result.pushed++;
            continue;
        }

        const res = await sendWebPushWithRetry(sub, payload);
        if (res.success) {
            result.pushed++;
        } else if (res.expired) {
            result.expiredEndpoints.push(res.endpoint);
            result.errors++;
        } else {
            result.errors++;
        }
    }

    if (result.pushed > 0 && !CONFIG.DRY_RUN) {
        await markSeenToday(uid, statusKey);
        await markTaskNotificationsAsPushed(uid);
    }

    return result;
}

// ══════════════════════════════════════════════════════════════
// PROCESS ALL MODULES FOR ONE USER AT THE CURRENT HOUR SLOT
// FIX-3: users are processed once — no second loop.
// ══════════════════════════════════════════════════════════════

async function processUserAtHour(uid, dhakaHour, today) {
    const stats = { pushed: 0, skipped: 0, errors: 0, expiredEndpoints: [] };

    const modules = HOUR_SCHEDULE[dhakaHour];
    if (!modules || modules.length === 0) {
        logger.debug(`No schedule for hour ${dhakaHour}`, { uid });
        return stats;
    }

    const subscriptions = await getUserSubscriptions(uid);
    if (subscriptions.length === 0) {
        logger.debug('No push subscriptions for user', { uid });
        stats.skipped += modules.length;
        return stats;
    }

    for (const module of modules) {
        try {
            let result;

            if (module.isTaskReminder) {
                result = await sendTaskReminderToUser(uid, today, subscriptions);
            } else {
                result = await sendModuleNotificationToUser(uid, module, subscriptions);
            }

            stats.pushed   += result.pushed;
            stats.skipped  += result.skipped;
            stats.errors   += result.errors;
            stats.expiredEndpoints.push(...result.expiredEndpoints);

        } catch (err) {
            logger.error(`Error processing module [${module.tag}] for user ${uid}`, err);
            stats.errors++;
        }
    }

    return stats;
}

// ══════════════════════════════════════════════════════════════
// MAIN WORKER
// ══════════════════════════════════════════════════════════════

async function runPushWorker() {
    const startTime  = Date.now();
    const dhakaHour  = getDhakaHour();
    const today      = getTodayDhaka();
    const isActive   = dhakaHour >= CONFIG.ACTIVE_HOUR_START && dhakaHour < CONFIG.ACTIVE_HOUR_END;
    const hasSlot    = !!HOUR_SCHEDULE[dhakaHour];

    // Auto-enable RESET_SEEN_TODAY for manual triggers and force_send runs
    // so testing always fires real pushes without needing to manually clear
    // Firestore state between test runs.
    if ((CONFIG.IS_MANUAL_TRIGGER || CONFIG.FORCE_SEND) && !CONFIG.RESET_SEEN_TODAY) {
        CONFIG.RESET_SEEN_TODAY = true;
        logger.info('ℹ️  Auto-enabled RESET_SEEN_TODAY for manual/force run.');
    }

    logger.info('🚀 Push worker started', {
        runId:           CONFIG.RUN_ID,
        runNumber:       CONFIG.RUN_NUMBER,
        actor:           CONFIG.GITHUB_ACTOR,
        isManual:        CONFIG.IS_MANUAL_TRIGGER,
        isDryRun:        CONFIG.DRY_RUN,
        forceSend:       CONFIG.FORCE_SEND,
        resetSeenToday:  CONFIG.RESET_SEEN_TODAY,
        dhakaHour,
        today,
        isActive,
        hasSlot,
    });

    // Guard: resting hours (23:00–06:00 Dhaka)
    if (!isActive && !CONFIG.FORCE_SEND && !CONFIG.IS_MANUAL_TRIGGER) {
        logger.warn('🔴 Resting hours — exiting silently', { dhakaHour });
        return { success: true, skipped: true, reason: 'Resting hours' };
    }

    // Guard: no schedule for this hour (e.g. worker fired at 07:xx)
    if (!hasSlot && !CONFIG.FORCE_SEND && !CONFIG.IS_MANUAL_TRIGGER) {
        logger.info(`⏩ No schedule for hour ${dhakaHour} — nothing to send.`);
        return { success: true, skipped: true, reason: `No schedule for hour ${dhakaHour}` };
    }

    if (CONFIG.DRY_RUN) logger.info('🧪 DRY RUN MODE — no actual pushes will be sent');

    // ── Determine which hour slot(s) to run ───────────────────
    // • Normal cron run   → the single slot matching the current Dhaka hour
    // • force_send=true   → same: honour the current hour (or nearest earlier)
    // • Manual trigger    → send ALL 9 slots so every module fires, giving a
    //                        full end-to-end test with one button press.
    //                        If you want only a specific slot, set force_send=true
    //                        instead (which picks the nearest earlier slot).
    let slotsToRun;

    if (CONFIG.IS_MANUAL_TRIGGER && !CONFIG.FORCE_SEND) {
        // Manual (no force_send) → run every scheduled slot
        slotsToRun = Object.keys(HOUR_SCHEDULE).map(Number).sort((a, b) => a - b);
        logger.info(`📋 Manual trigger: running ALL ${slotsToRun.length} slots → [${slotsToRun.join(', ')}]`);
    } else {
        // Cron or force_send → single effective hour
        const effectiveHour = hasSlot ? dhakaHour : (() => {
            const slots   = Object.keys(HOUR_SCHEDULE).map(Number).sort((a, b) => a - b);
            const earlier = slots.filter(h => h <= dhakaHour);
            return earlier.length ? earlier[earlier.length - 1] : slots[0];
        })();

        if (effectiveHour !== dhakaHour) {
            logger.info(`Force/cron: using nearest earlier slot (hour ${effectiveHour})`);
        }
        slotsToRun = [effectiveHour];
    }

    const globalStats = {
        usersProcessed:      0,
        usersFailed:         0,
        pushSent:            0,
        pushSkipped:         0,
        errors:              0,
        subscriptionsCleaned: 0,
    };

    // Fetch all users
    let usersSnap;
    try {
        usersSnap = await getDB().collection('artifacts/default-app-id/users').get();
    } catch (err) {
        logger.error('Failed to fetch users collection', err);
        return { success: false, error: err.message, duration: Date.now() - startTime };
    }

    if (usersSnap.empty) {
        logger.warn('⚠️ No users found in Firestore');
        return { success: true, stats: globalStats, duration: Date.now() - startTime };
    }

    const users = usersSnap.docs;

    // Clear seen-today before processing so deduplication never silently
    // suppresses modules during manual / force_send runs.
    if (CONFIG.RESET_SEEN_TODAY && !CONFIG.DRY_RUN) {
        await resetSeenTodayForAllUsers(users);
    }

    const allExpiredEndpoints = new Set();

    // Process each slot in order
    for (const slotHour of slotsToRun) {
        logger.info(`📊 Processing ${users.length} user(s) for hour ${slotHour}`, {
            modules: (HOUR_SCHEDULE[slotHour] || []).map(m => m.tag),
        });

        // FIX-3: single pass — no second loop that re-calls processUserAtHour
        for (let i = 0; i < users.length; i += CONFIG.BATCH_SIZE) {
            const batch = users.slice(i, i + CONFIG.BATCH_SIZE);

            await Promise.all(batch.map(async userDoc => {
                try {
                    const uid    = userDoc.id;
                    const result = await processUserAtHour(uid, slotHour, today);

                    globalStats.usersProcessed++;
                    globalStats.pushSent    += result.pushed;
                    globalStats.pushSkipped += result.skipped;
                    globalStats.errors      += result.errors;
                    result.expiredEndpoints.forEach(ep => allExpiredEndpoints.add(ep));

                } catch (err) {
                    logger.error('Failed to process user', err, { uid: userDoc.id });
                    globalStats.usersFailed++;
                    globalStats.errors++;
                }
            }));
        }
    }

    // Cleanup expired subscriptions (once, after all users)
    const expiredList = Array.from(allExpiredEndpoints);
    if (expiredList.length > 0) {
        for (const userDoc of users) {
            const cleaned = await cleanupExpiredSubscriptions(userDoc.id, expiredList);
            globalStats.subscriptionsCleaned += cleaned;
        }
    }

    const duration = Date.now() - startTime;

    logger.info('✅ Push worker completed', {
        ...globalStats,
        durationMs:  duration,
        durationSec: Math.round(duration / 1000),
        slots:       slotsToRun,
    });

    return { success: true, stats: globalStats, duration };
}

// ══════════════════════════════════════════════════════════════
// ENTRY POINT
// ══════════════════════════════════════════════════════════════

(async () => {
    try {
        logger.info('═══════════════════════════════════════════════════════════');
        logger.info('   অফিস ম্যানেজমেন্ট সিস্টেম — Push Notification Worker v4.0');
        logger.info('═══════════════════════════════════════════════════════════');

        initializeFirebase();
        initializeWebPush();

        const result = await runPushWorker();

        if (result.success) {
            logger.info('Worker exited successfully.', { skipped: result.skipped || false });
            process.exit(0);
        } else {
            logger.error('Worker exited with failure', new Error(result.error));
            process.exit(1);
        }
    } catch (err) {
        logger.error('💥 Uncaught error in main entry point', err);
        process.exit(1);
    }
})();

module.exports = { runPushWorker, buildStatusKey, buildTaskReminderPayload, buildModulePayload };
