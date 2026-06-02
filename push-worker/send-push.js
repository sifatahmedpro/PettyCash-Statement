/**
 * ============================================================
 * push-worker/send-push.js  —  v6.0  (Supabase migration)
 * Standalone Push Notification Worker for GitHub Actions
 * Project : অফিস ম্যানেজমেন্ট সিস্টেম
 *
 * MIGRATION SUMMARY (Firebase Admin → Supabase):
 *
 *   • firebase-admin             → @supabase/supabase-js (service-role key)
 *   • initializeFirebase()       → initializeSupabase()
 *   • admin.firestore()          → supabase.from(...)
 *   • Firestore path structure:
 *       artifacts/default-app-id/users/{uid}/{collection}
 *     replaced by flat Supabase tables with a `uid` column:
 *       push_subscriptions, push_state, push_logs, push_run_logs,
 *       tasks, notifications
 *   • Collection-based data modules (advance_payments, etc.) remain
 *     as Supabase tables with a `uid` column.
 *   • Single-doc modules (manpower, policy-files) stored in JSONB
 *     columns in a `user_data` table keyed by (uid, key).
 *   • admin.firestore.FieldValue.serverTimestamp() → omitted (DB default)
 *   • .count().get() aggregation → supabase .select('*', {count:'exact', head:true})
 *   • onSnapshot / .get()        → await supabase.from(...).select(...)
 *   • batch delete               → .delete().eq('uid', uid)
 *   • doc .set() / .update()     → .upsert() / .update()
 *
 * ENVIRONMENT VARIABLES (GitHub Secrets):
 *   SUPABASE_URL               (required — replaces FIREBASE_PROJECT_ID + CLIENT_EMAIL + PRIVATE_KEY)
 *   SUPABASE_SERVICE_ROLE_KEY  (required — service-role key bypasses RLS)
 *   VAPID_PUBLIC_KEY           (required — unchanged)
 *   VAPID_PRIVATE_KEY          (required — unchanged)
 *   VAPID_SUBJECT              (optional — unchanged)
 *   FORCE_SEND                 (optional, workflow_dispatch only)
 *   DRY_RUN                    (optional, workflow_dispatch only)
 *   RESET_SEEN_TODAY           (optional — auto-set for manual/force runs)
 *
 * SUPABASE TABLES REQUIRED:
 *   push_subscriptions  (uid, endpoint, keys jsonb, device_name, created_at)
 *   push_state          (uid, date, keys text[], snoozed jsonb)  — upsert by uid
 *   push_logs           (uid, tag, label, icon, status, sent_at, detail, sent,
 *                        failed, skipped, slot_hour, status_key, run_id,
 *                        record_count, devices jsonb)
 *   push_run_logs       (same columns as push_logs)
 *   tasks               (uid, title, status, date timestamptz, last_reminder_date)
 *   notifications       (uid, tag, pushed bool, timestamp timestamptz)
 *   user_data           (uid, key text, data jsonb)  — for single-doc modules
 *   + one table per collection module (advance_payments, logs, hbl_recovery_records,
 *     vatPayments, office_issues, notesheetReports, transport-bill-archive,
 *     business_analysis_archives, stationary_reports, archives_medical_bills_reports,
 *     ta_bills, license_archive, accounts, accountsArchive_1st_year,
 *     personal_dues, personal_expenses, donations, proposals, fprEntries)
 *     — each with a `uid` column.
 *
 * ALL CACHING, RETRY, DEDUP, AND SLOT LOGIC ARE PRESERVED UNCHANGED.
 * ============================================================
 */

'use strict';

const { createClient } = require('@supabase/supabase-js');
const webpush          = require('web-push');
const fs               = require('fs');
const path             = require('path');
// Node.js < 22 has no native WebSocket — the Supabase Realtime client needs
// the 'ws' package passed explicitly as the transport option.
const ws               = require('ws');

// ══════════════════════════════════════════════════════════════
// CONFIGURATION
// ══════════════════════════════════════════════════════════════

const CONFIG = {
    // ── Supabase (replaces FIREBASE_PROJECT_ID / CLIENT_EMAIL / PRIVATE_KEY) ──
    SUPABASE_URL:              process.env.SUPABASE_URL              || '',
    SUPABASE_SERVICE_ROLE_KEY: process.env.SUPABASE_SERVICE_ROLE_KEY || '',

    VAPID_PUBLIC_KEY:  process.env.VAPID_PUBLIC_KEY  || '',
    VAPID_PRIVATE_KEY: process.env.VAPID_PRIVATE_KEY || '',
    VAPID_SUBJECT:     process.env.VAPID_SUBJECT     || 'mailto:support@officemanagement.app',

    RUN_ID:            process.env.RUN_ID              || Math.random().toString(36).slice(2, 8),
    RUN_NUMBER:        process.env.WORKFLOW_RUN_NUMBER || 'unknown',
    GITHUB_ACTOR:      process.env.GITHUB_ACTOR        || 'system',
    IS_MANUAL_TRIGGER: process.env.IS_MANUAL_TRIGGER   === 'true',

    FORCE_SEND:        process.env.FORCE_SEND        === 'true',
    DRY_RUN:           process.env.DRY_RUN           === 'true',
    RESET_SEEN_TODAY:  process.env.RESET_SEEN_TODAY  === 'true',

    TIMEZONE: 'Asia/Dhaka',

    ACTIVE_HOUR_START:   6,
    ACTIVE_HOUR_END:    24,

    MAX_RETRY_ATTEMPTS: 3,
    RETRY_DELAY_MS:     1000,
    BATCH_SIZE:         10,
};

// ══════════════════════════════════════════════════════════════
// SCHEDULE TABLE  (unchanged from v5.9)
// ══════════════════════════════════════════════════════════════

const HOUR_SCHEDULE = {
    6: [
        {
            tag: 'advance-payment', icon: '💰', title: 'অগ্রিম পরিশোধ',
            body: 'অগ্রিম পরিশোধ রেকর্ড পর্যালোচনা করুন।',
            countLabel: 'টি সক্রিয় রেকর্ড', collection: 'advance_payments', countField: null,
        },
        {
            tag: 'manpower', icon: '👥', title: 'জনবল ব্যবস্থাপনা',
            body: 'আজকের জনবল তালিকা পর্যালোচনা করুন।',
            countLabel: 'টি কর্মী',
            isSingleDoc: true, docKey: 'manpower',
            arrayFields: ['jbc', 'dm', 'do', 'agent'], previewArray: 'agent', previewField: 'name',
        },
        {
            tag: 'policy-files', icon: '📂', title: 'পলিসি ফাইলসমূহ',
            body: 'পলিসি ফাইল তালিকা আপডেট করুন।',
            countLabel: 'টি পলিসি',
            isSingleDoc: true, docKey: 'policy-files',
            arrayFields: ['general', 'monthly'], previewArray: 'general', previewField: 'policyNo',
        },
    ],

    8: [
        {
            tag: 'fund-archive', icon: '🏦', title: 'ফান্ড ব্যবস্থাপনা',
            body: 'ফান্ড আর্কাইভ রেকর্ড পর্যালোচনা করুন।',
            countLabel: 'টি ফান্ড এন্ট্রি', collection: 'logs', orderByField: 'created_at',
        },
        {
            tag: 'hbl-recovery', icon: '🏧', title: 'গৃহ ঋণ পরিশোধ',
            body: 'HBL রিকভারি রেকর্ড পর্যালোচনা করুন।',
            countLabel: 'টি রিকভারি রেকর্ড', collection: 'hbl_recovery_records', countField: null,
        },
        {
            tag: 'vat-tax', icon: '🧾', title: 'ভ্যাট-ট্যাক্স হিসাব',
            body: 'ভ্যাট ও ট্যাক্স হিসাব পর্যালোচনা করুন।',
            countLabel: 'টি পেমেন্ট রেকর্ড', collection: 'vatPayments',
            orderByField: 'created_at', countField: null,
        },
    ],

    10: [
        {
            tag: 'task-manager', icon: '📋', title: 'টাস্ক ব্যবস্থাপনা',
            body: null, isTaskReminder: true,
        },
        {
            tag: 'office-issue', icon: '🆕', title: 'অফিস সমস্যা ও সমাধান',
            body: 'অফিসের সমস্যা সমাধান তালিকা দেখুন।',
            countLabel: 'টি অমীমাংসিত সমস্যা', collection: 'office_issues',
        },
        {
            tag: 'notesheet', icon: '📋', title: 'নোটশীট',
            body: 'নোটশীট রিপোর্ট পর্যালোচনা করুন।',
            countLabel: 'টি সক্রিয় নোটশীট', collection: 'notesheetReports', countField: null,
        },
    ],

    12: [
        {
            tag: 'lunch-allowance', icon: '🍱', title: 'লাঞ্চ ভাতা ব্যবস্থাপনা',
            body: 'লাঞ্চ ভাতার রেকর্ড পর্যালোচনা করুন।', staticOnly: true,
        },
        {
            tag: 'transport-bill', icon: '🚌', title: 'যাতায়াত বিল',
            body: 'যাতায়াত বিল আর্কাইভ পর্যালোচনা করুন।',
            countLabel: 'টি বিল রেকর্ড', collection: 'transport_bill_archive', countField: null,
        },
        {
            tag: 'business-stats', icon: '📊', title: 'ব্যবসা পরিসংখ্যান',
            body: 'ব্যবসায়িক পরিসংখ্যান রিপোর্ট দেখুন।',
            countLabel: 'টি রিপোর্ট', collection: 'business_analysis_archives',
            orderByField: 'meta->createdAt', countField: null,
        },
    ],

    14: [
        {
            tag: 'various-calculators', icon: '🧮', title: 'বিবিধ হিসাব',
            body: 'আজকের হিসাব-নিকাশের জন্য ক্যালকুলেটর ব্যবহার করুন।', staticOnly: true,
        },
        {
            tag: 'stationary-item', icon: '✏️', title: 'মুদ্রণ সামগ্রী সরবরাহ',
            body: 'স্টেশনারী রিপোর্ট পর্যালোচনা করুন।',
            countLabel: 'টি রিপোর্ট', collection: 'stationary_reports', countField: null,
        },
        {
            tag: 'medical-bill', icon: '🏥', title: 'মেডিকেল বিল পরিশোধ',
            body: 'মেডিকেল বিল পরিশোধ আর্কাইভ পর্যালোচনা করুন।',
            countLabel: 'টি রেকর্ড', collection: 'archives_medical_bills_reports', countField: null,
        },
    ],

    16: [
        {
            tag: 'ta-bill', icon: '🚗', title: 'ভ্রমণ বিল',
            body: 'ভ্রমণ বিলের রেকর্ড পর্যালোচনা করুন।',
            countLabel: 'টি বিল', collection: 'ta_bills', orderByField: 'saved_at', countField: null,
        },
        {
            tag: 'license-forwarding', icon: '📜', title: 'লাইসেন্স ফরোয়ার্ডিং এন্ট্রি',
            body: 'লাইসেন্স ফরওয়ার্ডিং এন্ট্রি পর্যালোচনা করুন।',
            countLabel: 'টি এন্ট্রি', collection: 'license_archive', countField: null,
        },
        {
            tag: 'premium-submit', icon: '✅', title: 'প্রিমিয়াম জমা',
            body: 'প্রিমিয়াম জমার রেকর্ড পর্যালোচনা করুন।',
            countLabel: 'টি প্রিমিয়াম এন্ট্রি',
            collection: 'accounts', extraCollections: ['accounts_archive_1st_year'], countField: null,
        },
    ],

    18: [
        {
            tag: 'personal-dues', icon: '💳', title: 'ব্যক্তিগত বকেয়া',
            body: 'ব্যক্তিগত বকেয়ার তালিকা পর্যালোচনা করুন।',
            countLabel: 'টি বকেয়া এন্ট্রি', collection: 'personal_dues',
        },
        {
            tag: 'personal-expense', icon: '💸', title: 'ব্যক্তিগত খরচ',
            body: 'আজকের ব্যক্তিগত খরচের হিসাব দেখুন।',
            countLabel: 'টি খরচ এন্ট্রি', collection: 'personal_expenses', countField: null,
        },
    ],

    20: [
        {
            tag: 'donation', icon: '🤲', title: 'অনুদান',
            body: 'দান ও চাঁদার রেকর্ড পর্যালোচনা করুন।',
            countLabel: 'টি রেকর্ড', collection: 'donations', countField: null,
        },
        { tag: 'help', icon: '❓', title: 'সহায়তা', body: 'সহায়তা তালিকা পর্যালোচনা করুন।', staticOnly: true },
        { tag: 'help-requisition', icon: '📝', title: 'চাহিদা তালিকা ও ফিক্স নোট', body: 'স্টেশনারী চাহিদা ও ফিক্স নোট পর্যালোচনা করুন।', staticOnly: true },
    ],

    22: [
        {
            tag: 'proposal-index', icon: '📑', title: 'প্রস্তাবপত্র রেজিস্টার',
            body: 'প্রস্তাবপত্র রেজিস্টার পর্যালোচনা করুন।',
            countLabel: 'টি প্রস্তাবপত্র', collection: 'proposals', countField: null,
        },
        {
            tag: 'fpr-register', icon: '📒', title: 'এফপিআর রেজিস্টার',
            body: 'FPR রেজিস্টার পর্যালোচনা করুন।',
            countLabel: 'টি FPR এন্ট্রি', collection: 'fprEntries', countField: null,
        },
        {
            tag: 'license-archive', icon: '🗃️', title: 'লাইসেন্স ফরোয়ার্ডিং আর্কাইভ',
            body: 'এজেন্সি নিবন্ধন আর্কাইভ পর্যালোচনা করুন।',
            countLabel: 'টি আর্কাইভ এন্ট্রি', collection: 'license_archive', countField: null,
        },
    ],
};

// ══════════════════════════════════════════════════════════════
// LOGGING SYSTEM  (unchanged)
// ══════════════════════════════════════════════════════════════

class Logger {
    constructor(name = 'PUSH-WORKER') {
        this.name    = name;
        this.logsDir = path.join(__dirname, 'logs');
        this._ensureLogsDir();
    }

    _ensureLogsDir() {
        try {
            if (!fs.existsSync(this.logsDir)) fs.mkdirSync(this.logsDir, { recursive: true });
        } catch (err) {
            console.warn('⚠️ Failed to create logs directory:', err.message);
        }
    }

    _write(level, message, data = {}) {
        const entry = { timestamp: new Date().toISOString(), level, name: this.name, message, ...data };
        const prefix = `[${entry.timestamp}] [${level}] [${this.name}]`;
        if (level === 'ERROR') {
            console.error(prefix, message, data.errorMessage || '');
            if (data.errorStack) console.error('Stack:', data.errorStack);
        } else if (level === 'WARN') {
            console.warn(prefix, message, data);
        } else {
            console.log(prefix, message, Object.keys(data).length > 0 ? data : '');
        }
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
// SUPABASE INITIALIZATION
// Replaces: initializeFirebase() + admin.initializeApp()
// The service-role key bypasses RLS so the worker can read/write
// any row without being authenticated as a specific user.
// ══════════════════════════════════════════════════════════════

let _supabase = null;

function getDB() {
    if (!_supabase) throw new Error('Supabase not initialised. Call initializeSupabase() first.');
    return _supabase;
}

// Must match ADMIN_UID in app-backend.js
const ADMIN_UID = 'd4831f3c-2cd8-4598-9acf-fe509614c3af';

function initializeSupabase() {
    if (!CONFIG.SUPABASE_URL || !CONFIG.SUPABASE_SERVICE_ROLE_KEY) {
        logger.error('Supabase credentials incomplete', null, {
            hasUrl:            !!CONFIG.SUPABASE_URL,
            hasServiceRoleKey: !!CONFIG.SUPABASE_SERVICE_ROLE_KEY,
        });
        process.exit(1);
    }

    _supabase = createClient(CONFIG.SUPABASE_URL, CONFIG.SUPABASE_SERVICE_ROLE_KEY, {
        auth:     { persistSession: false },
        realtime: { transport: ws },
    });

    logger.info('✅ Supabase initialised', { url: CONFIG.SUPABASE_URL });
}

// ══════════════════════════════════════════════════════════════
// WEB PUSH INITIALIZATION  (unchanged)
// ══════════════════════════════════════════════════════════════

function initializeWebPush() {
    if (!CONFIG.VAPID_PUBLIC_KEY || !CONFIG.VAPID_PRIVATE_KEY) {
        logger.error('VAPID keys missing', null, {
            hasPublicKey:  !!CONFIG.VAPID_PUBLIC_KEY,
            hasPrivateKey: !!CONFIG.VAPID_PRIVATE_KEY,
        });
        process.exit(1);
    }
    webpush.setVapidDetails(CONFIG.VAPID_SUBJECT, CONFIG.VAPID_PUBLIC_KEY, CONFIG.VAPID_PRIVATE_KEY);
    logger.info('✅ Web Push VAPID configured', { subject: CONFIG.VAPID_SUBJECT });
}

// ══════════════════════════════════════════════════════════════
// TIME UTILITIES  (unchanged)
// ══════════════════════════════════════════════════════════════

function getDhakaHour() {
    return (new Date().getUTCHours() + 6) % 24;
}

function resolveSlotHour(dhakaHour) {
    if (HOUR_SCHEDULE[dhakaHour])                    return dhakaHour;
    const next = (dhakaHour + 1) % 24;
    if (HOUR_SCHEDULE[next])                         return next;
    return null;
}

function getTodayDhaka() {
    const dhaka = new Date(Date.now() + 6 * 3600 * 1000);
    return dhaka.getUTCFullYear() + '-' +
        String(dhaka.getUTCMonth() + 1).padStart(2, '0') + '-' +
        String(dhaka.getUTCDate()).padStart(2, '0');
}

/** Convert an ISO timestamp string or Date to a Dhaka date string. */
function taskDateToDhaka(isoOrDate) {
    if (!isoOrDate) return null;
    try {
        const d     = new Date(isoOrDate);
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
// STATUS KEY  (unchanged)
// ══════════════════════════════════════════════════════════════

function buildStatusKey(tag, pendingTasks) {
    if (tag !== 'task-manager') return `module_${tag}_${getTodayDhaka()}`;
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
// Replaces: Firestore pushState/seenToday document
// Now:      push_state table  (uid PK, date, keys[], snoozed jsonb)
// ══════════════════════════════════════════════════════════════

/** In-memory check — no DB read (uses pre-fetched row data). */
function _checkSeenOrSnoozedFromData(seenRow, statusKey) {
    try {
        const today = getTodayDhaka();
        if (!seenRow || seenRow.date !== today) return { skip: false };
        if (Array.isArray(seenRow.keys) && seenRow.keys.includes(statusKey))
            return { skip: true, reason: 'Already sent today' };
        if (seenRow.snoozed && seenRow.snoozed[statusKey]) {
            const until = seenRow.snoozed[statusKey];
            if (Date.now() < until)
                return { skip: true, reason: `Snoozed until ${new Date(until).toLocaleTimeString()}` };
        }
        return { skip: false };
    } catch (err) {
        logger.warn('_checkSeenOrSnoozedFromData error — proceeding', { statusKey, error: err.message });
        return { skip: false };
    }
}

async function checkSeenOrSnoozed(uid, statusKey) {
    try {
        const { data, error } = await getDB()
            .from('push_state')
            .select('date, keys, snoozed')
            .eq('uid', uid)
            .maybeSingle();
        if (error) throw error;
        return _checkSeenOrSnoozedFromData(data, statusKey);
    } catch (err) {
        logger.warn('checkSeenOrSnoozed failed — proceeding', { uid, statusKey, error: err.message });
        return { skip: false };
    }
}

async function markSeenToday(uid, statusKey) {
    try {
        const today = getTodayDhaka();

        // Fetch current row first so we can merge keys[] correctly
        const { data: existing } = await getDB()
            .from('push_state')
            .select('date, keys, snoozed')
            .eq('uid', uid)
            .maybeSingle();

        const sameDay = existing && existing.date === today;
        const keys    = sameDay ? (existing.keys || []) : [];
        const snoozed = sameDay ? (existing.snoozed || {}) : {};

        if (!keys.includes(statusKey)) {
            const trimmed = keys.length >= 200 ? keys.slice(-199) : keys;
            trimmed.push(statusKey);

            const { error } = await getDB()
                .from('push_state')
                .upsert({ uid, date: today, keys: trimmed, snoozed }, { onConflict: 'uid' });
            if (error) throw error;
        }
    } catch (err) {
        logger.warn('markSeenToday failed', { uid, statusKey, error: err.message });
    }
}

// ══════════════════════════════════════════════════════════════
// MARK TASK NOTIFICATIONS AS PUSHED
// Replaces: Firestore notifications sub-collection update
// Now:      UPDATE notifications SET pushed=true WHERE uid=$1
//           AND pushed=false AND tag='task-manager'
//           AND timestamp >= (now - 2min)
// ══════════════════════════════════════════════════════════════

async function markTaskNotificationsAsPushed(uid) {
    try {
        const twoMinutesAgo = new Date(Date.now() - 2 * 60 * 1000).toISOString();

        const { data, error } = await getDB()
            .from('notifications')
            .select('id, timestamp')
            .eq('uid', uid)
            .eq('pushed', false)
            .eq('tag', 'task-manager')
            .gte('timestamp', twoMinutesAgo);
        if (error) throw error;

        if (!data || data.length === 0) return 0;

        const ids = data.map(r => r.id);
        const { error: updateErr } = await getDB()
            .from('notifications')
            .update({ pushed: true, pushed_at: new Date().toISOString() })
            .in('id', ids);
        if (updateErr) throw updateErr;

        if (ids.length > 0) logger.debug(`Marked ${ids.length} task notifications as pushed`, { uid });
        return ids.length;
    } catch (err) {
        logger.warn('markTaskNotificationsAsPushed failed', { uid, error: err.message });
        return 0;
    }
}

// ══════════════════════════════════════════════════════════════
// COLLECTION RECORD COUNT + PREVIEW
// Replaces: Firestore .count().get() + .orderBy().limit().get()
// Now:      Supabase count:exact head query + select with order+limit
//
// Single-doc modules: reads user_data table (uid, key, data jsonb)
// Multi-collection:   iterates tables with extraCollections list
// Normal collection:  queries the named table with uid filter
// ══════════════════════════════════════════════════════════════

const PREVIEW_LIMIT = 5;

async function getCollectionData(uid, module) {
    const { collection: tableName, statusFilter, extraCollections,
            isSingleDoc, docKey, arrayFields, previewArray, previewField,
            orderByField } = module;

    try {

        // ── 1. Single-document source (manpower, policy-files) ───
        // Stored in user_data table as (uid, key, data jsonb)
        if (isSingleDoc && docKey) {
            const { data: row, error } = await getDB()
                .from('user_data')
                .select('data')
                .eq('uid', uid)
                .eq('key', docKey)
                .maybeSingle();
            if (error) throw error;
            if (!row) return { count: 0, docs: [] };

            const docData = row.data || {};
            const fields  = arrayFields || Object.keys(docData).filter(k => Array.isArray(docData[k]));
            const count   = fields.reduce((sum, f) => sum + (Array.isArray(docData[f]) ? docData[f].length : 0), 0);

            const src   = docData[previewArray] || docData[fields[0]] || [];
            const field = previewField || 'name';
            const docs  = src
                .filter(item => item && item[field])
                .slice(0, PREVIEW_LIMIT)
                .map(item => ({ _id: item.id || '', ...item }));

            return { count, docs };
        }

        // ── 2. Multi-collection (premium-submit spans 2 tables) ──
        if (extraCollections && extraCollections.length > 0) {
            const allTables = [tableName, ...extraCollections];
            let totalCount  = 0;
            let allDocs     = [];

            for (const tbl of allTables) {
                try {
                    // COUNT
                    const { count, error: cErr } = await getDB()
                        .from(tbl)
                        .select('*', { count: 'exact', head: true })
                        .eq('uid', uid);
                    if (!cErr && count != null) totalCount += count;

                    // PREVIEW — try common timestamp columns
                    let snap = null;
                    for (const col of ['timestamp', 'sent_at', 'created_at', 'date']) {
                        const { data, error } = await getDB()
                            .from(tbl)
                            .select('*')
                            .eq('uid', uid)
                            .order(col, { ascending: false })
                            .limit(PREVIEW_LIMIT);
                        if (!error && data) { snap = data; break; }
                    }
                    if (!snap) {
                        const { data } = await getDB().from(tbl).select('*').eq('uid', uid).limit(PREVIEW_LIMIT);
                        snap = data || [];
                    }
                    allDocs.push(...snap.map(r => ({ _col: tbl, ...r })));
                } catch (_) {}
            }

            allDocs.sort((a, b) => {
                const ta = a.timestamp ? new Date(a.timestamp).getTime() : 0;
                const tb = b.timestamp ? new Date(b.timestamp).getTime() : 0;
                return tb - ta;
            });
            return { count: totalCount, docs: allDocs.slice(0, PREVIEW_LIMIT) };
        }

        // ── 3. Normal table ───────────────────────────────────────
        async function fetchQuery(filterFn) {
            // COUNT via head query
            let total = null;
            try {
                const q = filterFn(getDB().from(tableName).select('*', { count: 'exact', head: true }));
                const { count, error } = await q;
                if (!error) total = count;
            } catch (_) {}

            // PREVIEW — try columns in order
            const orderCols = orderByField
                ? [orderByField, 'timestamp', 'sent_at', 'created_at', 'date']
                : ['timestamp', 'sent_at', 'created_at', 'date'];

            let snap = null;
            for (const col of orderCols) {
                try {
                    const { data, error } = await filterFn(
                        getDB().from(tableName).select('*')
                    ).order(col, { ascending: false }).limit(PREVIEW_LIMIT);
                    if (!error && data) { snap = data; break; }
                } catch (_) {}
            }
            if (!snap) {
                try {
                    const { data } = await filterFn(
                        getDB().from(tableName).select('*')
                    ).limit(PREVIEW_LIMIT);
                    snap = data || [];
                } catch (_) { snap = []; }
            }

            const docs = snap || [];
            if (total === null) total = docs.length;
            return { count: total, docs };
        }

        const baseFilter = q => q.eq('uid', uid);

        if (statusFilter) {
            const filteredFilter = q => baseFilter(q).eq(statusFilter.field, statusFilter.value);
            let result;
            try {
                result = await fetchQuery(filteredFilter);
            } catch (e) {
                logger.warn('statusFilter query failed — using total', { uid, tableName, error: e.message });
                return await fetchQuery(baseFilter);
            }
            if (result.count === 0) {
                const fallback = await fetchQuery(baseFilter);
                if (fallback.count > 0) {
                    logger.info(`statusFilter=0 but total=${fallback.count} — using total`, { uid, tableName });
                    return fallback;
                }
            }
            return result;
        }

        return await fetchQuery(baseFilter);

    } catch (err) {
        logger.warn('getCollectionData failed', { uid, module: tableName, error: err.message });
        return { count: null, docs: [] };
    }
}

// ══════════════════════════════════════════════════════════════
// PUSH PAYLOAD BUILDERS  (unchanged)
// ══════════════════════════════════════════════════════════════

const _PREVIEW_FIELDS = {
    'advance-payment':    ['name', 'code', 'type'],
    'manpower':           ['name'],
    'policy-files':       ['policyNo', 'clientName', 'name'],
    'fund-archive':       ['type', 'amount', 'method'],
    'hbl-recovery':       ['customerName', 'accountNumber'],
    'vat-tax':            ['khat', 'desc', 'amount'],
    'office-issue':       ['description', 'issue', 'title'],
    'notesheet':          ['subject', 'date'],
    'transport-bill':     ['name', 'billDate'],
    'business-stats':     ['meta'],
    'stationary-item':    ['smarok', 'date'],
    'medical-bill':       ['date', 'totalEntries', 'totalAmount'],
    'ta-bill':            ['travelerName', 'memoDate'],
    'license-forwarding': ['dateCreated', 'totalAgents'],
    'premium-submit':     ['typeDisplay', 'deposit', 'amount'],
    'personal-dues':      ['name', 'desc'],
    'personal-expense':   ['description'],
    'donation':           ['name', 'code', 'type'],
    'help':               ['title', 'description'],
    'help-requisition':   ['name', 'itemName'],
    'proposal-index':     ['name', 'proposalNo'],
    'fpr-register':       ['propName', 'name'],
    'license-archive':    ['dateCreated', 'totalAgents'],
};

function _docPreviewLine(docData, moduleTag) {
    if (!docData) return null;

    if (moduleTag === 'business-stats') {
        const meta = docData.meta || {};
        return ([meta.inchargeName, meta.reportDate].filter(Boolean).join(' — ') || null)?.slice(0, 40) ?? null;
    }
    if (moduleTag === 'medical-bill') {
        const parts = [
            docData.date,
            docData.totalEntries != null ? `${docData.totalEntries}টি এন্ট্রি` : '',
            docData.totalAmount  != null ? `৳${docData.totalAmount}` : '',
        ].filter(Boolean);
        return (parts.join(' — ') || null)?.slice(0, 40) ?? null;
    }
    if (moduleTag === 'fund-archive') {
        const typeLabel = { opening: 'জের', fund: 'ফান্ড', expense: 'খরচ' }[docData.type] || docData.type || '';
        return ([typeLabel, docData.amount != null ? `৳${docData.amount}` : '', docData.method || '']
            .filter(Boolean).join(' ') || null)?.slice(0, 40) ?? null;
    }
    if (moduleTag === 'premium-submit') {
        const type = docData.typeDisplay || docData.type || '';
        const dep  = docData.deposit != null ? `৳${docData.deposit}` : (docData.amount != null ? `৳${docData.amount}` : '');
        return ([type, dep].filter(Boolean).join(' ') || null)?.slice(0, 40) ?? null;
    }

    const fields = _PREVIEW_FIELDS[moduleTag] || ['name', 'title', 'description'];
    const all    = [...new Set([...fields, 'name', 'title', 'description'])];
    for (const f of all) {
        const v = docData[f];
        if (v && typeof v === 'string' && v.trim()) return v.trim().slice(0, 40);
        if (v && typeof v === 'number')              return String(v);
    }
    return null;
}

function buildRichBody(module, count, docs) {
    if (module.staticOnly || !module.collection) return module.body || `${module.title} পর্যালোচনা করুন।`;
    if (!count || count === 0 || !docs || docs.length === 0)
        return (module.body || `${module.title} পর্যালোচনা করুন।`) + ' নতুন এন্ট্রি যোগ করবেন?';

    const previews = [];
    for (const d of docs.slice(0, 3)) {
        const line = _docPreviewLine(d, module.tag);
        if (line && !previews.includes(line)) previews.push(line);
        if (previews.length >= 3) break;
    }

    const countLabel = module.countLabel || 'টি এন্ট্রি';
    return previews.length > 0
        ? `${count} ${countLabel}: ${previews.join(', ')} — নতুন এন্ট্রি যোগ করবেন?`
        : `${count} ${countLabel} আছে। পর্যালোচনা করুন।`;
}

function buildTaskReminderPayload(statusKey, pendingTasks, uid, deviceName) {
    const firstTitle = pendingTasks[0]?.title || 'কোনো শিরোনাম নেই';
    const remaining  = Math.max(0, pendingTasks.length - 1);
    const remainText = remaining > 0 ? ` এবং আরও ${remaining} টি` : '';
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
            statusKey, uid,
            supabaseUrl:   CONFIG.SUPABASE_URL,   // replaces projectId + apiKey
            deviceName:    deviceName || '',
            type:          'task-reminder',
            taskCount:     pendingTasks.length,
            firstTask:     firstTitle,
            url:           'https://officemanagement.app/dashboard.html',
        },
        actions: [
            { action: 'open',      title: '📋 দেখুন'        },
            { action: 'snooze',    title: '⏰ ১ ঘণ্টা পরে'  },
            { action: 'mark_read', title: '✅ দেখা হয়েছে'   },
            { action: 'dismiss',   title: '✕ বন্ধ করুন'     },
        ],
    };
}

function buildModulePayload(module, statusKey, uid, count, docs, deviceName) {
    return {
        title:              `${module.icon} ${module.title}`,
        body:               buildRichBody(module, count, docs),
        icon:               'https://raw.githubusercontent.com/sifatahmedpro/PettyCash-Statement/main/logo.png',
        badge:              'https://raw.githubusercontent.com/sifatahmedpro/PettyCash-Statement/main/logo.png',
        tag:                `notif-${module.tag}-${statusKey}`,
        requireInteraction: false,
        vibrate:            [100, 50, 100],
        timestamp:          Date.now(),
        data: {
            statusKey, uid, notifId: null,
            supabaseUrl:   CONFIG.SUPABASE_URL,   // replaces projectId + apiKey
            deviceName:    deviceName || '',
            type:          'module-notif',
            tag:           module.tag,
            url:           'https://officemanagement.app/dashboard.html',
        },
        actions: [
            { action: 'open',   title: '📂 দেখুন'       },
            { action: 'snooze', title: '⏰ ১ ঘণ্টা পরে' },
        ],
    };
}

// ══════════════════════════════════════════════════════════════
// SEND WITH RETRY  (unchanged)
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
                logger.warn(`Push failed (attempt ${attempt}), retrying in ${delayMs}ms`, { device: deviceName, error: err.message });
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
// Replaces: Firestore pushSubscriptions sub-collection delete
// Now:      DELETE FROM push_subscriptions WHERE uid=$1
//           AND endpoint = ANY($2)
// ══════════════════════════════════════════════════════════════

async function cleanupExpiredSubscriptions(uid, expiredEndpoints) {
    if (!expiredEndpoints.length) return 0;
    try {
        const { data, error } = await getDB()
            .from('push_subscriptions')
            .delete()
            .eq('uid', uid)
            .in('endpoint', expiredEndpoints)
            .select('id');
        if (error) throw error;
        const deleted = data?.length || 0;
        if (deleted > 0) logger.info(`Deleted ${deleted} expired subscription(s)`, { uid });
        return deleted;
    } catch (err) {
        logger.warn('cleanupExpiredSubscriptions failed', { uid, error: err.message });
        return 0;
    }
}

// ══════════════════════════════════════════════════════════════
// RESET SEEN-TODAY STATE FOR ALL USERS
// Replaces: delete Firestore pushState/seenToday document per user
// Now:      DELETE FROM push_state WHERE uid = ANY($1)
// ══════════════════════════════════════════════════════════════

async function resetSeenTodayForAllUsers(uids) {
    let reset = 0;
    for (const uid of uids) {
        try {
            const { error } = await getDB()
                .from('push_state')
                .delete()
                .eq('uid', uid);
            if (error) throw error;
            reset++;
        } catch (err) {
            logger.warn('resetSeenToday failed for user', { uid, error: err.message });
        }
    }
    logger.info(`🔄 Seen-today state cleared for ${reset} user(s).`);
    return reset;
}

// ══════════════════════════════════════════════════════════════
// WRITE PUSH DELIVERY LOG
// Replaces: Firestore pushLogs + pushRunLogs addDoc
// Now:      INSERT INTO push_logs and push_run_logs
// ══════════════════════════════════════════════════════════════

async function writePushLog(uid, module, result, count, statusKey, slotHour, deviceResults) {
    try {
        const tag   = module.tag;
        const icon  = module.icon  || '🔔';
        const label = module.isTaskReminder ? 'টাস্ক ম্যানেজার' : (module.title || tag);

        let detail = '';
        if (module.isTaskReminder) {
            detail = `টাস্ক রিমাইন্ডার — ঘণ্টা ${slotHour}:০০`;
        } else if (count !== null && count !== undefined && module.countLabel) {
            detail = `${count} ${module.countLabel} — ঘণ্টা ${slotHour}:০০ ঢাকা`;
        } else {
            detail = `নির্ধারিত রিমাইন্ডার — ঘণ্টা ${slotHour}:০০ ঢাকা`;
        }

        const devices  = Array.isArray(deviceResults)
            ? deviceResults.map(d => ({ name: d.name, status: d.status }))
            : [];

        const logEntry = {
            uid, tag, label, icon,
            status:       result.errors > 0 && result.pushed === 0 ? 'failed' : 'sent',
            sent_at:      new Date().toISOString(),
            detail,
            sent:         result.pushed,
            failed:       result.errors,
            skipped:      result.skipped,
            slot_hour:    slotHour,
            status_key:   statusKey,
            run_id:       CONFIG.RUN_ID,
            record_count: count ?? null,
            devices,
        };

        // 1. Per-user log (uid = the actual user who received the push)
        const { error: e1 } = await getDB().from('push_logs').insert(logEntry);
        if (e1) throw e1;

        // 2. Global run log (admin summary view)
        // FIX: must use ADMIN_UID so fetchPushRunLogs() (which queries by ADMIN_UID) can find these rows.
        // Previously used the user's uid here, causing push_run_logs to always return 0 rows on the log page.
        const { error: e2 } = await getDB().from('push_run_logs').insert({
            ...logEntry,
            uid: ADMIN_UID,
        });
        if (e2) logger.warn('push_run_logs insert failed (non-fatal)', { error: e2.message });

    } catch (err) {
        logger.warn('writePushLog failed (non-fatal)', { uid, tag: module.tag, error: err.message });
    }
}

// ══════════════════════════════════════════════════════════════
// FETCH ALL PUSH SUBSCRIPTIONS FOR A USER
// Replaces: Firestore pushSubscriptions .get()
// Now:      SELECT * FROM push_subscriptions WHERE uid=$1
// ══════════════════════════════════════════════════════════════

async function getUserSubscriptions(uid) {
    try {
        const { data, error } = await getDB()
            .from('push_subscriptions')
            .select('*')
            .eq('uid', uid);
        if (error) throw error;

        const all   = data || [];
        const valid = [];

        for (const s of all) {
            if (!s.endpoint) {
                logger.warn('Subscription missing endpoint — skipping', { uid, id: s.id });
                continue;
            }
            // app-backend.js saves keys as flat columns (p256dh, auth_key) rather than
            // a nested JSONB `keys` object. Support both layouts so this worker works
            // with the current schema and any future migration to a JSONB `keys` column.
            const p256dh  = s.keys?.p256dh || s.p256dh   || null;
            const authKey = s.keys?.auth   || s.auth_key  || null;

            if (!p256dh || !authKey) {
                logger.warn('Subscription missing p256dh / auth_key — SKIPPING. ' +
                    'User must re-enable push notifications in browser to fix.', {
                    uid, id: s.id,
                    hasP256dh: !!p256dh,
                    hasAuth:   !!authKey,
                    endpoint:  s.endpoint.slice(0, 40),
                });
                continue;
            }
            // Reconstruct the shape web-push expects: { endpoint, keys: { p256dh, auth } }
            valid.push({
                endpoint:   s.endpoint,
                keys:       { p256dh, auth: authKey },
                deviceName: s.device_name || 'Unknown Device',
            });
        }

        logger.debug(`getUserSubscriptions: ${all.length} docs, ${valid.length} usable`, { uid });
        return valid;
    } catch (err) {
        logger.warn('getUserSubscriptions failed', { uid, error: err.message });
        return [];
    }
}

// ══════════════════════════════════════════════════════════════
// SEND ONE MODULE NOTIFICATION  (logic unchanged)
// ══════════════════════════════════════════════════════════════

async function sendModuleNotificationToUser(uid, module, subscriptions, slotHour = null, seenRow = null) {
    const result    = { pushed: 0, skipped: 0, errors: 0, expiredEndpoints: [] };
    const statusKey = buildStatusKey(module.tag, null);

    const seenCheck = seenRow !== null
        ? _checkSeenOrSnoozedFromData(seenRow, statusKey)
        : await checkSeenOrSnoozed(uid, statusKey);

    if (seenCheck.skip) {
        logger.debug(`Module ${module.tag} skipped for ${uid}: ${seenCheck.reason}`);
        result.skipped++;
        return result;
    }

    let count = null;
    let docs  = [];
    if (!module.staticOnly && (module.collection || module.isSingleDoc)) {
        const data = await getCollectionData(uid, module);
        count = data.count;
        docs  = data.docs;
    }

    const deviceResults = [];
    for (const sub of subscriptions) {
        const payload = buildModulePayload(module, statusKey, uid, count, docs, sub.deviceName);
        const dName   = sub.deviceName || 'Unknown Device';

        if (CONFIG.DRY_RUN) {
            logger.info(`🧪 DRY RUN: would send [${module.tag}]`, { uid, device: dName, count });
            result.pushed++;
            deviceResults.push({ name: dName, status: 'sent' });
            continue;
        }

        const res = await sendWebPushWithRetry(sub, payload);
        if (res.success) {
            result.pushed++;
            deviceResults.push({ name: dName, status: 'sent' });
        } else if (res.expired) {
            result.expiredEndpoints.push(res.endpoint);
            result.errors++;
            deviceResults.push({ name: dName, status: 'failed' });
        } else {
            result.errors++;
            deviceResults.push({ name: dName, status: 'failed' });
        }
    }

    // FIX: write a log entry for every module attempt — not just when pushes succeed.
    // Previously if all devices failed (errors > 0, pushed = 0) nothing was logged.
    // Now we log 'sent', 'failed', or 'skipped' for every attempt so the run-log
    // page always shows a complete record of what the worker did.
    if (!CONFIG.DRY_RUN) {
        if (result.pushed > 0 || result.errors > 0) {
            if (result.pushed > 0) await markSeenToday(uid, statusKey);
            await writePushLog(uid, module, result, count, statusKey, slotHour, deviceResults);
        } else if (result.skipped === 0) {
            // No subscriptions reached but also not already counted as skipped above —
            // write a skipped entry so the slot is visible in the log.
            await writePushLog(uid, module, { pushed: 0, skipped: 1, errors: 0 },
                count, statusKey, slotHour, []);
        }
    }

    return result;
}

// ══════════════════════════════════════════════════════════════
// SEND TASK REMINDER  (logic unchanged)
// Replaces: Firestore tasks sub-collection .where('status','!=','done')
// Now:      SELECT * FROM tasks WHERE uid=$1 AND status != 'done'
// ══════════════════════════════════════════════════════════════

async function sendTaskReminderToUser(uid, today, subscriptions) {
    const result = { pushed: 0, skipped: 0, errors: 0, expiredEndpoints: [] };

    let pendingTasks = [];
    try {
        const { data, error } = await getDB()
            .from('tasks')
            .select('title, status, date')
            .eq('uid', uid)
            .neq('status', 'done');
        if (error) throw error;

        for (const task of (data || [])) {
            if (!task.date) {
                pendingTasks.push({ title: task.title || 'Untitled Task' });
                continue;
            }
            const taskDate = taskDateToDhaka(task.date);
            if (taskDate && taskDate <= today) {
                pendingTasks.push({ title: task.title || 'Untitled Task' });
            }
        }
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

    const deviceResults = [];
    for (const sub of subscriptions) {
        const payload = buildTaskReminderPayload(statusKey, pendingTasks, uid, sub.deviceName);
        const dName   = sub.deviceName || 'Unknown Device';

        if (CONFIG.DRY_RUN) {
            logger.info('🧪 DRY RUN: would send task reminder', { uid, device: dName, taskCount: pendingTasks.length });
            result.pushed++;
            deviceResults.push({ name: dName, status: 'sent' });
            continue;
        }

        const res = await sendWebPushWithRetry(sub, payload);
        if (res.success) {
            result.pushed++;
            deviceResults.push({ name: dName, status: 'sent' });
        } else if (res.expired) {
            result.expiredEndpoints.push(res.endpoint);
            result.errors++;
            deviceResults.push({ name: dName, status: 'failed' });
        } else {
            result.errors++;
            deviceResults.push({ name: dName, status: 'failed' });
        }
    }

    if ((result.pushed > 0 || result.errors > 0) && !CONFIG.DRY_RUN) {
        if (result.pushed > 0) await markSeenToday(uid, statusKey);
        if (result.pushed > 0) await markTaskNotificationsAsPushed(uid);
        const taskModule = { tag: 'task-manager', icon: '📋', title: 'টাস্ক ম্যানেজার', isTaskReminder: true };
        await writePushLog(uid, taskModule, result, pendingTasks.length, statusKey, 10, deviceResults);
    }

    return result;
}

// ══════════════════════════════════════════════════════════════
// PROCESS ALL MODULES FOR ONE USER AT THE CURRENT HOUR SLOT
// ══════════════════════════════════════════════════════════════

async function processUserAtHour(uid, dhakaHour, today) {
    const stats = { pushed: 0, skipped: 0, errors: 0, expiredEndpoints: [] };

    const resolvedHour = resolveSlotHour(dhakaHour);
    const modules      = resolvedHour ? HOUR_SCHEDULE[resolvedHour] : null;
    if (!modules || modules.length === 0) {
        logger.debug(`No schedule for hour ${dhakaHour} (resolved: ${resolvedHour ?? 'none'})`, { uid });
        return stats;
    }

    // Pre-fetch seenToday row ONCE per user (saves N-1 DB reads per slot)
    let _seenRow = null;
    try {
        const { data, error } = await getDB()
            .from('push_state')
            .select('date, keys, snoozed')
            .eq('uid', uid)
            .maybeSingle();
        if (!error) _seenRow = data || {};
    } catch (err) {
        logger.warn('processUserAtHour: failed to pre-fetch push_state — will fall back per-module', { uid, error: err.message });
        _seenRow = null;
    }

    const subscriptions = await getUserSubscriptions(uid);

    if (subscriptions.length === 0) {
        logger.debug('No valid push subscriptions for user', { uid });

        // Write a skipped log for each module (Fix B — preserved)
        // FIX: also write to push_run_logs (uid=ADMIN_UID) so the global
        // run summary on the notification-log page is never 0 after a run.
        for (const module of modules) {
            try {
                const skippedEntry = {
                    uid,
                    tag:          module.tag,
                    label:        module.title || module.tag,
                    icon:         module.icon  || '🔔',
                    status:       'skipped',
                    sent_at:      new Date().toISOString(),
                    detail:       'কোনো বৈধ পুশ সাবস্ক্রিপশন নেই — ব্রাউজারে পুনরায় পুশ চালু করুন',
                    sent:         0,
                    failed:       0,
                    skipped:      1,
                    slot_hour:    resolvedHour,
                    status_key:   buildStatusKey(module.tag, null),
                    run_id:       CONFIG.RUN_ID,
                    record_count: null,
                    devices:      [],
                };
                await getDB().from('push_logs').insert(skippedEntry);
                // Global run log — must use ADMIN_UID (mirrors writePushLog fix)
                await getDB().from('push_run_logs').insert({ ...skippedEntry, uid: ADMIN_UID });
            } catch (logErr) {
                logger.warn('Failed to write skipped log for module', { uid, tag: module.tag, error: logErr.message });
            }
        }

        stats.skipped += modules.length;
        return stats;
    }

    for (const module of modules) {
        try {
            const result = module.isTaskReminder
                ? await sendTaskReminderToUser(uid, today, subscriptions)
                : await sendModuleNotificationToUser(uid, module, subscriptions, resolvedHour, _seenRow);

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
// Replaces: Firestore users collection .select().get()
// Now:      SELECT DISTINCT uid FROM push_subscriptions
//           (only users who have subscriptions need processing)
// ══════════════════════════════════════════════════════════════

async function runPushWorker() {
    const startTime    = Date.now();
    const dhakaHour    = getDhakaHour();
    const today        = getTodayDhaka();
    const isActive     = dhakaHour >= CONFIG.ACTIVE_HOUR_START && dhakaHour < CONFIG.ACTIVE_HOUR_END;
    const resolvedSlot = resolveSlotHour(dhakaHour);
    const hasSlot      = resolvedSlot !== null;

    if ((CONFIG.IS_MANUAL_TRIGGER || CONFIG.FORCE_SEND) && !CONFIG.RESET_SEEN_TODAY) {
        CONFIG.RESET_SEEN_TODAY = true;
        logger.info('ℹ️  Auto-enabled RESET_SEEN_TODAY for manual/force run.');
    }

    logger.info('🚀 Push worker started', {
        runId:          CONFIG.RUN_ID, runNumber: CONFIG.RUN_NUMBER,
        actor:          CONFIG.GITHUB_ACTOR, isManual: CONFIG.IS_MANUAL_TRIGGER,
        isDryRun:       CONFIG.DRY_RUN, forceSend: CONFIG.FORCE_SEND,
        resetSeenToday: CONFIG.RESET_SEEN_TODAY, dhakaHour, today, isActive, hasSlot,
    });

    if (!isActive && !CONFIG.FORCE_SEND && !CONFIG.IS_MANUAL_TRIGGER) {
        logger.warn('🔴 Resting hours — exiting silently', { dhakaHour });
        return { success: true, skipped: true, reason: 'Resting hours' };
    }

    if (!hasSlot && !CONFIG.FORCE_SEND && !CONFIG.IS_MANUAL_TRIGGER) {
        logger.info(`⏩ No schedule for hour ${dhakaHour} — nothing to send.`);
        return { success: true, skipped: true, reason: `No schedule for hour ${dhakaHour}` };
    }

    if (CONFIG.DRY_RUN) logger.info('🧪 DRY RUN MODE — no actual pushes will be sent');

    // ── Determine slot(s) to run ──────────────────────────────
    let slotsToRun;
    if (CONFIG.IS_MANUAL_TRIGGER && !CONFIG.FORCE_SEND) {
        slotsToRun = Object.keys(HOUR_SCHEDULE).map(Number).sort((a, b) => a - b);
        logger.info(`📋 Manual trigger: running ALL ${slotsToRun.length} slots → [${slotsToRun.join(', ')}]`);
    } else {
        const effectiveHour = resolvedSlot ?? (() => {
            const slots   = Object.keys(HOUR_SCHEDULE).map(Number).sort((a, b) => a - b);
            const earlier = slots.filter(h => h <= dhakaHour);
            return earlier.length ? earlier[earlier.length - 1] : slots[0];
        })();
        if (effectiveHour !== dhakaHour)
            logger.info(`Force/cron: using resolved slot (hour ${effectiveHour}) for Dhaka hour ${dhakaHour}`);
        slotsToRun = [effectiveHour];
    }

    const globalStats = { usersProcessed: 0, usersFailed: 0, pushSent: 0, pushSkipped: 0, errors: 0, subscriptionsCleaned: 0 };

    // ── Fetch all distinct UIDs that have push subscriptions ──
    // Replaces: Firestore users collection .select().get()
    // This is more efficient — only users with subscriptions matter.
    let uids;
    try {
        const { data, error } = await getDB()
            .from('push_subscriptions')
            .select('uid');
        if (error) throw error;
        // Deduplicate
        uids = [...new Set((data || []).map(r => r.uid))];
    } catch (err) {
        logger.error('Failed to fetch user UIDs from push_subscriptions', err);
        return { success: false, error: err.message, duration: Date.now() - startTime };
    }

    if (uids.length === 0) {
        logger.warn('⚠️ No users found with push subscriptions');
        return { success: true, stats: globalStats, duration: Date.now() - startTime };
    }

    if (CONFIG.RESET_SEEN_TODAY && !CONFIG.DRY_RUN) {
        await resetSeenTodayForAllUsers(uids);
    }

    const allExpiredEndpoints = new Set();

    for (const slotHour of slotsToRun) {
        logger.info(`📊 Processing ${uids.length} user(s) for hour ${slotHour}`, {
            modules: (HOUR_SCHEDULE[slotHour] || []).map(m => m.tag),
        });

        for (let i = 0; i < uids.length; i += CONFIG.BATCH_SIZE) {
            const batch = uids.slice(i, i + CONFIG.BATCH_SIZE);
            await Promise.all(batch.map(async uid => {
                try {
                    const result = await processUserAtHour(uid, slotHour, today);
                    globalStats.usersProcessed++;
                    globalStats.pushSent    += result.pushed;
                    globalStats.pushSkipped += result.skipped;
                    globalStats.errors      += result.errors;
                    result.expiredEndpoints.forEach(ep => allExpiredEndpoints.add(ep));
                } catch (err) {
                    logger.error('Failed to process user', err, { uid });
                    globalStats.usersFailed++;
                    globalStats.errors++;
                }
            }));
        }
    }

    // Cleanup expired subscriptions (once, after all users)
    const expiredList = Array.from(allExpiredEndpoints);
    if (expiredList.length > 0) {
        for (const uid of uids) {
            const cleaned = await cleanupExpiredSubscriptions(uid, expiredList);
            globalStats.subscriptionsCleaned += cleaned;
        }
    }

    const duration = Date.now() - startTime;
    logger.info('✅ Push worker completed', {
        ...globalStats, durationMs: duration,
        durationSec: Math.round(duration / 1000), slots: slotsToRun,
    });

    return { success: true, stats: globalStats, duration };
}

// ══════════════════════════════════════════════════════════════
// ENTRY POINT
// ══════════════════════════════════════════════════════════════

(async () => {
    try {
        logger.info('═══════════════════════════════════════════════════════════');
        logger.info('   অফিস ম্যানেজমেন্ট সিস্টেম — Push Notification Worker v6.0');
        logger.info('═══════════════════════════════════════════════════════════');

        initializeSupabase();
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
