/**
 * ============================================================
 * push-worker/send-push.js  —  v5.5
 * Standalone Push Notification Worker for GitHub Actions
 * Project : অফিস ম্যানেজমেন্ট সিস্টেম
 *
 * WHAT CHANGED in v5.5:
 *
 *   BUG FIXES from v5.4:
 *     [FIX-22] CRITICAL — premium-submit showed "০ টি প্রিমিয়াম এন্ট্রি"
 *              (confirmed in the Firestore push notification screenshot).
 *              The worker queried 5 non-existent collections:
 *              accounts_1st_year, accounts_renewal, accounts_deferred,
 *              accounts_mr, accounts_loan — none of which exist.
 *              The Firestore console screenshot shows the real collections
 *              under the user are 'accounts' (active entries) and
 *              'accountsArchive_1st_year' (archived entries).
 *              Fixed: collection → 'accounts',
 *                     extraCollections → ['accountsArchive_1st_year'].
 *
 *   UNCHANGED from v5.4:
 *     All v5.4 fixes (FIX-19 through FIX-21) are preserved.
 *     Cron schedule, payload builders, retry logic, and
 *     subscription cleanup are unchanged.
 *
 * WHAT CHANGED in v5.4:
 *
 *   BUG FIXES from v5.3:
 *     [FIX-19] vat-tax showed "0 টি পেমেন্ট রেকর্ড" because the worker
 *              queried 'vatTaxPayments' which doesn't exist.
 *              vat-tax-calculation.js paymentCollection() writes active
 *              records to 'vatPayments' (archived to 'vatPaymentsArchive').
 *              Fixed: 'vatTaxPayments' → 'vatPayments', added
 *              orderByField:'createdAt' to match the collection's index.
 *              Also fixed preview field: 'description' → 'desc' (the
 *              actual field name saved by addDoc: { khat, desc, amount }).
 *
 *     [FIX-20] help showed "0 টি আইটেম" because 'help' was queried as a
 *              collection but help-backend.js stores accordion data as a
 *              single document (not a queryable per-user collection).
 *              Fixed: switched to staticOnly:true — sends a plain reminder.
 *
 *     [FIX-21] help-requisition showed "0 টি আইটেম" because 'helpRequisition'
 *              doesn't exist. help-requisition.js writes to a ROOT-LEVEL
 *              collection 'requisition_{projectId}' that is shared across
 *              all users — it cannot be queried per-user by the push worker.
 *              Fixed: switched to staticOnly:true — sends a plain reminder.
 *
 *   UNCHANGED from v5.3:
 *     All v5.3 fixes (FIX-16 through FIX-18) are preserved.
 *     Cron schedule, payload builders, retry logic, and
 *     subscription cleanup are unchanged.
 *
 * WHAT CHANGED in v5.3:
 *
 *   BUG FIXES from v5.2:
 *     [FIX-16] office-issue showed "0 টি অমীমাংসিত সমস্যা" because
 *              the worker queried 'officeIssues' which doesn't exist.
 *              office-solution-backend.js _issuesPath() writes to
 *              'office_issues'. Fixed: 'officeIssues' → 'office_issues'.
 *
 *     [FIX-17] business-stats showed "0 টি রিপোর্ট" because the worker
 *              queried 'businessStats' which doesn't exist.
 *              business-statistics-backend.js BizStatDB.saveReportToArchive()
 *              writes to 'business_analysis_archives'. Fixed collection name
 *              and orderByField to 'meta.createdAt' (matches the archive
 *              query in the backend).
 *              Preview fields (inchargeName, reportDate) are nested inside
 *              a meta{} sub-object — added a business-stats special-case in
 *              _docPreviewLine that reads meta.inchargeName and meta.reportDate
 *              directly instead of searching root-level fields.
 *
 *     [FIX-18] fpr-register showed "0 টি FPR এন্ট্রি" because the worker
 *              queried 'fprRegister' which doesn't exist.
 *              fpr-register-backend.js FprDB._entriesPath() writes to
 *              'fprEntries'. Fixed: 'fprRegister' → 'fprEntries'.
 *              Preview fields ['propName', 'name'] were already correct
 *              (propName is the saved field per FprDB.saveEntry()).
 *
 *   UNCHANGED from v5.2:
 *     All v5.2 fixes (FIX-12 through FIX-15) are preserved.
 *     Cron schedule, payload builders, retry logic, and
 *     subscription cleanup are unchanged.
 *
 * WHAT CHANGED in v5.2:
 *
 *   BUG FIXES from v5.1:
 *     [FIX-12] policy-files showed "0 টি পলিসি" because the worker
 *              was querying a non-existent collection 'policyFiles'.
 *              policy-files-backend.js stores data as a SINGLE DOCUMENT
 *              at data/policy-files with general[] and monthly[] arrays.
 *              Fixed by switching to isSingleDoc:true with
 *              docPath:'data/policy-files', arrayFields:['general','monthly'],
 *              previewArray:'general', previewField:'policyNo' — matching
 *              exactly how PolicyFilesDB.loadActiveData() reads the data.
 *
 *     [FIX-13] medical-bill showed "0 টি রেকর্ড" because the worker
 *              was querying a non-existent collection 'medicalBills'.
 *              Active entries live in a single document at data/medical-bills
 *              (entries[] array). Archived reports live in the real collection
 *              archives/medical-bills/reports — fixed collection path to use
 *              the archive collection for count+preview.
 *              Preview fields updated to match actual archive doc shape:
 *              date, totalEntries, totalAmount (with a dedicated special-case
 *              in _docPreviewLine that formats them as a readable line).
 *
 *     [FIX-14] proposal-index showed "0 টি প্রস্তাবপত্র" because the worker
 *              used collection name 'proposalIndex'.
 *              proposal-index-backend.js (ProposalDB) writes to 'proposals'.
 *              Fixed: collection: 'proposalIndex' → 'proposals'.
 *
 *     [FIX-15] donation preview showed blank names because _PREVIEW_FIELDS
 *              listed 'donorName' which does not exist in donation documents.
 *              donation-backend.js (DonationDB.addDonation) saves: code, name,
 *              branch, type, amount, date. Fixed: removed 'donorName',
 *              kept ['name', 'code', 'type'] which always have values.
 *
 *   UNCHANGED from v5.1:
 *     All v5.1 fixes (FIX-8 through FIX-11) are preserved.
 *     Cron schedule, HOUR_SCHEDULE table, payload builders,
 *     retry logic, and subscription cleanup are unchanged.
 *
 * WHAT CHANGED in v5.1:
 *
 *   BUG FIXES from v5.0:
 *     [FIX-8] CRITICAL — Push notifications always showed "0 টি এন্ট্রি"
 *             because getCollectionData() used Admin SDK's count()
 *             aggregation which silently fails on many collection shapes.
 *             When it failed the code fell back to snap.size — but snap
 *             was a limit(3) query, so the count was capped at 3 at best
 *             and 0 at worst. Fixed by:
 *             (a) Multi-collection path (premium-submit etc.): replaced
 *                 count()+limit(3) with a full ref.get() for the count,
 *                 then a separate limited+ordered query for preview docs.
 *             (b) Normal collection path: same separation — full q.get()
 *                 for the accurate total, separate ordered limit() query
 *                 for the 5 preview docs shown in the notification body.
 *             (c) Admin SDK Timestamp fallback: sort now handles both
 *                 .toMillis() and .seconds so all modules sort correctly.
 *
 *     [FIX-9] Wrong Firestore collection names and preview field names
 *             for three modules — caused 0-count and blank previews:
 *             • hbl-recovery:     'hblRecovery'    → 'hbl_recovery_records'
 *                                 preview: removed wrong 'name' fallback,
 *                                 now uses 'customerName','accountNumber'
 *             • personal-dues:    'personalDues'   → 'personal_dues'
 *                                 preview: 'description' → 'desc'
 *                                 (actual field saved by the module)
 *             • personal-expense: 'personalExpenses'→ 'personal_expenses'
 *                                 preview: removed non-existent 'category'
 *
 *    [FIX-10] Wrong collection names and preview fields for three more
 *             modules (license-forwarding, license-archive, notesheet):
 *             • notesheet:          'notesheets'      → 'notesheetReports'
 *                                   preview: removed wrong 'title' fallback,
 *                                   now uses 'subject','date' (actual fields)
 *             • license-forwarding: 'licenseForwarding'→ 'license_archive'
 *                                   preview: 'agentName','agentCode' are
 *                                   nested inside rows[] sub-array — not
 *                                   accessible at doc root. Changed to
 *                                   top-level fields 'dateCreated','totalAgents'
 *             • license-archive:    'licenseArchive'  → 'license_archive'
 *                                   preview: same nested-rows issue as above,
 *                                   fixed identically to license-forwarding
 *
 *    [FIX-11] Wrong collection names/types for lunch-allowance,
 *             transport-bill, and stationary-item:
 *             • lunch-allowance:  'lunchAllowance' collection doesn't
 *                                 exist — data is stored as a SINGLE
 *                                 DOCUMENT at data/lunchAllowance per
 *                                 user. Changed to staticOnly:true so
 *                                 the worker sends a plain reminder
 *                                 instead of querying a missing collection.
 *             • transport-bill:   'transportBills' → 'transport-bill-archive'
 *                                 preview: removed wrong 'employeeName',
 *                                 now uses 'name','billDate' (actual fields)
 *             • stationary-item:  'stationaryItems' → 'stationary_reports'
 *                                 preview: removed non-existent 'description',
 *                                 now uses 'smarok','date' (actual fields)
 *
 *   UNCHANGED from v5.0:
 *     All v5.0 fixes (FIX-1 through FIX-7) are preserved.
 *     Cron schedule, HOUR_SCHEDULE table, payload builders,
 *     retry logic, and subscription cleanup are unchanged.
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
            collection: 'advance_payments',
            countField: null,              // count all docs
        },
        {
            tag:        'manpower',
            icon:       '👥',
            title:      'জনবল তথ্য',
            body:       'আজকের জনবল তালিকা পর্যালোচনা করুন।',
            countLabel: 'টি কর্মী',
            // Manpower is stored as a SINGLE DOCUMENT at data/manpower,
            // not a queryable collection. Fields: jbc[], dm[], do[], agent[].
            isSingleDoc: true,
            docPath:     'data/manpower',
            // Which arrays to sum for total count
            arrayFields: ['jbc', 'dm', 'do', 'agent'],
            // Which array to pull preview names from (agents have the most detail)
            previewArray: 'agent',
            previewField: 'name',
        },
        {
            tag:        'policy-files',
            icon:       '📂',
            title:      'পলিসি ফাইলসমূহ',
            body:       'পলিসি ফাইল তালিকা আপডেট করুন।',
            countLabel: 'টি পলিসি',
            // Data lives in ONE document (not a collection).
            // policy-files-backend.js stores general[] and monthly[] arrays
            // inside artifacts/.../users/{uid}/data/policy-files
            isSingleDoc:  true,
            docPath:      'data/policy-files',
            arrayFields:  ['general', 'monthly'],
            previewArray: 'general',
            previewField: 'policyNo',
        },
    ],

    8: [
        {
            tag:        'fund-archive',
            icon:       '🏦',
            title:      'ফান্ড আর্কাইভ',
            body:       'ফান্ড আর্কাইভ রেকর্ড পর্যালোচনা করুন।',
            countLabel: 'টি ফান্ড এন্ট্রি',
            // Real collection path: artifacts/.../users/{uid}/logs
            // Fields: type (opening/fund/expense), amount, method (Cash/Bank),
            //         date, checkNo, createdAt
            collection: 'logs',
            orderByField: 'createdAt',
        },
        {
            tag:        'hbl-recovery',
            icon:       '🏧',
            title:      'HBL রিকভারি',
            body:       'HBL রিকভারি রেকর্ড পর্যালোচনা করুন।',
            countLabel: 'টি রিকভারি রেকর্ড',
            collection: 'hbl_recovery_records',
            countField: null,
        },
        {
            tag:        'vat-tax',
            icon:       '🧾',
            title:      'ভ্যাট-ট্যাক্স হিসাব',
            body:       'ভ্যাট ও ট্যাক্স হিসাব পর্যালোচনা করুন।',
            countLabel: 'টি পেমেন্ট রেকর্ড',
            // vat-tax-calculation.js paymentCollection() writes to 'vatPayments'
            // (archived copy goes to 'vatPaymentsArchive').
            // Saved fields: date, khat, desc, chalan, amount, createdAt.
            collection: 'vatPayments',
            orderByField: 'createdAt',
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
            // office-solution-backend.js _issuesPath → 'office_issues'
            collection: 'office_issues',
        },
        {
            tag:        'notesheet',
            icon:       '📋',
            title:      'নোটশীট',
            body:       'নোটশীট রিপোর্ট পর্যালোচনা করুন।',
            countLabel: 'টি সক্রিয় নোটশীট',
            collection: 'notesheetReports',
            countField: null,
        },
    ],

    12: [
        {
            tag:        'lunch-allowance',
            icon:       '🍱',
            title:      'লাঞ্চ ভাতা',
            body:       'লাঞ্চ ভাতার রেকর্ড পর্যালোচনা করুন।',
            staticOnly: true,              // stored as a single document, not a queryable collection
        },
        {
            tag:        'transport-bill',
            icon:       '🚌',
            title:      'যাতায়াত বিল',
            body:       'যাতায়াত বিল আর্কাইভ পর্যালোচনা করুন।',
            countLabel: 'টি বিল রেকর্ড',
            collection: 'transport-bill-archive',
            countField: null,
        },
        {
            tag:        'business-stats',
            icon:       '📊',
            title:      'ব্যবসায়িক পরিসংখ্যান',
            body:       'ব্যবসায়িক পরিসংখ্যান রিপোর্ট দেখুন।',
            countLabel: 'টি রিপোর্ট',
            // business-statistics-backend.js BizStatDB.saveReportToArchive()
            // writes to 'business_analysis_archives'. Preview fields
            // (inchargeName, reportDate) are nested inside meta{} —
            // handled by the business-stats special-case in _docPreviewLine.
            collection: 'business_analysis_archives',
            orderByField: 'meta.createdAt',
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
            collection: 'stationary_reports',
            countField: null,
        },
        {
            tag:        'medical-bill',
            icon:       '🏥',
            title:      'মেডিকেল বিল',
            body:       'মেডিকেল বিল আর্কাইভ পর্যালোচনা করুন।',
            countLabel: 'টি রেকর্ড',
            // Active entries live in ONE document at data/medical-bills (entries[] array).
            // Archived reports live in the collection archives/medical-bills/reports.
            // We count+preview from the archive collection for the richest notification.
            collection: 'archives/medical-bills/reports',
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
            collection: 'ta_bills',
            orderByField: 'savedAt',
            countField: null,
        },
        {
            tag:        'license-forwarding',
            icon:       '📜',
            title:      'লাইসেন্স ফরওয়ার্ডিং',
            body:       'লাইসেন্স ফরওয়ার্ডিং এন্ট্রি পর্যালোচনা করুন।',
            countLabel: 'টি এন্ট্রি',
            collection: 'license_archive',
            countField: null,
        },
        {
            tag:        'premium-submit',
            icon:       '✅',
            title:      'প্রিমিয়াম জমা',
            body:       'প্রিমিয়াম জমার রেকর্ড পর্যালোচনা করুন।',
            countLabel: 'টি প্রিমিয়াম এন্ট্রি',
            // Firestore screenshot confirms real collections under the user are:
            //   'accounts'                 — active premium entries
            //   'accountsArchive_1st_year' — archived entries
            // All previous names (accounts_1st_year, accounts_renewal, etc.) were wrong.
            collection: 'accounts',
            extraCollections: ['accountsArchive_1st_year'],
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
            collection: 'personal_dues',
        },
        {
            tag:        'personal-expense',
            icon:       '💸',
            title:      'ব্যক্তিগত খরচ',
            body:       'আজকের ব্যক্তিগত খরচের হিসাব দেখুন।',
            countLabel: 'টি খরচ এন্ট্রি',
            collection: 'personal_expenses',
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
            tag:        'help',
            icon:       '❓',
            title:      'সহায়তা',
            body:       'সহায়তা তালিকা পর্যালোচনা করুন।',
            // help-backend.js stores accordion data as a single document,
            // not a queryable collection — send a plain reminder.
            staticOnly: true,
        },
        {
            tag:        'help-requisition',
            icon:       '📝',
            title:      'চাহিদা তালিকা ও ফিক্স নোট',
            body:       'স্টেশনারী চাহিদা ও ফিক্স নোট পর্যালোচনা করুন।',
            // help-requisition.js writes to a ROOT-LEVEL collection
            // 'requisition_{projectId}' (not per-user) — it cannot be
            // queried per-user by the push worker. Send a plain reminder.
            staticOnly: true,
        },
    ],

    22: [
        {
            tag:        'proposal-index',
            icon:       '📑',
            title:      'প্রস্তাবপত্র ইন্ডেক্স',
            body:       'প্রস্তাবপত্র রেজিস্টার পর্যালোচনা করুন।',
            countLabel: 'টি প্রস্তাবপত্র',
            // proposal-index-backend.js writes to the 'proposals' collection
            collection: 'proposals',
            countField: null,
        },
        {
            tag:        'fpr-register',
            icon:       '📒',
            title:      'FPR রেজিস্টার',
            body:       'FPR রেজিস্টার পর্যালোচনা করুন।',
            countLabel: 'টি FPR এন্ট্রি',
            // fpr-register-backend.js FprDB._entriesPath → 'fprEntries'
            collection: 'fprEntries',
            countField: null,
        },
        {
            tag:        'license-archive',
            icon:       '🗃️',
            title:      'লাইসেন্স আর্কাইভ',
            body:       'এজেন্সি নিবন্ধন আর্কাইভ পর্যালোচনা করুন।',
            countLabel: 'টি আর্কাইভ এন্ট্রি',
            collection: 'license_archive',
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
        if (!snap.exists) return { skip: false };

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
        const data  = snap.exists ? snap.data() : {};

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

/**
 * getCollectionData — unified data fetcher that handles three source types:
 *
 *  1. isSingleDoc  — data lives in ONE document (e.g. manpower at data/manpower).
 *                    Count = sum of named array field lengths.
 *                    Preview = first N items of a designated array.
 *
 *  2. extraCollections — aggregate across multiple sibling collections
 *                    (e.g. premium-submit spans 5 accounts_* tabs).
 *
 *  3. Normal collection — standard Firestore sub-collection.
 *                    Supports statusFilter with automatic fallback:
 *                    if filter returns 0 but total > 0 the filter field
 *                    may be misnamed — we return the unfiltered total.
 *
 * Returns { count: number|null, docs: Array<object> }
 */
async function getCollectionData(uid, module) {
    const { collection: collectionName, statusFilter, extraCollections,
            isSingleDoc, docPath, arrayFields, previewArray, previewField,
            orderByField } = module;

    const PREVIEW_LIMIT = 5;

    try {

        // ── 1. Single-document source (e.g. manpower) ────────────
        if (isSingleDoc && docPath) {
            const ref  = getDB().doc(`artifacts/default-app-id/users/${uid}/${docPath}`);
            const snap = await ref.get();
            if (!snap.exists) return { count: 0, docs: [] };

            const data = snap.data();

            // Sum all designated array field lengths for total count
            const fields = arrayFields || Object.keys(data).filter(k => Array.isArray(data[k]));
            const count  = fields.reduce((sum, f) => sum + (Array.isArray(data[f]) ? data[f].length : 0), 0);

            // Pull preview items from the designated array
            const src    = data[previewArray] || data[fields[0]] || [];
            const field  = previewField || 'name';
            const docs   = src
                .filter(item => item && item[field])
                .slice(0, PREVIEW_LIMIT)
                .map(item => ({ _id: item.id || '', ...item }));

            return { count, docs };
        }

        // ── 2. Multi-collection (e.g. premium-submit 5 tabs) ─────
        if (extraCollections && extraCollections.length > 0) {
            const allCols = [collectionName, ...extraCollections];
            let totalCount = 0;
            let allDocs    = [];

            for (const col of allCols) {
                try {
                    const ref = getDB().collection(`artifacts/default-app-id/users/${uid}/${col}`);

                    // COUNT: always do a full get() for accurate total —
                    // Admin SDK count() aggregation silently fails on some
                    // collection shapes, and limit(N).get().size only returns
                    // N at most, not the real total.
                    let count = 0;
                    try {
                        const countSnap = await ref.get();
                        count = countSnap.size;
                    } catch (_) {}

                    // PREVIEW: fetch latest N docs ordered by timestamp
                    let snap = null;
                    for (const field of ['timestamp', 'sentAt', 'createdAt', 'date']) {
                        try {
                            snap = await ref.orderBy(field, 'desc').limit(PREVIEW_LIMIT).get();
                            break;
                        } catch (_) {}
                    }
                    if (!snap) {
                        try { snap = await ref.limit(PREVIEW_LIMIT).get(); } catch (_) { snap = null; }
                    }

                    const docs = snap ? snap.docs.map(d => ({ _id: d.id, _col: col, ...d.data() })) : [];
                    totalCount += count;
                    allDocs.push(...docs);
                } catch (_) {}
            }

            allDocs.sort((a, b) => {
                const ta = a.timestamp?.toMillis ? a.timestamp.toMillis() : (a.timestamp?.seconds ? a.timestamp.seconds * 1000 : 0);
                const tb = b.timestamp?.toMillis ? b.timestamp.toMillis() : (b.timestamp?.seconds ? b.timestamp.seconds * 1000 : 0);
                return tb - ta;
            });
            return { count: totalCount, docs: allDocs.slice(0, PREVIEW_LIMIT) };
        }

        // ── 3. Normal collection ──────────────────────────────────
        const ref = getDB().collection(`artifacts/default-app-id/users/${uid}/${collectionName}`);

        async function fetchQuery(q) {
            // COUNT: always fetch the full result set for accurate total.
            // Admin SDK count() aggregation can silently fail, and using
            // limit(N).get().size caps the count at N — not the real total.
            let total = null;
            try {
                const countSnap = await q.get();
                total = countSnap.size;
            } catch (_) {}

            // PREVIEW: separate limited+ordered query for the notification body
            const orderFields = orderByField
                ? [orderByField, 'timestamp', 'sentAt', 'createdAt', 'date']
                : ['timestamp', 'sentAt', 'createdAt', 'date'];
            let snap = null;
            for (const field of orderFields) {
                try { snap = await q.orderBy(field, 'desc').limit(PREVIEW_LIMIT).get(); break; } catch (_) {}
            }
            if (!snap) { try { snap = await q.limit(PREVIEW_LIMIT).get(); } catch (_) { snap = null; } }
            const docs = snap ? snap.docs.map(d => ({ _id: d.id, ...d.data() })) : [];
            if (total === null) total = docs.length;
            return { count: total, docs };
        }

        if (statusFilter) {
            const filteredQ = ref.where(statusFilter.field, '==', statusFilter.value);
            let result;
            try { result = await fetchQuery(filteredQ); } catch (e) {
                logger.warn('statusFilter query failed — using total', { uid, collectionName, error: e.message });
                return await fetchQuery(ref);
            }
            // If filter returns 0 but collection has data, field naming likely differs
            if (result.count === 0) {
                const fallback = await fetchQuery(ref);
                if (fallback.count > 0) {
                    logger.info('statusFilter=0 but total=' + fallback.count + ' — using total', { uid, collectionName });
                    return fallback;
                }
            }
            return result;
        }

        return await fetchQuery(ref);

    } catch (err) {
        logger.warn('getCollectionData failed', { uid, module: collectionName, error: err.message });
        return { count: null, docs: [] };
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

// ── Per-module field hints for building a preview line ───────
// Maps module tag → ordered list of fields to try for a short label.
// Fund-archive `logs` docs: type + amount + method (e.g. "fund ৳500 Cash")
const _PREVIEW_FIELDS = {
    'advance-payment':    ['name', 'code', 'type'],
    'manpower':           ['name'],                          // from single-doc arrays
    'policy-files':       ['policyNo', 'clientName', 'name'],
    'fund-archive':       ['type', 'amount', 'method'],     // logs collection
    'hbl-recovery':       ['customerName', 'accountNumber'],
    'vat-tax':            ['khat', 'desc', 'amount'],      // vatPayments: date, khat, desc, chalan, amount
    'office-issue':       ['description', 'issue', 'title'],
    'notesheet':          ['subject', 'date'],
    'transport-bill':     ['name', 'billDate'],
    'business-stats':     ['meta'],                          // fields nested in meta{} — handled by special-case in _docPreviewLine
    'stationary-item':    ['smarok', 'date'],
    'medical-bill':       ['date', 'totalEntries', 'totalAmount'],
    'ta-bill':            ['travelerName', 'memoDate'],
    'license-forwarding': ['dateCreated', 'totalAgents'],
    'premium-submit':     ['typeDisplay', 'deposit', 'amount'],  // accounts_* tabs
    'personal-dues':      ['name', 'desc'],
    'personal-expense':   ['description'],
    'donation':           ['name', 'code', 'type'],
    'help':               ['title', 'description'],
    'help-requisition':   ['name', 'itemName'],
    'proposal-index':     ['name', 'proposalNo'],
    'fpr-register':       ['propName', 'name'],
    'license-archive':    ['dateCreated', 'totalAgents'],
};

/**
 * Extract a short human-readable preview line from one Firestore document.
 * Special-cased for modules whose data pattern requires combining fields.
 */
function _docPreviewLine(docData, moduleTag) {
    if (!docData) return null;

    // Business-stats archives: preview fields live inside meta{} sub-object
    if (moduleTag === 'business-stats') {
        const meta   = docData.meta || {};
        const name   = meta.inchargeName || '';
        const date   = meta.reportDate   || '';
        const line   = [name, date].filter(Boolean).join(' — ');
        return line.slice(0, 40) || null;
    }

    // Medical-bill archives: date + entry count + total amount
    if (moduleTag === 'medical-bill') {
        const date    = docData.date || '';
        const entries = docData.totalEntries != null ? `${docData.totalEntries}টি এন্ট্রি` : '';
        const amount  = docData.totalAmount  != null ? `৳${docData.totalAmount}` : '';
        const line = [date, entries, amount].filter(Boolean).join(' — ');
        return line.slice(0, 40) || null;
    }

    // Fund-archive logs: combine type label + amount + method
    if (moduleTag === 'fund-archive') {
        const typeLabel = { opening: 'জের', fund: 'ফান্ড', expense: 'খরচ' }[docData.type] || docData.type || '';
        const amt  = docData.amount != null ? `৳${docData.amount}` : '';
        const meth = docData.method || '';
        const line = [typeLabel, amt, meth].filter(Boolean).join(' ');
        return line.slice(0, 40) || null;
    }

    // Premium-submit: typeDisplay + deposit amount
    if (moduleTag === 'premium-submit') {
        const type = docData.typeDisplay || docData.type || '';
        const dep  = docData.deposit != null ? `৳${docData.deposit}` : (docData.amount != null ? `৳${docData.amount}` : '');
        const line = [type, dep].filter(Boolean).join(' ');
        return line.slice(0, 40) || null;
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

/**
 * Build a rich, data-driven notification body.
 *
 * With data:    "<count> <countLabel>: preview1, preview2 — নতুন এন্ট্রি যোগ করবেন?"
 * Empty/static: "<static body> নতুন এন্ট্রি যোগ করবেন?"
 */
function buildRichBody(module, count, docs) {
    if (module.staticOnly || !module.collection) {
        return module.body || `${module.title} পর্যালোচনা করুন।`;
    }
    if (!count || count === 0 || !docs || docs.length === 0) {
        return (module.body || `${module.title} পর্যালোচনা করুন।`) + ' নতুন এন্ট্রি যোগ করবেন?';
    }

    const previews = [];
    for (const d of docs.slice(0, 3)) {
        const line = _docPreviewLine(d, module.tag);
        if (line && !previews.includes(line)) previews.push(line);
        if (previews.length >= 3) break;
    }

    const countLabel = module.countLabel || 'টি এন্ট্রি';
    const countPart  = `${count} ${countLabel}`;

    return previews.length > 0
        ? `${countPart}: ${previews.join(', ')} — নতুন এন্ট্রি যোগ করবেন?`
        : `${countPart} আছে। পর্যালোচনা করুন।`;
}

/**
 * Build payload for a MODULE REMINDER push (all slots except task-manager).
 * Uses real Firestore data for a rich, actionable notification body.
 */
function buildModulePayload(module, statusKey, uid, count, docs, deviceName) {
    const body = buildRichBody(module, count, docs);

    return {
        title:              `${module.icon} ${module.title}`,
        body,
        icon:               'https://raw.githubusercontent.com/sifatahmedpro/PettyCash-Statement/main/logo.png',
        badge:              'https://raw.githubusercontent.com/sifatahmedpro/PettyCash-Statement/main/logo.png',
        tag:                `notif-${module.tag}-${statusKey}`,
        requireInteraction: false,
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
        actions: [
            { action: 'open',   title: '📂 দেখুন'       },
            { action: 'snooze', title: '⏰ ১ ঘণ্টা পরে' },
        ],
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
// WRITE PUSH DELIVERY LOG TO FIRESTORE
// Writes to two paths:
//   1. users/{uid}/pushLogs/{auto-id}      ← per-user delivery record
//   2. artifacts/default-app-id/pushRunLogs/{auto-id} ← global run log
// The notification-log page reads both collections.
// ══════════════════════════════════════════════════════════════

async function writePushLog(uid, module, result, count, statusKey, slotHour) {
    try {
        const now  = admin.firestore.FieldValue.serverTimestamp();
        const tag  = module.tag;
        const icon = module.icon || '🔔';
        const title = module.isTaskReminder ? 'টাস্ক ম্যানেজার' : (module.title || tag);

        // Build a rich detail string showing record count and slot context
        let detail = '';
        if (module.isTaskReminder) {
            detail = `টাস্ক রিমাইন্ডার — ঘণ্টা ${slotHour}:০০`;
        } else if (count !== null && count !== undefined && module.countLabel) {
            detail = `${count} ${module.countLabel} — ঘণ্টা ${slotHour}:০০ ঢাকা`;
        } else {
            detail = `নির্ধারিত রিমাইন্ডার — ঘণ্টা ${slotHour}:০০ ঢাকা`;
        }

        const logEntry = {
            tag,
            label:    title,
            icon,
            status:   result.errors > 0 && result.pushed === 0 ? 'failed' : 'sent',
            sentAt:   now,
            detail,
            sent:     result.pushed,
            failed:   result.errors,
            skipped:  result.skipped,
            slotHour,
            statusKey,
            runId:    CONFIG.RUN_ID,
            recordCount: count ?? null,
        };

        // 1. Per-user log
        await getDB()
            .collection(`artifacts/default-app-id/users/${uid}/pushLogs`)
            .add(logEntry);

        // 2. Global run log (for the run-level summary view)
        await getDB()
            .collection('artifacts/default-app-id/pushRunLogs')
            .add({ ...logEntry, uid });

    } catch (err) {
        logger.warn('writePushLog failed (non-fatal)', { uid, tag: module.tag, error: err.message });
    }
}

// ══════════════════════════════════════════════════════════════
// FETCH ALL PUSH SUBSCRIPTIONS FOR A USER
// ══════════════════════════════════════════════════════════════

async function getUserSubscriptions(uid) {
    try {
        const snap = await getDB()
            .collection(`artifacts/default-app-id/users/${uid}/pushSubscriptions`)
            .get();

        const all   = snap.docs.map(d => ({ _docId: d.id, ...d.data() }));
        const valid = [];

        for (const s of all) {
            if (!s.endpoint) {
                logger.warn('Subscription missing endpoint — skipping', { uid, docId: s._docId });
                continue;
            }
            // FIX: Previously `!s.keys` silently dropped subscriptions where the
            // keys field was missing (e.g. docs saved before the JSON-serialise fix),
            // causing ALL pushes to fail silently for that user.
            // Now we log the problem and still attempt delivery — web-push will
            // surface a clear error if keys are genuinely malformed.
            if (!s.keys) {
                logger.warn('Subscription missing keys field — attempting delivery anyway', {
                    uid, docId: s._docId, endpoint: s.endpoint.slice(0, 40)
                });
            }
            const { _docId, ...subData } = s;
            valid.push(subData);
        }

        logger.debug(`getUserSubscriptions: ${all.length} docs, ${valid.length} usable`, { uid });
        return valid;
    } catch (err) {
        logger.warn('getUserSubscriptions failed', { uid, error: err.message });
        return [];
    }
}

// ══════════════════════════════════════════════════════════════
// SEND ONE MODULE NOTIFICATION TO ALL SUBSCRIPTIONS OF A USER
// Returns { pushed, skipped, errors, expiredEndpoints }
// ══════════════════════════════════════════════════════════════

async function sendModuleNotificationToUser(uid, module, subscriptions, slotHour = null) {
    const result = { pushed: 0, skipped: 0, errors: 0, expiredEndpoints: [] };

    const statusKey  = buildStatusKey(module.tag, null);
    const seenCheck  = await checkSeenOrSnoozed(uid, statusKey);

    if (seenCheck.skip) {
        logger.debug(`Module ${module.tag} skipped for ${uid}: ${seenCheck.reason}`);
        result.skipped++;
        return result;
    }

    // Fetch data — count + up to 5 preview docs for the rich notification body.
    // isSingleDoc modules (e.g. manpower) and multi-collection modules
    // (e.g. premium-submit) are handled transparently by getCollectionData.
    let count = null;
    let docs  = [];
    if (!module.staticOnly && (module.collection || module.isSingleDoc)) {
        const data = await getCollectionData(uid, module);
        count = data.count;
        docs  = data.docs;
    }

    // Send to each subscription
    for (const sub of subscriptions) {
        const payload = buildModulePayload(module, statusKey, uid, count, docs, sub.deviceName);

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
        await writePushLog(uid, module, result, count, statusKey, slotHour);
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
        const taskModule = {
            tag:           'task-manager',
            icon:          '📋',
            title:         'টাস্ক ম্যানেজার',
            isTaskReminder: true,
        };
        await writePushLog(uid, taskModule, result, pendingTasks.length, statusKey, 10);
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
                result = await sendModuleNotificationToUser(uid, module, subscriptions, dhakaHour);
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
        logger.info('   অফিস ম্যানেজমেন্ট সিস্টেম — Push Notification Worker v5.5');
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
