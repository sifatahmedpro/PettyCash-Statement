/**
 * ============================================================
 * email-backend.js  —  v2.0 (PER-PAGE PREFERENCES)
 * Firebase Backend Layer — Email Notification System
 *
 * WHAT CHANGED vs v1:
 *   The prefs object now supports a nested `pages` map so users
 *   can opt in/out of emails from individual pages:
 *
 *   prefs: {
 *     morning: true,        // 8 AM digest slot
 *     evening: true,        // 8 PM digest slot
 *     tasks:   true,        // (legacy — still read by send-email)
 *     pages: {
 *       advance_payment:  true,
 *       business_stats:   true,
 *       donation:         true,
 *       help:             true,
 *       office_issue:     true,
 *       premium_submit:   true,
 *     }
 *   }
 *
 *   All page keys default to true (send) if not present —
 *   so existing subscriptions continue working without any
 *   migration.
 *
 * CONTRACT — window.EmailDB exposes:
 *   .saveEmailSubscription(uid, email, prefs)  → Promise
 *   .removeEmailSubscription(uid)              → Promise
 *   .getEmailSubscription(uid)                 → Promise<sub|null>
 *   .isSubscribed(uid)                         → Promise<boolean>
 *   .setPagePref(uid, pageKey, enabled)        → Promise   ← NEW v2
 *
 * FIRESTORE PATH (unchanged):
 *   artifacts/default-app-id/users/{uid}/data/emailSubscription
 *
 * TO ADD TO ANY HTML PAGE:
 *   <script src="firebase-config.js"></script>
 *   <script type="module" src="email-backend.js"></script>
 * ============================================================
 */

import { initializeApp, getApps }
    from 'https://www.gstatic.com/firebasejs/11.6.1/firebase-app.js';
import {
    getFirestore,
    doc, getDoc, setDoc, deleteDoc, serverTimestamp
} from 'https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js';

// ── Firebase init (reuse existing app if already initialised) ──
const _app = getApps().length
    ? getApps()[0]
    : initializeApp(window.__APP_CONFIG__ || window.FIREBASE_CONFIG);

const _db = getFirestore(_app);

// ── Path helper ───────────────────────────────────────────────
const _emailSubRef = (uid) =>
    doc(_db, 'artifacts', 'default-app-id', 'users', uid, 'data', 'emailSubscription');

// ── All valid page keys (must match send-email.js getPageDefinitions) ──
const PAGE_KEYS = [
    'advance_payment',
    'business_stats',
    'donation',
    'help',
    'office_issue',
    'premium_submit',
];

/**
 * window.EmailDB
 */
window.EmailDB = {

    /**
     * Save (or update) the user's email subscription.
     *
     * Prefs shape (v2):
     *   {
     *     morning: boolean,   // 8 AM digest
     *     evening: boolean,   // 8 PM digest
     *     tasks:   boolean,   // (legacy key — keep for compatibility)
     *     pages: {
     *       advance_payment:  boolean,
     *       business_stats:   boolean,
     *       donation:         boolean,
     *       help:             boolean,
     *       office_issue:     boolean,
     *       premium_submit:   boolean,
     *     }
     *   }
     *
     * If prefs.pages is omitted, all pages default to true.
     *
     * @param {string} uid
     * @param {string} email
     * @param {object} [prefs]
     * @returns {Promise}
     */
    async saveEmailSubscription(uid, email, prefs = {}) {
        if (!uid)   throw new Error('EmailDB.saveEmailSubscription: uid required');
        if (!email) throw new Error('EmailDB.saveEmailSubscription: email required');

        // Build default pages map (all enabled)
        const defaultPages = Object.fromEntries(PAGE_KEYS.map(k => [k, true]));

        const defaults = {
            morning: true,
            evening: true,
            tasks:   true,
            pages:   defaultPages
        };

        // Deep-merge pages so callers can supply partial overrides
        const mergedPages = { ...defaultPages, ...(prefs.pages || {}) };
        const merged = { ...defaults, ...prefs, pages: mergedPages };

        return setDoc(_emailSubRef(uid), {
            uid,
            email:     email.trim().toLowerCase(),
            prefs:     merged,
            active:    true,
            updatedAt: serverTimestamp()
        }, { merge: true });
    },

    /**
     * Soft-disable (set active: false) rather than hard-delete.
     *
     * @param {string} uid
     * @returns {Promise}
     */
    async removeEmailSubscription(uid) {
        if (!uid) throw new Error('EmailDB.removeEmailSubscription: uid required');
        return setDoc(_emailSubRef(uid), { active: false, updatedAt: serverTimestamp() }, { merge: true });
    },

    /**
     * Load the subscription document.
     * Returns null if the user has never subscribed.
     *
     * @param {string} uid
     * @returns {Promise<object|null>}
     */
    async getEmailSubscription(uid) {
        if (!uid) return null;
        try {
            const snap = await getDoc(_emailSubRef(uid));
            if (!snap.exists()) return null;
            return snap.data();
        } catch (err) {
            console.error('EmailDB.getEmailSubscription error:', err);
            return null;
        }
    },

    /**
     * Quick boolean check — is this user actively subscribed?
     *
     * @param {string} uid
     * @returns {Promise<boolean>}
     */
    async isSubscribed(uid) {
        const sub = await this.getEmailSubscription(uid);
        return !!(sub && sub.active === true);
    },

    /**
     * Toggle a single page's email on or off without touching
     * any other preferences.
     *
     * Called from the email widget when a user flips a per-page
     * toggle (if you add per-page toggles to email-ui.js).
     *
     * @param {string}  uid
     * @param {string}  pageKey   — one of PAGE_KEYS above
     * @param {boolean} enabled
     * @returns {Promise}
     */
    async setPagePref(uid, pageKey, enabled) {
        if (!uid) throw new Error('EmailDB.setPagePref: uid required');
        if (!PAGE_KEYS.includes(pageKey)) {
            throw new Error(`EmailDB.setPagePref: unknown pageKey "${pageKey}"`);
        }
        try {
            // Firestore dot-notation merge updates only the nested field
            return setDoc(_emailSubRef(uid), {
                [`prefs.pages.${pageKey}`]: !!enabled,
                updatedAt: serverTimestamp()
            }, { merge: true });
        } catch (err) {
            console.error('EmailDB.setPagePref error:', err);
            throw err;
        }
    }
};
