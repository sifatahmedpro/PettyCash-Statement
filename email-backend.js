/**
 * ============================================================
 * email-backend.js
 * Firebase Backend Layer — Email Notification System
 *
 * PURPOSE:
 *   Mirrors the architecture of app-backend.js but for EMAIL.
 *   Stores each user's email address and preferences in
 *   Firestore so the GitHub Actions send-email.js worker
 *   can pick them up at 8 AM and 8 PM (Dhaka time).
 *
 *   No localStorage. No cookies. Pure Firestore state.
 *
 * CONTRACT — window.EmailDB exposes:
 *   .saveEmailSubscription(uid, email, prefs) → Promise
 *   .removeEmailSubscription(uid)             → Promise
 *   .getEmailSubscription(uid)                → Promise<sub|null>
 *   .isSubscribed(uid)                        → Promise<boolean>
 *
 * FIRESTORE PATH:
 *   artifacts/default-app-id/users/{uid}/data/emailSubscription
 *   (one document per user — no sub-collection needed)
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

// ── Path helper — matches app-backend.js convention ──────────
const _emailSubRef = (uid) =>
    doc(_db, 'artifacts', 'default-app-id', 'users', uid, 'data', 'emailSubscription');

/**
 * window.EmailDB
 * All email-notification pages call this object exclusively.
 * Zero Firebase imports needed in any HTML or module page.
 */
window.EmailDB = {

    /**
     * Save (or update) the user's email subscription.
     *
     * Prefs shape:
     *   {
     *     morning: boolean,   // 8 AM digest
     *     evening: boolean,   // 8 PM digest
     *     tasks:   boolean,   // include pending tasks section
     *   }
     *
     * If prefs is omitted, defaults to all-enabled.
     *
     * @param {string} uid
     * @param {string} email
     * @param {object} [prefs]
     * @returns {Promise}
     */
    async saveEmailSubscription(uid, email, prefs = {}) {
        if (!uid)   throw new Error('EmailDB.saveEmailSubscription: uid required');
        if (!email) throw new Error('EmailDB.saveEmailSubscription: email required');

        const defaults = { morning: true, evening: true, tasks: true };
        const merged   = { ...defaults, ...prefs };

        return setDoc(_emailSubRef(uid), {
            uid,
            email:     email.trim().toLowerCase(),
            prefs:     merged,
            active:    true,
            updatedAt: serverTimestamp()
        }, { merge: true });
    },

    /**
     * Soft-disable (set active: false) rather than hard-delete,
     * so the worker can cleanly skip without leaving stale docs.
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
    }
};
