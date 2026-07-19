/**
 * ============================================================
 * email-backend.js  —  v3.0 (SUPABASE MIGRATION)
 * Supabase Backend Layer — Email Notification System
 *
 * MIGRATION (Firebase → Supabase):
 *   Firebase SDK imports   → removed entirely
 *   getFirestore / doc / getDoc / setDoc / deleteDoc / serverTimestamp
 *                          → window.__supabaseClient (set by app-backend.js)
 *   Firestore path:          artifacts/default-app-id/users/{uid}/data/emailSubscription
 *                          → Supabase table: email_subscriptions  (uid PK)
 *
 *   Firestore dot-notation merge for setPagePref:
 *     setDoc(ref, { 'prefs.pages.advance_payment': true }, { merge: true })
 *   → Supabase:
 *     fetch existing row → mutate pages object locally → upsert full prefs column
 *     (Supabase does not support dot-notation partial JSONB updates from the client)
 *
 * TABLE SCHEMA  (email_subscriptions):
 *   uid          TEXT PRIMARY KEY
 *   email        TEXT NOT NULL
 *   prefs        JSONB NOT NULL DEFAULT '{}'
 *   active       BOOLEAN NOT NULL DEFAULT true
 *   updated_at   TIMESTAMPTZ DEFAULT now()
 *
 *   prefs JSONB shape (unchanged from v2):
 *   {
 *     morning: boolean,
 *     evening: boolean,
 *     tasks:   boolean,
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
 * CONTRACT — window.EmailDB exposes (unchanged API surface):
 *   .saveEmailSubscription(uid, email, prefs)  → Promise
 *   .removeEmailSubscription(uid)              → Promise
 *   .getEmailSubscription(uid)                 → Promise<sub|null>
 *   .isSubscribed(uid)                         → Promise<boolean>
 *   .setPagePref(uid, pageKey, enabled)        → Promise
 *
 * TO ADD TO ANY HTML PAGE:
 *   <script src="../js/common/supabase-config.js"></script>
 *   <script type="module" src="../js/common/app-backend.js"></script>
 *   <script type="module" src="../js/email/email-backend.js"></script>
 * ============================================================
 */

// ── Supabase client — set by app-backend.js ───────────────────
// We do NOT import createClient here; app-backend.js already created
// the shared client and exposed it as window.__supabaseClient.
// Using the shared instance avoids a second connection.
function _supa() {
    const c = window.__supabaseClient;
    if (!c) throw new Error('[EmailDB] window.__supabaseClient not ready — ensure app-backend.js loads first');
    return c;
}

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
     * Prefs shape (unchanged from v2):
     *   {
     *     morning: boolean,
     *     evening: boolean,
     *     tasks:   boolean,
     *     pages: { advance_payment, business_stats, donation, help, office_issue, premium_submit }
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

        const defaultPages = Object.fromEntries(PAGE_KEYS.map(k => [k, true]));
        const defaults = { morning: true, evening: true, tasks: true, pages: defaultPages };
        const mergedPages = { ...defaultPages, ...(prefs.pages || {}) };
        const merged = { ...defaults, ...prefs, pages: mergedPages };

        const { error } = await _supa()
            .from('email_subscriptions')
            .upsert({
                uid,
                email:      email.trim().toLowerCase(),
                prefs:      merged,
                active:     true,
                updated_at: new Date().toISOString(),
            }, { onConflict: 'uid' });

        if (error) throw error;
    },

    /**
     * Soft-disable (set active: false) rather than hard-delete.
     *
     * @param {string} uid
     * @returns {Promise}
     */
    async removeEmailSubscription(uid) {
        if (!uid) throw new Error('EmailDB.removeEmailSubscription: uid required');

        const { error } = await _supa()
            .from('email_subscriptions')
            .update({ active: false, updated_at: new Date().toISOString() })
            .eq('uid', uid);

        if (error) throw error;
    },

    /**
     * Load the subscription row.
     * Returns null if the user has never subscribed.
     *
     * @param {string} uid
     * @returns {Promise<object|null>}
     */
    async getEmailSubscription(uid) {
        if (!uid) return null;
        try {
            const { data, error } = await _supa()
                .from('email_subscriptions')
                .select('uid, email, prefs, active, updated_at')
                .eq('uid', uid)
                .maybeSingle();

            if (error) throw error;
            if (!data) return null;

            // Map snake_case column back to the camelCase shape the UI expects
            return {
                uid:       data.uid,
                email:     data.email,
                prefs:     data.prefs || {},
                active:    data.active,
                updatedAt: data.updated_at,
            };
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
     * Supabase does not support dot-notation partial JSONB updates
     * from the client SDK, so we fetch → mutate locally → upsert.
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
            // Fetch current prefs so we can do a safe merge
            const existing = await this.getEmailSubscription(uid);
            const currentPrefs = existing?.prefs || {};
            const currentPages = currentPrefs.pages || {};

            const updatedPrefs = {
                ...currentPrefs,
                pages: { ...currentPages, [pageKey]: !!enabled }
            };

            const { error } = await _supa()
                .from('email_subscriptions')
                .update({
                    prefs:      updatedPrefs,
                    updated_at: new Date().toISOString(),
                })
                .eq('uid', uid);

            if (error) throw error;
        } catch (err) {
            console.error('EmailDB.setPagePref error:', err);
            throw err;
        }
    }
};
