/**
 * ============================================================
 * revenue-entry-backend.js  (v1.0 — Supabase)
 * Backend Service Layer — Revenue Entry Page
 *
 * ROLE IN THE SYSTEM
 * ──────────────────
 * Supabase connectivity layer for the Revenue Entry page.
 * Exposes window.RevenueEntryDB with methods for:
 *   • Fetching previous day's balance
 *   • Saving new revenue entries
 *   • Updating existing entries
 *   • Deleting entries
 *   • Printing/exporting data
 *
 * MODULE CONTRACT
 * ───────────────
 * revenue-entry.module.js calls only window.RevenueEntryDB.* methods.
 * NO direct Supabase imports in the module file.
 *
 * WINDOW OBJECT:  window.RevenueEntryDB
 * MODULE FILE:    revenue-entry.module.js  (UI logic only)
 * PAGE:           revenue-entry.html
 *
 * TABLE SCHEMA (Supabase PostgreSQL)
 * ────────────────────────────────────
 * revenue_entries:
 *   id               UUID PRIMARY KEY
 *   uid              TEXT (admin/staff user id)
 *   office_id        TEXT NULL (null for admin, office_id for staff)
 *   entry_date       DATE
 *   previous_balance NUMERIC
 *   today_expense    NUMERIC
 *   new_revenue      NUMERIC
 *   revenue_details  TEXT
 *   notes            TEXT
 *   created_at       TIMESTAMP DEFAULT now()
 *   updated_at       TIMESTAMP DEFAULT now()
 *   created_by       TEXT (uid of creator)
 *
 * DEPENDENCIES
 * ────────────
 *   window.AppDB              — getOfficeContext() for uid/officeId
 *   window.AppCore            — auth state
 *   window.__supabaseClient   — Supabase client from app-backend.js
 *
 * ============================================================
 */

// ── Supabase client ────────────────────────────────────────────────────────
function _getSupabase() {
    if (window.__supabaseClient) return window.__supabaseClient;
    if (window.AppDB?.db) return window.AppDB.db;
    console.error('[RevenueEntryDB] Supabase client not available');
    return null;
}

// ── Boot: wait for AppCore ────────────────────────────────────────────────
function _boot() {
    const AppCore = window.AppCore;
    if (!AppCore) {
        console.error('[RevenueEntry] Cannot initialise — AppCore unavailable.');
        return;
    }

    const _auth = window.AppDB?.auth || null;
    const _db   = window.AppDB?.db   || null;

    AppCore.init(_auth, _db, {
        loginPage: 'log-in.html',

        onProfileLoaded(profileData) {
            // Profile loaded
        },

        async onReady(user) {
            console.log('[RevenueEntry] Auth ready. UID:', user.uid);

            const supabase = _getSupabase();
            if (!supabase) return;

            // ── Expose window.RevenueEntryDB ─────────────────────────────
            window.RevenueEntryDB = {

                /**
                 * Get today's date in YYYY-MM-DD format (local timezone).
                 */
                getTodayString() {
                    const d = new Date();
                    return d.getFullYear() + '-' +
                        String(d.getMonth() + 1).padStart(2, '0') + '-' +
                        String(d.getDate()).padStart(2, '0');
                },

                /**
                 * Get yesterday's date in YYYY-MM-DD format.
                 */
                getYesterdayString() {
                    const d = new Date();
                    d.setDate(d.getDate() - 1);
                    return d.getFullYear() + '-' +
                        String(d.getMonth() + 1).padStart(2, '0') + '-' +
                        String(d.getDate()).padStart(2, '0');
                },

                /**
                 * Fetch the previous day's closing balance (remaining balance).
                 * Returns the calculated closing balance from yesterday's entry,
                 * or 0 if no entry exists for yesterday.
                 *
                 * @param {string} uid
                 * @returns {Promise<number>}
                 */
                async getPreviousBalance(uid) {
                    try {
                        const yesterday = this.getYesterdayString();

                        // Get office context for data visibility
                        const ctx = await window.AppDB.getOfficeContext(uid);

                        let query = supabase
                            .from('revenue_entries')
                            .select('*')
                            .eq('uid', ctx.dataUid)
                            .eq('entry_date', yesterday);

                        if (ctx.officeId) {
                            query = query.eq('office_id', ctx.officeId);
                        } else {
                            query = query.is('office_id', null);
                        }

                        const { data, error } = await query.maybeSingle();

                        if (error) throw error;
                        if (!data) return 0;

                        // Calculate yesterday's closing balance:
                        // closing = previous_balance + new_revenue - today_expense
                        const closing = (data.previous_balance || 0) +
                                       (data.new_revenue || 0) -
                                       (data.today_expense || 0);
                        return Math.max(0, closing);
                    } catch (err) {
                        console.warn('[RevenueEntryDB] getPreviousBalance error:', err);
                        return 0;
                    }
                },

                /**
                 * Fetch an existing entry by date.
                 * Returns null if no entry found.
                 *
                 * @param {string} uid
                 * @param {string} dateStr  (YYYY-MM-DD)
                 * @returns {Promise<object|null>}
                 */
                async getEntryByDate(uid, dateStr) {
                    try {
                        const ctx = await window.AppDB.getOfficeContext(uid);

                        let query = supabase
                            .from('revenue_entries')
                            .select('*')
                            .eq('uid', ctx.dataUid)
                            .eq('entry_date', dateStr);

                        if (ctx.officeId) {
                            query = query.eq('office_id', ctx.officeId);
                        } else {
                            query = query.is('office_id', null);
                        }

                        const { data, error } = await query.maybeSingle();

                        if (error) throw error;
                        return data || null;
                    } catch (err) {
                        console.warn('[RevenueEntryDB] getEntryByDate error:', err);
                        return null;
                    }
                },

                /**
                 * Save a new revenue entry or update an existing one.
                 *
                 * @param {string} uid
                 * @param {object} entryData
                 *   {
                 *     entry_date: string         (YYYY-MM-DD)
                 *     previous_balance: number
                 *     today_expense: number
                 *     new_revenue: number
                 *     revenue_details: string
                 *     notes: string
                 *   }
                 * @returns {Promise<object>}  inserted/updated row
                 */
                async saveEntry(uid, entryData) {
                    try {
                        const ctx = await window.AppDB.getOfficeContext(uid);

                        // Check if entry already exists for this date
                        const existing = await this.getEntryByDate(uid, entryData.entry_date);

                        const payload = {
                            uid:                ctx.dataUid,
                            office_id:         ctx.officeId || null,
                            entry_date:        entryData.entry_date,
                            previous_balance:  parseFloat(entryData.previous_balance) || 0,
                            today_expense:     parseFloat(entryData.today_expense) || 0,
                            new_revenue:       parseFloat(entryData.new_revenue) || 0,
                            revenue_details:   entryData.revenue_details || '',
                            notes:             entryData.notes || '',
                            updated_at:        new Date().toISOString(),
                        };

                        if (existing) {
                            // Update
                            const { data, error } = await supabase
                                .from('revenue_entries')
                                .update(payload)
                                .eq('id', existing.id)
                                .select()
                                .single();

                            if (error) throw error;
                            console.info('[RevenueEntryDB] Entry updated for:', entryData.entry_date);
                            return data;
                        } else {
                            // Insert
                            payload.created_by = uid;
                            payload.created_at = new Date().toISOString();

                            const { data, error } = await supabase
                                .from('revenue_entries')
                                .insert([payload])
                                .select()
                                .single();

                            if (error) throw error;
                            console.info('[RevenueEntryDB] Entry created for:', entryData.entry_date);
                            return data;
                        }
                    } catch (err) {
                        console.error('[RevenueEntryDB] saveEntry error:', err);
                        throw err;
                    }
                },

                /**
                 * Delete an entry by date.
                 *
                 * @param {string} uid
                 * @param {string} dateStr  (YYYY-MM-DD)
                 * @returns {Promise<boolean>}
                 */
                async deleteEntry(uid, dateStr) {
                    try {
                        const ctx = await window.AppDB.getOfficeContext(uid);
                        const existing = await this.getEntryByDate(uid, dateStr);

                        if (!existing) return false;

                        const { error } = await supabase
                            .from('revenue_entries')
                            .delete()
                            .eq('id', existing.id);

                        if (error) throw error;
                        console.info('[RevenueEntryDB] Entry deleted for:', dateStr);
                        return true;
                    } catch (err) {
                        console.error('[RevenueEntryDB] deleteEntry error:', err);
                        throw err;
                    }
                },

                /**
                 * Fetch all entries for a date range (for reports).
                 *
                 * @param {string} uid
                 * @param {string} startDate  (YYYY-MM-DD)
                 * @param {string} endDate    (YYYY-MM-DD)
                 * @returns {Promise<Array>}
                 */
                async getEntriesInRange(uid, startDate, endDate) {
                    try {
                        const ctx = await window.AppDB.getOfficeContext(uid);

                        let query = supabase
                            .from('revenue_entries')
                            .select('*')
                            .eq('uid', ctx.dataUid)
                            .gte('entry_date', startDate)
                            .lte('entry_date', endDate)
                            .order('entry_date', { ascending: true });

                        if (ctx.officeId) {
                            query = query.eq('office_id', ctx.officeId);
                        } else {
                            query = query.is('office_id', null);
                        }

                        const { data, error } = await query;

                        if (error) throw error;
                        return data || [];
                    } catch (err) {
                        console.error('[RevenueEntryDB] getEntriesInRange error:', err);
                        return [];
                    }
                },

                /**
                 * Format currency with Bengali numerals and symbols.
                 *
                 * @param {number} value
                 * @returns {string}
                 */
                formatCurrency(value) {
                    const num = parseFloat(value) || 0;
                    const banglaNum = this._convertToBangla(num.toFixed(2));
                    return banglaNum + ' টাকা';
                },

                /**
                 * Convert English numerals to Bengali.
                 * Helper method for currency formatting.
                 */
                _convertToBangla(str) {
                    const engToBangla = {
                        '0': '০', '1': '১', '2': '২', '3': '৩', '4': '৪',
                        '5': '৫', '6': '৬', '7': '৭', '8': '৮', '9': '৯'
                    };
                    return String(str).replace(/\d/g, d => engToBangla[d]);
                },

                /**
                 * Generate a print-friendly HTML representation.
                 * (Used by revenue-entry.module.js for printing)
                 *
                 * @param {object} entry
                 * @returns {string}
                 */
                generatePrintHTML(entry) {
                    const closing = (entry.previous_balance || 0) +
                                   (entry.new_revenue || 0) -
                                   (entry.today_expense || 0);

                    const fmt = this.formatCurrency.bind(this);

                    return `
                        <div style="text-align: center; margin-bottom: 20px;">
                            <img src="https://raw.githubusercontent.com/sifatahmedpro/PettyCash-Statement/main/sales-16.png"
                                 alt="Logo" style="width: 80px; height: 80px;">
                            <h1 style="margin: 10px 0; font-size: 24px;">রেভিনিউ এন্ট্রি</h1>
                            <p style="color: #666;">${entry.entry_date}</p>
                        </div>

                        <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">
                            <tr>
                                <td style="border: 1px solid #ccc; padding: 10px; text-align: right; width: 50%;">
                                    <strong>পূর্বের জের:</strong>
                                </td>
                                <td style="border: 1px solid #ccc; padding: 10px; text-align: right; width: 50%;">
                                    ${fmt(entry.previous_balance)}
                                </td>
                            </tr>
                            <tr>
                                <td style="border: 1px solid #ccc; padding: 10px; text-align: right;">
                                    <strong>আজকের খরচ:</strong>
                                </td>
                                <td style="border: 1px solid #ccc; padding: 10px; text-align: right;">
                                    ${fmt(entry.today_expense)}
                                </td>
                            </tr>
                            <tr>
                                <td style="border: 1px solid #ccc; padding: 10px; text-align: right;">
                                    <strong>অবশিষ্ট:</strong>
                                </td>
                                <td style="border: 1px solid #ccc; padding: 10px; text-align: right;">
                                    ${fmt((entry.previous_balance || 0) - (entry.today_expense || 0))}
                                </td>
                            </tr>
                            <tr>
                                <td style="border: 1px solid #ccc; padding: 10px; text-align: right;">
                                    <strong>নতুন প্রাপ্তি:</strong>
                                </td>
                                <td style="border: 1px solid #ccc; padding: 10px; text-align: right;">
                                    ${fmt(entry.new_revenue)}
                                </td>
                            </tr>
                            <tr style="background: #f5f5ff;">
                                <td style="border: 1px solid #ccc; padding: 10px; text-align: right; font-weight: bold;">
                                    চূড়ান্ত ব্যালেন্স:
                                </td>
                                <td style="border: 1px solid #ccc; padding: 10px; text-align: right; font-weight: bold; font-size: 16px;">
                                    ${fmt(closing)}
                                </td>
                            </tr>
                        </table>

                        ${entry.revenue_details ? `
                            <div style="margin-bottom: 15px;">
                                <strong>প্রাপ্তির বিবরণ:</strong><br>
                                <p style="white-space: pre-wrap;">${entry.revenue_details}</p>
                            </div>
                        ` : ''}

                        ${entry.notes ? `
                            <div style="margin-bottom: 15px;">
                                <strong>নোট:</strong><br>
                                <p style="white-space: pre-wrap;">${entry.notes}</p>
                            </div>
                        ` : ''}
                    `;
                }

            }; // window.RevenueEntryDB

            // Signal revenue-entry.module.js that RevenueEntryDB is ready
            try { window.dispatchEvent(new Event('revenueentrydb-ready')); } catch (_) {}
        }
    });
}

// ── Event-driven boot ──────────────────────────────────────────────────────
if (window.__appBackendLoaded) {
    _boot();
} else {
    window.addEventListener('appdb-ready', _boot, { once: true });
}
