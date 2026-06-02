/**
 * ============================================================
 * revenue-entry-layout.js
 * লেআউট লোডার — রেভিনিউ এন্ট্রি পেজ
 *
 * Fetches revenue-entry-layout.html into #app, then dispatches
 * a 'layoutReady' CustomEvent on document so that
 * revenue-entry.module.js (loaded via <script type="module"> in
 * revenue-entry.html) knows the DOM is ready before it runs.
 *
 * Pattern matches: admin-view-layout.js
 * ============================================================
 */

(function () {

    // ── Resolve layout HTML path relative to this script ─────
    function _resolveLayoutPath() {
        const scripts = document.querySelectorAll('script[src]');
        for (const s of scripts) {
            if (s.src && s.src.includes('revenue-entry-layout.js')) {
                const base = s.src.substring(0, s.src.lastIndexOf('/js/layout/'));
                return base + '/htmls/layout/revenue-entry-layout.html';
            }
        }
        return 'layout/revenue-entry-layout.html';
    }

    // ── Show a simple inline error if layout fails ────────────
    function _showError(msg) {
        const app = document.getElementById('app');
        if (!app) return;
        app.innerHTML = `
            <div style="
                max-width:520px; margin:60px auto; padding:28px 32px;
                background:#fff; border:1.5px solid #fca5a5;
                border-radius:12px; text-align:center;
                font-family:'Kalpurush','SolaimanLipi',Arial,sans-serif;
                box-shadow:0 4px 16px rgba(220,38,38,0.1);">
                <div style="font-size:36px; margin-bottom:12px;">⚠️</div>
                <div style="font-size:16px; font-weight:900;
                            color:#dc2626; margin-bottom:8px;">
                    লেআউট লোড করতে ব্যর্থ
                </div>
                <div style="font-size:16px; color:#6b7280; line-height:1.6;">
                    ${msg}
                </div>
                <button onclick="location.reload()"
                    style="margin-top:18px; padding:8px 22px;
                           background:#251577; color:#fff; border:none;
                           border-radius:6px; cursor:pointer;
                           font-family:inherit; font-size:16px;
                           font-weight:900;">
                    পুনরায় চেষ্টা করুন
                </button>
            </div>`;
    }

    // ── Main loader ───────────────────────────────────────────
    const layoutPath = _resolveLayoutPath();

    fetch(layoutPath)
        .then(function (res) {
            if (!res.ok) {
                throw new Error('HTTP ' + res.status + ' — ' + layoutPath);
            }
            return res.text();
        })
        .then(function (html) {
            // ── 1. Inject layout HTML into #app ───────────────
            const app = document.getElementById('app');
            if (!app) {
                throw new Error('#app element not found in the page.');
            }
            app.innerHTML = html;

            // ── 2. Notify revenue-entry.module.js that the DOM ──
            //       is ready. The module listens for this event
            //       before touching any layout elements.
            document.dispatchEvent(new CustomEvent('layoutReady', {
                detail: { layout: 'revenue-entry' }
            }));
        })
        .catch(function (err) {
            console.error('revenue-entry-layout.js:', err);
            _showError(
                'লেআউট ফাইল লোড করতে পারেনি:<br><code>' +
                layoutPath + '</code><br><br>' +
                'Error: ' + err.message
            );
        });

})();
