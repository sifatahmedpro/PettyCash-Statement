/**
 * ============================================================
 * revenue-entry.module.js  (v1.0)
 * UI Logic & Calculation Engine — Revenue Entry Page
 *
 * ZERO database imports. All data access via window.RevenueEntryDB.
 * Loaded as <script type="module"> in revenue-entry.html.
 *
 * FEATURES
 * ────────
 * • Auto-fill previous balance from yesterday's entry
 * • Real-time calculation: remaining = previous - expense
 * • Final balance = remaining + new_revenue
 * • Save entries with auto-detect insert vs update
 * • Delete entries
 * • Print/export to PDF
 * • Responsive mobile UI with Bangla numerals
 *
 * ============================================================
 */

// ── Wait for layout + backend to be ready ──────────────────────────────────
async function _waitForDependencies() {
    return new Promise((resolve) => {
        let layoutReady = false;
        let backendReady = false;

        const _check = () => {
            if (layoutReady && backendReady && window.RevenueEntryDB) {
                resolve();
            }
        };

        // Listen for layout ready
        document.addEventListener('layoutReady', () => {
            layoutReady = true;
            _check();
        });

        // Listen for backend ready
        window.addEventListener('revenueentrydb-ready', () => {
            backendReady = true;
            _check();
        });

        // Fallback: check every 50ms for 10 seconds
        const maxAttempts = 200;
        let attempts = 0;
        const fallbackTimer = setInterval(() => {
            attempts++;
            if (document.getElementById('revenueEntryForm') && window.RevenueEntryDB) {
                layoutReady = true;
                backendReady = true;
                clearInterval(fallbackTimer);
                _check();
            }
            if (attempts >= maxAttempts) {
                clearInterval(fallbackTimer);
                console.error('[RevenueEntry] Dependencies timeout');
                resolve(); // proceed anyway
            }
        }, 50);
    });
}

// ── Format number to Bengali with thousands separator ─────────────────────
function _formatBengaliNumber(num) {
    const banglaDigits = ['०', '१', '२', '३', '४', '५', '६', '७', '८', '९'];
    const formatted = parseFloat(num || 0).toLocaleString('en-US', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    });
    return formatted.replace(/\d/g, d => banglaDigits[d]);
}

// ── Main initialization ────────────────────────────────────────────────────
async function _initRevenueEntry() {
    await _waitForDependencies();

    const form = document.getElementById('revenueEntryForm');
    if (!form) {
        console.error('[RevenueEntry] Form not found');
        return;
    }

    const user = window.AppCore?.user;
    if (!user) {
        console.error('[RevenueEntry] User not authenticated');
        return;
    }

    const DB = window.RevenueEntryDB;

    // ── DOM element references ────────────────────────────────────────────
    const elDate              = document.getElementById('entryDate');
    const elTodayExpense      = document.getElementById('todayExpense');
    const elNewRevenue        = document.getElementById('newRevenue');
    const elRevenueDetails    = document.getElementById('revenueDetails');
    const elEntryNotes        = document.getElementById('entryNotes');
    const elPreviousBalance   = document.getElementById('previousBalance');
    const elRemainingBalance  = document.getElementById('remainingBalance');
    const elTodayDate         = document.getElementById('todayDate');
    const elSummaryOpening    = document.getElementById('summaryOpeningBalance');
    const elSummaryExpense    = document.getElementById('summaryExpense');
    const elSummaryRevenue    = document.getElementById('summaryRevenue');
    const elSummaryFinal      = document.getElementById('summaryFinalBalance');
    const btnPrint            = document.getElementById('printBtn');
    const btnEdit             = document.getElementById('editBtn');
    const btnDelete           = document.getElementById('deleteBtn');
    const btnCancel           = document.getElementById('cancelBtn');
    const btnSubmit           = document.querySelector('button[type="submit"]');

    let _currentEntry = null;  // Track loaded entry
    let _isEditMode = false;   // Track if in edit mode

    // ── Initialize date display ───────────────────────────────────────────
    function _updateDateDisplay() {
        const today = new Date();
        const options = { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' };
        const dateStr = today.toLocaleDateString('bn-BD', options);
        if (elTodayDate) elTodayDate.textContent = dateStr;
    }

    // ── Load previous balance ─────────────────────────────────────────────
    async function _loadPreviousBalance() {
        try {
            const prevBalance = await DB.getPreviousBalance(user.uid);
            if (elPreviousBalance) {
                elPreviousBalance.textContent = _formatBengaliNumber(prevBalance);
            }
            return prevBalance;
        } catch (err) {
            console.warn('[RevenueEntry] Error loading previous balance:', err);
            if (elPreviousBalance) elPreviousBalance.textContent = '०';
            return 0;
        }
    }

    // ── Calculate and update all derived fields ───────────────────────────
    async function _recalculate() {
        const prevBalance = parseFloat(elPreviousBalance.textContent.replace(/[०-९]/g, (d) => {
            const benglaDigits = '०१२३४५६७८९';
            const idx = benglaDigits.indexOf(d);
            return idx > -1 ? String(idx) : d;
        })) || 0;

        const todayExpense = parseFloat(elTodayExpense.value) || 0;
        const newRevenue = parseFloat(elNewRevenue.value) || 0;

        const remaining = prevBalance - todayExpense;
        const finalBalance = remaining + newRevenue;

        // Update display fields
        if (elRemainingBalance) {
            elRemainingBalance.textContent = _formatBengaliNumber(Math.max(0, remaining));
        }
        if (elSummaryOpening) {
            elSummaryOpening.textContent = _formatBengaliNumber(prevBalance);
        }
        if (elSummaryExpense) {
            elSummaryExpense.textContent = _formatBengaliNumber(todayExpense);
        }
        if (elSummaryRevenue) {
            elSummaryRevenue.textContent = _formatBengaliNumber(newRevenue);
        }
        if (elSummaryFinal) {
            elSummaryFinal.textContent = _formatBengaliNumber(Math.max(0, finalBalance));
        }
    }

    // ── Load entry by date ────────────────────────────────────────────────
    async function _loadEntryForDate(dateStr) {
        try {
            const entry = await DB.getEntryByDate(user.uid, dateStr);
            if (entry) {
                _currentEntry = entry;
                elTodayExpense.value = entry.today_expense || '';
                elNewRevenue.value = entry.new_revenue || '';
                elRevenueDetails.value = entry.revenue_details || '';
                elEntryNotes.value = entry.notes || '';
                _isEditMode = true;
                btnSubmit.textContent = '✏️ আপডেট';
                btnEdit.style.display = 'inline-flex';
                btnDelete.style.display = 'inline-flex';
                await _recalculate();
            } else {
                _currentEntry = null;
                elTodayExpense.value = '';
                elNewRevenue.value = '';
                elRevenueDetails.value = '';
                elEntryNotes.value = '';
                _isEditMode = false;
                btnSubmit.textContent = '💾 সংরক্ষণ';
                btnEdit.style.display = 'none';
                btnDelete.style.display = 'none';
                await _loadPreviousBalance();
                await _recalculate();
            }
        } catch (err) {
            console.error('[RevenueEntry] Error loading entry:', err);
            alert('প্রবেশ লোড করতে ব্যর্থ: ' + err.message);
        }
    }

    // ── Event: Date input change ──────────────────────────────────────────
    if (elDate) {
        elDate.addEventListener('change', async (e) => {
            if (e.target.value) {
                await _loadEntryForDate(e.target.value);
            }
        });
    }

    // ── Event: Input changes trigger recalculation ────────────────────────
    const recalcElements = [elTodayExpense, elNewRevenue];
    recalcElements.forEach(el => {
        if (el) {
            el.addEventListener('input', _recalculate);
            el.addEventListener('change', _recalculate);
        }
    });

    // ── Event: Form submit ────────────────────────────────────────────────
    form.addEventListener('submit', async (e) => {
        e.preventDefault();

        if (!elDate.value) {
            alert('অনুগ্রহ করে তারিখ নির্বাচন করুন');
            return;
        }

        try {
            const entryData = {
                entry_date: elDate.value,
                previous_balance: parseFloat(elPreviousBalance.textContent.replace(/[०-९]/g, d => {
                    const benglaDigits = '०१२३४५६७८९';
                    const idx = benglaDigits.indexOf(d);
                    return idx > -1 ? String(idx) : d;
                })) || 0,
                today_expense: parseFloat(elTodayExpense.value) || 0,
                new_revenue: parseFloat(elNewRevenue.value) || 0,
                revenue_details: elRevenueDetails.value || '',
                notes: elEntryNotes.value || ''
            };

            const result = await DB.saveEntry(user.uid, entryData);

            if (result) {
                _currentEntry = result;
                _isEditMode = true;
                btnSubmit.textContent = '✏️ আপডেট';
                btnEdit.style.display = 'inline-flex';
                btnDelete.style.display = 'inline-flex';
                alert('প্রবেশ সফলভাবে সংরক্ষিত হয়েছে');
            }
        } catch (err) {
            console.error('[RevenueEntry] Save error:', err);
            alert('সংরক্ষণে ব্যর্থ: ' + err.message);
        }
    });

    // ── Event: Cancel button ──────────────────────────────────────────────
    if (btnCancel) {
        btnCancel.addEventListener('click', () => {
            if (confirm('সব পরিবর্তন বাতিল করতে চান?')) {
                form.reset();
                elDate.value = DB.getTodayString();
                _isEditMode = false;
                btnSubmit.textContent = '💾 সংরক্ষণ';
                btnEdit.style.display = 'none';
                btnDelete.style.display = 'none';
                _currentEntry = null;
                _loadPreviousBalance();
                _recalculate();
            }
        });
    }

    // ── Event: Delete button ──────────────────────────────────────────────
    if (btnDelete) {
        btnDelete.addEventListener('click', async () => {
            if (!_currentEntry || !confirm('এই প্রবেশ মুছতে চান?')) return;

            try {
                const success = await DB.deleteEntry(user.uid, elDate.value);
                if (success) {
                    alert('প্রবেশ সফলভাবে মুছে ফেলা হয়েছে');
                    form.reset();
                    elDate.value = DB.getTodayString();
                    _isEditMode = false;
                    btnSubmit.textContent = '💾 সংরক্ষণ';
                    btnEdit.style.display = 'none';
                    btnDelete.style.display = 'none';
                    _currentEntry = null;
                    await _loadPreviousBalance();
                    await _recalculate();
                }
            } catch (err) {
                console.error('[RevenueEntry] Delete error:', err);
                alert('মুছতে ব্যর্থ: ' + err.message);
            }
        });
    }

    // ── Event: Print button ───────────────────────────────────────────────
    if (btnPrint) {
        btnPrint.addEventListener('click', () => {
            if (!elDate.value) {
                alert('অনুগ্রহ করে তারিখ নির্বাচন করুন');
                return;
            }

            const printData = {
                entry_date: elDate.value,
                previous_balance: parseFloat(elPreviousBalance.textContent.replace(/[०-९]/g, d => {
                    const benglaDigits = '०१२३४५६७८९';
                    const idx = benglaDigits.indexOf(d);
                    return idx > -1 ? String(idx) : d;
                })) || 0,
                today_expense: parseFloat(elTodayExpense.value) || 0,
                new_revenue: parseFloat(elNewRevenue.value) || 0,
                revenue_details: elRevenueDetails.value,
                notes: elEntryNotes.value
            };

            const printHTML = DB.generatePrintHTML(printData);
            const printWindow = window.open('', '_blank');
            printWindow.document.write(`
                <!DOCTYPE html>
                <html lang="bn">
                <head>
                    <meta charset="UTF-8">
                    <title>রেভিনিউ এন্ট্রি</title>
                    <style>
                        body {
                            font-family: 'SolaimanLipi', 'Kalpurush', sans-serif;
                            padding: 20px;
                            background: #f5f5ff;
                        }
                        .container {
                            max-width: 800px;
                            margin: 0 auto;
                            background: white;
                            padding: 30px;
                            border-radius: 8px;
                            box-shadow: 0 2px 8px rgba(37, 21, 119, 0.1);
                        }
                        h1 { color: #251577; text-align: center; }
                        table { width: 100%; border-collapse: collapse; }
                        td { padding: 10px; border: 1px solid #ccc; }
                        strong { color: #251577; }
                        @media print {
                            body { background: white; padding: 0; }
                            .container { box-shadow: none; }
                        }
                    </style>
                </head>
                <body>
                    <div class="container">${printHTML}</div>
                    <script>
                        setTimeout(() => window.print(), 500);
                        setTimeout(() => window.close(), 2000);
                    </script>
                </body>
                </html>
            `);
            printWindow.document.close();
        });
    }

    // ── Initialize page ───────────────────────────────────────────────────
    _updateDateDisplay();
    elDate.value = DB.getTodayString();
    await _loadPreviousBalance();
    await _recalculate();

    console.log('[RevenueEntry] Page initialized successfully');
}

// ── Boot on DOM ready ──────────────────────────────────────────────────────
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _initRevenueEntry);
} else {
    _initRevenueEntry();
}
