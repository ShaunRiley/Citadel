(async function () {
  "use strict";

  const CONFIG = {
    baseUrl: window.location.origin + "/api",
    defaultPollIntervalMs: 3000,
    defaultPollMaxAttempts: 50,
    downloadRetryDelayMs: 3000,
    downloadRetryCount: 20,
    maxRegenerations: 3,
    debug: true,
    consolidatedGroupId: "1658FA48-2BA6-4E06-98C8-0053F42C5610",
    reportPolling: {
      MonthlyReturn: { intervalMs: 3000, maxAttempts: 50 },
      AccountPerformance: { intervalMs: 3000, maxAttempts: 50 },
      Consolidated: { intervalMs: 15000, maxAttempts: 120 }
    }
  };

  let AUTH_TOKEN = null;

  function log() { console.log("[CitazenEngine]", ...arguments); }
  function debug() { if (CONFIG.debug) console.log("[CitazenEngine][DEBUG]", ...arguments); }
  function assert(c, m) { if (!c) throw new Error(m); }

  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const normalize = v => String(v || "").trim().toLowerCase();
  const ensureArray = v => !v ? [] : Array.isArray(v) ? v : Object.values(v);
  const unique = v => [...new Set(ensureArray(v).filter(Boolean))];
  
function removeNulls(obj) {
  if (Array.isArray(obj)) {
    return obj.map(removeNulls);
  }

  if (obj && typeof obj === "object") {
    const result = {};
    for (const key in obj) {
      const val = obj[key];

      if (val === null) continue; // CRITICAL

      result[key] = removeNulls(val);
    }
    return result;
  }

  return obj;
}
  function coerceJson(v) {
    if (!v || typeof v === "object") return v;
    try { return JSON.parse(v); } catch { return v; }
  }

  function shouldRetry(msg) {
    if (!msg) return true;
    const m = msg.toLowerCase();

    if (m.includes("an error has occurred")) return false;
    if (m.includes("ssl") || m.includes("trust relationship")) return false;

    return (
      m.includes("object reference") ||
      m.includes("timeout") ||
      m.includes("internal") ||
      m.includes("null") ||
      m.includes("not ready")
    );
  }

  function formatDateOnly(dateValue) {
    const d = new Date(dateValue);
    if (isNaN(d.getTime())) return String(dateValue || "").slice(0, 10);
    const yyyy = d.getFullYear();
    const mm = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd}`;
  }

  function monthNameToIndex(monthName) {
    const months = {
      january: 0, february: 1, march: 2, april: 3, may: 4, june: 5,
      july: 6, august: 7, september: 8, october: 9, november: 10, december: 11
    };
    return months[normalize(monthName)];
  }

  function getCurrencyAbbreviation(currencyL, currencies) {
    const list = ensureArray(currencies);
    const found = list.find(x => Number(x.lid) === Number(currencyL));
    return found?.abbreviation || "ZAR";
  }
function enforceNumericTypes(obj) {
  const numericFields = new Set([
    "TotalValue",
    "MandateAmount",
    "MarketValue",
    "MarketValueLoaded",
    "Units",
    "UnitPrice",
    "UnitLoaded",
    "CumulativeContribution",
    "CumulativeWithdrawal",
    "BaseCost",
    "UnrealisedGainOrLoss",
    "BaseCostPerUnit",
    "Allocation"
  ]);

  function walk(o) {
    if (Array.isArray(o)) {
      o.forEach(walk);
      return;
    }

    if (o && typeof o === "object") {
      for (const key in o) {
        const val = o[key];

        if (numericFields.has(key)) {
          const n = Number(val);
          o[key] = isNaN(n) ? 0 : n;
        } else {
          walk(val);
        }
      }
    }
  }

  walk(obj);
  return obj;
}
function sanitizeValuationRows(rows) {
  const decimalFields = [
    "DefaultAmount",
    "MandateAmount",
    "LoadedAmount",
    "MarketValue",
    "MarketValueLoaded",
    "Units",
    "CumulativeContribution",
    "CumulativeWithdrawal",
    "BaseCostInRand",
    "BaseCostInFundCurrency"
  ];

  return ensureArray(rows).map(row => {
    const clean = { ...row };

    for (const field of decimalFields) {
      const value = clean[field];

      if (value === null || value === undefined || value === "") {
        clean[field] = 0;
        continue;
      }

      if (typeof value === "string") {
        const trimmed = value.trim();
        clean[field] = trimmed === "" ? 0 : Number(trimmed);
      }
    }

    if (!clean.FundCurrency || String(clean.FundCurrency).trim() === "") {
      clean.FundCurrency = "ZAR";
    }

    return clean;
  });
}
  
  async function getCitazenToken() {
    for (const store of [localStorage, sessionStorage]) {
      for (const key of Object.keys(store)) {
        const val = store.getItem(key);
        if (!val) continue;

        try {
          const parsed = JSON.parse(val);
          if (parsed?.access_token) return parsed.access_token;
        } catch {}

        if (val.includes("eyJ") && val.split(".").length === 3) return val;
      }
    }
    throw new Error("Token not found");
  }

  async function getAuthHeaders(extra = {}) {
    if (!AUTH_TOKEN) {
      AUTH_TOKEN = await getCitazenToken();
      log("Token resolved");
    }

    return {
      accept: "application/json, text/plain, */*",
      authorization: "Bearer " + AUTH_TOKEN,
      ...extra
    };
  }

  async function handleResponse(res) {
    if (!res.ok) {
      const txt = await res.text();
      const err = new Error("HTTP " + res.status + " -> " + txt);
      err.status = res.status;
      err.body = txt;
      throw err;
    }
    return coerceJson(await res.text());
  }

  async function apiGet(path) {
    const res = await fetch(CONFIG.baseUrl + path, {
      method: "GET",
      credentials: "include",
      headers: await getAuthHeaders()
    });
    return handleResponse(res);
  }

  async function apiPostJson(path, body) {
    const res = await fetch(CONFIG.baseUrl + path, {
      method: "POST",
      credentials: "include",
      headers: await getAuthHeaders({ "content-type": "application/json" }),
      body: JSON.stringify(body)
    });
    return handleResponse(res);
  }

  async function apiPostForm(path, formData) {
    const res = await fetch(CONFIG.baseUrl + path, {
      method: "POST",
      credentials: "include",
      headers: await getAuthHeaders(),
      body: formData
    });
    return handleResponse(res);
  }

  async function getInbox() {
    const data = await apiGet("/citazen-advice-reporting/ReportInbox");
    return Array.isArray(data) ? data : ensureArray(data?.Table || data);
  }

  function inboxId(x) {
    return x?.id || x?.Id || x?.ReportID || x?.reportID;
  }

  function inboxDate(x) {
    return new Date(
      x?.dateCompleted ||
      x?.DateCompleted ||
      x?.CreatedOn ||
      x?.createdOn ||
      x?.dateRequested ||
      0
    ).getTime();
  }

  function inboxName(x) {
    return normalize(x?.title || x?.Title || x?.Name || "");
  }

  async function downloadReport(id, prefix) {
    for (let i = 0; i < CONFIG.downloadRetryCount; i++) {
      try {
        const data = await apiGet("/citazen-advice-reporting/ReportInbox/Download/" + encodeURIComponent(id));
        const base64 = data?.data || data?.Data;
        assert(base64, "No file");

        let ext = "xlsx";
        if (typeof base64 === "string") {
          if (base64.startsWith("JVBER")) ext = "pdf";
          else if (base64.startsWith("UEs")) ext = "xlsx";
        }

        const bytes = Uint8Array.from(atob(base64), c => c.charCodeAt(0));
        const blob = new Blob([bytes]);

        const a = document.createElement("a");
        a.href = URL.createObjectURL(blob);
        a.download = `${prefix}_${Date.now()}.${ext}`;
        a.click();

        log("Downloaded:", a.download);
        return;
      } catch (err) {
        if (i === CONFIG.downloadRetryCount - 1) throw err;
        await sleep(CONFIG.downloadRetryDelayMs);
      }
    }
  }

  async function waitForReportWithRetry({ baseline, prefix, hint, regenerate, intervalMs, maxAttempts }) {
    let retries = 0;

    for (let i = 0; i < maxAttempts; i++) {
      await sleep(intervalMs);

      const list = await getInbox();
      const newItems = list.filter(x => {
        const id = inboxId(x);
        return id && !baseline.has(id);
      });

      if (!newItems.length) continue;

      let candidates = newItems;
      if (hint) {
        const hinted = newItems.filter(x => inboxName(x).includes(normalize(hint)));
        if (hinted.length) candidates = hinted;
      }

      const item = candidates.sort((a, b) => inboxDate(b) - inboxDate(a))[0];
      debug("Inbox:", item);

      if (item.reportStatus === 2) {
        return downloadReport(inboxId(item), prefix);
      }

      if (item.reportStatus === 3) {
        const msg = item.reportStatusMessage || "";
        log(`Report error (${prefix}): ${msg}`);

        if (!shouldRetry(msg) || retries >= CONFIG.maxRegenerations) {
          log(`Skipping ${prefix}`);
          return;
        }

        retries++;
        await regenerate();
      }
    }

    throw new Error(`Timeout waiting for ${prefix}`);
  }

  async function resolveClient(v) {
    const lookupValue = String(v);

    const data = await apiPostJson("/tyrus-entity/Client", {
      words: [lookupValue],
      includeDeleted: false,
      maxRows: 50
    });

    debug("Client RAW:", data);

    let rows = [];

    if (Array.isArray(data)) {
      rows = data;
    } else if (Array.isArray(data?.Table)) {
      rows = data.Table;
    } else if (Array.isArray(data?.data)) {
      rows = data.data;
    } else if (data && typeof data === "object") {
      rows = Object.values(data).flat().filter(x => x && typeof x === "object" && !Array.isArray(x));
    }

    if (!rows.length) {
      throw new Error("No clients returned from lookup");
    }

    const match = rows.find(r => String(r.clientNumber) === lookupValue) || rows[0];

    if (!match?.id) {
      throw new Error("Resolved client missing GUID");
    }

    return {
      clientID: match.id,
      partyID: match.id,
      clientName: match.clientName || match.name || ""
    };
  }

  function getInvestments(rel) {
    const r = coerceJson(rel);
    return ensureArray(r.Table1).length ? ensureArray(r.Table1)
      : ensureArray(r.Table2).length ? ensureArray(r.Table2)
      : ensureArray(r.Table);
  }

  async function monthlyReturn(input, ctx) {
    const s = input.MonthlyReturn;
    if (!s) return;

    const accounts = ensureArray(s.accounts).filter(a => a.checked !== false);
    const contracts = ensureArray(s.contracts).filter(c => c.selected !== false);

    const baseline = new Set((await getInbox()).map(inboxId).filter(Boolean));

    const rel = await apiPostJson("/tyrus-investment/Accounts/GetRelatedInvestmentAccount", {
      ClientID: ctx.clientID
    });

    const investments = getInvestments(rel);

    const invIds = unique(
      investments
        .filter(i =>
          accounts.some(a => a.AccountID === i.AccountID) &&
          contracts.some(c =>
            normalize(String(c.contractNumber).split(" ").pop()) === normalize(i.ContractNumber)
          )
        )
        .map(i => i.InvestmentID)
    );

    const payload = {
      partyID: ctx.partyID,
      clientName: ctx.clientName,
      summariseOn: s.filterOn,
      accountList: unique(accounts.map(a => a.AccountID)).join(","),
      investmentList: invIds.join(","),
      reportingCurrency: s.reportingCurrency,
      includeClosedAccounts: true,
      sinceInceptionRecalc: false,
      overrideIRRCalcErrors: false,
      includeCombinedAccountIRRs: true,
      displayResults: true
    };

    await apiPostJson("/citazen-advice-reporting/MonthEndValueReport/GenerateReport", payload);

    return waitForReportWithRetry({
      baseline,
      prefix: "MonthlyReturn",
      hint: "month",
      intervalMs: CONFIG.reportPolling.MonthlyReturn.intervalMs,
      maxAttempts: CONFIG.reportPolling.MonthlyReturn.maxAttempts,
      regenerate: () =>
        apiPostJson("/citazen-advice-reporting/MonthEndValueReport/GenerateReport", payload)
    });
  }

  async function accountPerformance(input, ctx) {
    const s = input.AccountPerformance;
    if (!s) return;

    const baseline = new Set((await getInbox()).map(inboxId).filter(Boolean));

    const rel = await apiPostJson("/tyrus-investment/Accounts/GetRelatedInvestmentAccount", {
      ClientID: ctx.clientID
    });

    const investments = getInvestments(rel);
    const validInvestments = investments.filter(
      i => normalize(i.PartyID) === normalize(ctx.partyID)
    );
    const accountIDs = unique(validInvestments.map(i => i.AccountID));

    if (!accountIDs.length) {
      log("No valid accounts for AccountPerformance");
      return;
    }

    const payload = {
      partyID: ctx.partyID,
      clientName: ctx.clientName,
      accountList: accountIDs.join(","),
      includeClosedAccounts: false,
      reportingCurrency: s.reportingCurrency,
      sinceInceptionRecalc: false,
      overrideIRRCalcErrors: false,
      includeCombinedAccountIRRs: true,
      displayResults: true
    };

    await apiPostJson("/citazen-advice-reporting/AccountPerformanceSummary/GenerateReport", payload);

    return waitForReportWithRetry({
      baseline,
      prefix: "AccountPerformance",
      hint: "performance",
      intervalMs: CONFIG.reportPolling.AccountPerformance.intervalMs,
      maxAttempts: CONFIG.reportPolling.AccountPerformance.maxAttempts,
      regenerate: () =>
        apiPostJson("/citazen-advice-reporting/AccountPerformanceSummary/GenerateReport", payload)
    });
  }
async function valuationSummary(input, ctx) {
  const s = input.ValuationSummary;
  if (!s) return;

  const log = (msg) => console.log(`[CitazenEngine] ${msg}`);
  const cleanBase64 = (v) => String(v || "").trim().replace(/^"|"$/g, "").replace(/\s/g, "");

  const headers = await getAuthHeaders();

  // STEP 1: FETCH RAW DATA
  const dataForm = new FormData();
  dataForm.append("ClientID", ctx.clientID);
  dataForm.append("CurrencyL", String(s.reportingCurrency));
  dataForm.append("ReportDate", s.reportingDate);
  dataForm.append("IncludeNotInforce", "false");
  dataForm.append("AccountID", "undefined");

  log("Step 1/2: Fetching Valuation Data...");
  const dataRes = await fetch(
    CONFIG.baseUrl + "/tyrus-investment/Accounts/getCZNValuationReportData",
    { method: "POST", credentials: "include", headers, body: dataForm }
  );

  let reportData = await dataRes.json();
  if (typeof reportData === "string") reportData = JSON.parse(reportData);

  const allTable1 = reportData.Table1 || [];
  const allTable3 = reportData.Table3 || [];
  const allTable4 = reportData.Table4 || [];
  const allTable5 = reportData.Table5 || [];
  const allTable6 = reportData.Table6 || [];

  const normalizeContractRoot = (value) =>
    String(value || "")
      .trim()
      .split("/")[0]
      .replace(/\s+/g, "")
      .toLowerCase();

  const normalizedClientID = normalize(ctx.clientID);
  const selectedRelatedPartyIDs = unique(
    ensureArray(s.relatedValuationParties)
      .filter(party => party && party.checked !== false)
      .map(party => party.relatedID || party.RelatedID || party.partyID || party.PartyID)
      .filter(Boolean)
  );
  const allowedPartyIDs = new Set(
    (selectedRelatedPartyIDs.length ? selectedRelatedPartyIDs : [ctx.clientID]).map(normalize)
  );
  const selectedInputAccounts = ensureArray(s.accounts).filter(
    account => account && account.checked !== false
  );
  const selectedInputAccountIDs = unique(
    selectedInputAccounts
      .map(account => account.AccountID || account.accountID)
      .filter(Boolean)
  );
  const selectedAccountDescription =
    selectedRelatedPartyIDs.length <= 1 && selectedInputAccounts.length === 1
      ? (selectedInputAccounts[0].AccountDescription ||
         selectedInputAccounts[0].accountDescription ||
         "")
      : " All Accounts";
  const clientAccountLinks = allTable1.filter(
    row => normalize(row.ClientID) === normalizedClientID
  );
  const directAccountIDs = unique(
    clientAccountLinks
      .map(row => row.AccountID)
      .filter(accountID =>
        accountID &&
        allTable1.filter(link => normalize(link.AccountID) === normalize(accountID)).length === 1
      )
  );
  const allowedAccountIDs = new Set(
    ((selectedInputAccountIDs.length
      ? selectedInputAccountIDs
      : directAccountIDs.length
      ? directAccountIDs
      : clientAccountLinks.map(row => row.AccountID))
    ).map(accountID => normalize(accountID))
  );
  const toNumberOrZero = (value) => {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  };
  const isPlaceholderProductRow = (row) =>
    !row?.ContractNumber &&
    !row?.Managed &&
    toNumberOrZero(row?.MarketValue) === 0 &&
    toNumberOrZero(row?.TotalValue) === 0;
  const table3 = allTable3.filter(row =>
    allowedPartyIDs.has(normalize(row.PartyID || row.ClientID || normalizedClientID)) &&
    (!allowedAccountIDs.size || allowedAccountIDs.has(normalize(row.AccountID))) &&
    !isPlaceholderProductRow(row)
  );
  const isAllowedAccountRow = (row) =>
    !row?.AccountID || !allowedAccountIDs.size || allowedAccountIDs.has(normalize(row.AccountID));
  const allowedProductInvestmentIDs = new Set(table3.map(row => row.InvestmentID));
  const table4 = allTable4.filter(row =>
    allowedProductInvestmentIDs.has(row.ParentInvestmentID) &&
    isAllowedAccountRow(row)
  );
  const allowedPortfolioInvestmentIDs = new Set(table4.map(row => row.InvestmentID));
  const table5 = allTable5.filter(row =>
    allowedPortfolioInvestmentIDs.has(row.ParentInvestmentID) &&
    isAllowedAccountRow(row)
  );
  const relevantContractNumbers = new Set(
    [...table3, ...table4]
      .map(row => normalizeContractRoot(row.ContractNumber))
      .filter(Boolean)
  );
  const relevantPortfolioIDs = new Set(
    [...table4, ...table5]
      .map(row => row.ProductIDExternal || "")
      .filter(Boolean)
  );
  const table6 = allTable6.filter(row =>
    relevantContractNumbers.has(normalizeContractRoot(row.contractNumber)) ||
    relevantPortfolioIDs.has(row.portfolioID || "") ||
    relevantPortfolioIDs.has(row.fundID || "")
  );

  const supplierNames = [...new Set(table3.map(i => i.ProductSupplier))].filter(Boolean);
  const products = table3.filter(i => !i.IsShare);

  const safeNum = (v, fallback = 0) => {
    if (v === null || v === undefined || v === "") return fallback;
    const n = Number(v);
    return Number.isFinite(n) ? n : fallback;
  };

  const isZeroDate = (v) => {
    if (v === null || v === undefined || v === "") return true;
    const s = String(v);
    return (
      s.startsWith("1900-01-01") ||
      s.startsWith("1899-12-30") ||
      s.startsWith("1899-12-31") ||
      s.startsWith("0001-01-01")
    );
  };

  const toDisplayDate = (v) => {
    if (!v || isZeroDate(v)) return "";
    const d = new Date(v);
    return Number.isNaN(d.getTime()) ? "" : d.toDateString();
  };

  const toIsoOrNull = (v) => {
    if (!v || isZeroDate(v)) return null;
    const d = new Date(v);
    return Number.isNaN(d.getTime()) ? null : d.toISOString();
  };

  const contractRoot = (value) => String(value || "").split("/")[0] || "";

  const portfolioByInvestmentId = new Map(
    table4.map((row) => [row.InvestmentID, row])
  );

  const table6Lookup = new Map(
    table6.map((row) => [
      [
        contractRoot(row.contractNumber),
        String(row.portfolioID || "").toUpperCase(),
        String(row.fundID || "").toUpperCase()
      ].join("|"),
      row
    ])
  );

  const enrichedTable5 = table5.map((row) => {
    const parentPortfolio = portfolioByInvestmentId.get(row.ParentInvestmentID);
    const key = [
      contractRoot(row.ContractNumber),
      String(parentPortfolio?.ProductIDExternal || "").toUpperCase(),
      String(row.ProductIDExternal || "").toUpperCase()
    ].join("|");

    const baseCostRow = table6Lookup.get(key);
    if (!baseCostRow) return row;

    const next = { ...row };
    const units = safeNum(baseCostRow.availableUnits || baseCostRow.totalUnits || next.Shares || next.Units, 0);

    if (safeNum(next.Shares, 0) === 0 && units !== 0) next.Shares = units;
    if (safeNum(next.Units, 0) === 0 && units !== 0) next.Units = units;

    if (next.InstrumentBaseCost == null || safeNum(next.InstrumentBaseCost) === 0) {
      next.InstrumentBaseCost = baseCostRow.baseCostInFundCurrency;
    }
    if (next.BaseCostInFundCurrency == null || safeNum(next.BaseCostInFundCurrency) === 0) {
      next.BaseCostInFundCurrency = baseCostRow.baseCostInFundCurrency;
    }
    if (next.BaseCostInRand == null || safeNum(next.BaseCostInRand) === 0) {
      next.BaseCostInRand = baseCostRow.baseCostInRand;
    }
    if ((next.BaseCost == null || next.BaseCost === "" || safeNum(next.BaseCost) === 0) && units !== 0) {
      next.BaseCost = safeNum(baseCostRow.baseCostInFundCurrency) / units;
    }
    if (isZeroDate(next.BaseCostDateEffective)) {
      next.BaseCostDateEffective = baseCostRow.effectiveDate;
    }

    return next;
  });

  const totalBaseCost = (row) => {
    const qty = safeNum(row.Shares || row.Units, 0);
    const price = sharePriceValue(row);
    const pricePerUnit = sharePricePerUnit(row);
    const hasFxRatio = price != null && safeNum(price) !== 0 && pricePerUnit !== 0;
    const fxRatio = hasFxRatio ? pricePerUnit / safeNum(price) : 0;
    const instrumentCurrency = (row.InstrumentCurrency || "").toUpperCase();

    if (row.BaseCostInFundCurrency != null && row.BaseCostInFundCurrency !== "" && safeNum(row.BaseCostInFundCurrency) !== 0) {
      const baseCostInFundCurrency = safeNum(row.BaseCostInFundCurrency);
      if (instrumentCurrency === "ZAR") return baseCostInFundCurrency;
      if (hasFxRatio) return baseCostInFundCurrency * fxRatio;
      if (row.BaseCostInRand != null && row.BaseCostInRand !== "") return safeNum(row.BaseCostInRand);
      return baseCostInFundCurrency;
    }

    if (row.BaseCost != null && row.BaseCost !== "" && safeNum(row.BaseCost) !== 0) {
      const baseCost = safeNum(row.BaseCost);
      if (qty !== 0) {
        if (instrumentCurrency === "ZAR") {
          return baseCost * qty;
        }
        if (hasFxRatio) return baseCost * qty * fxRatio;
      }
      return baseCost;
    }
    if (row.InstrumentBaseCost != null && row.InstrumentBaseCost !== "") {
      const instrumentBaseCost = safeNum(row.InstrumentBaseCost);
      if (instrumentCurrency === "ZAR") {
        return instrumentBaseCost;
      }
      if (hasFxRatio) return instrumentBaseCost * fxRatio;
      return instrumentBaseCost;
    }
    if (row.BaseCostInRand != null && row.BaseCostInRand !== "") {
      return safeNum(row.BaseCostInRand);
    }
    return 0;
  };

  const perUnitBaseCost = (row) => {
    if (row.BaseCostPerUnit != null && row.BaseCostPerUnit !== "") {
      return safeNum(row.BaseCostPerUnit);
    }
    if (row.BaseCost != null && row.BaseCost !== "") {
      return safeNum(row.BaseCost);
    }
    const qty = safeNum(row.Shares || row.Units, 0);
    if (row.InstrumentBaseCost != null && row.InstrumentBaseCost !== "" && qty !== 0) {
      return safeNum(row.InstrumentBaseCost) / qty;
    }
    if (qty !== 0 && row.BaseCostInRand != null) {
      return safeNum(row.BaseCostInRand) / qty;
    }
    return 0;
  };

  const sharePriceValue = (row) => {
    if (row.Price != null && row.Price !== "") {
      const price = safeNum(row.Price);
      if (price === 0 && (row.IsCash || row.ProductID === 3419)) return null;
      if (price !== 0) return price;
    }
    if (row.SharePrice != null && row.SharePrice !== "") {
      const sharePrice = safeNum(row.SharePrice);
      if (sharePrice !== 0) return sharePrice;
    }
    if (row.UnitPrice != null && row.UnitPrice !== "") {
      const unitPrice = safeNum(row.UnitPrice);
      if (unitPrice !== 0) return unitPrice;
    }
    if (row.PriceInstrumentCurrency != null && row.PriceInstrumentCurrency !== "") {
      const priceInstrumentCurrency = safeNum(row.PriceInstrumentCurrency);
      if (priceInstrumentCurrency !== 0) return priceInstrumentCurrency;
    }
    const qty = shareQuantityValue(row);
    if (qty !== 0 && row.MarketValueLoaded != null && row.MarketValueLoaded !== "") {
      const loadedPerUnit = safeNum(row.MarketValueLoaded) / qty;
      if (loadedPerUnit !== 0) return loadedPerUnit;
    }
    if (qty !== 0 && row.LoadedAmount != null && row.LoadedAmount !== "") {
      const loadedPerUnit = safeNum(row.LoadedAmount) / qty;
      if (loadedPerUnit !== 0) return loadedPerUnit;
    }
    return null;
  };

  const shareQuantityValue = (row) => {
    if (row.Quantity != null && row.Quantity !== "") return safeNum(row.Quantity);
    if (row.Shares != null && row.Shares !== "") return safeNum(row.Shares);
    if (row.Units != null && row.Units !== "") return safeNum(row.Units);
    return 1;
  };

  const sharePricePerUnit = (row) => {
    if (row.PricePerUnit != null && row.PricePerUnit !== "") return safeNum(row.PricePerUnit);
    const price = sharePriceValue(row);
    if (price != null) return price;
    const qty = shareQuantityValue(row);
    if (qty !== 0 && row.MarketValue != null) return safeNum(row.MarketValue) / qty;
    return 0;
  };

  const shareUnrealisedGainOrLoss = (row) => {
    if (row.UnrealisedGainOrLoss != null && row.UnrealisedGainOrLoss !== "") {
      return safeNum(row.UnrealisedGainOrLoss, 0);
    }
    return safeNum(row.MarketValue) - totalBaseCost(row);
  };

  const makeShareRow = (sh) => ({
    "row-type": "share",
    "GroupID": sh.GroupID || "",
    "ParentInvestmentID": sh.ParentInvestmentID || "",
    "Supplier": sh.ProductSupplier || "",
    "ProductName": sh.ProductName || "",
    "ContractNumber": "",
    "ValuationDate": "",
    "TotalValue": safeNum(sh.MarketValue),
    "Owner": "",
    "Collapsable": false,
    "Price": sharePriceValue(sh),
    "Quantity": shareQuantityValue(sh),
    "MDDName": sh.MDDName || "",
    "MDDID": sh.MDDID || "00000000-0000-0000-0000-000000000000",
    "ProductID": safeNum(sh.ProductID || sh.FundID),
    "ProductIDExternal": sh.ProductIDExternal || "",
    "PriceAPI": sh.PriceAPI || "",
    "IsPriced": !!sh.IsPriced,
    "GenevaReport": !!sh.GenevaReport,
    "ShowLine": false,
    "Currency": "R",
    "InstrumentBaseCost": sh.InstrumentBaseCost == null ? 0 : safeNum(sh.InstrumentBaseCost),
    "BaseCost": totalBaseCost(sh),
    "InstrumentCurrency": sh.InstrumentCurrency || "",
    "PriceDate": toIsoOrNull(sh.PriceDate),
    "Allocation": safeNum(sh.Allocation),
    "PricePerUnit": sharePricePerUnit(sh),
    "BaseCostPerUnit": perUnitBaseCost(sh),
    "UnrealisedGainOrLoss": shareUnrealisedGainOrLoss(sh),
    "Discretion": sh.Discretion || "",
    "DiscretionL": safeNum(sh.DiscretionL),
    "ClassL": safeNum(sh.ClassL),
    "Class": sh.Class || "",
    "IsCash": !!sh.IsCash,
    "IsShare": !!sh.IsShare,
    "CumulativeContribution": safeNum(sh.CumulativeContribution, 0),
    "CumulativeWithdrawal": safeNum(sh.CumulativeWithdrawal, 0)
  });

  const groupedReportLines = products.reduce((acc, item) => {
    const sName = item.ProductSupplier || "Unknown Supplier";
    if (!acc[sName]) {
      acc[sName] = {
        ProductSupplier: sName,
        rows: [],
        CumulativeContribution: 0,
        CumulativeWithdrawal: 0
      };
    }

    const productPortfolios = table4
      .filter(p => p.ParentInvestmentID === item.InvestmentID)
      .map(p => {
        const nestedShares = enrichedTable5
          .filter(sh => sh.ParentInvestmentID === p.InvestmentID)
          .map(makeShareRow);

        const portfolioBaseCost =
          p.BaseCostInRand != null && p.BaseCostInRand !== ""
            ? safeNum(p.BaseCostInRand)
            : p.BaseCost != null && p.BaseCost !== ""
            ? safeNum(p.BaseCost)
            : nestedShares.reduce((sum, r) => sum + safeNum(r.BaseCost), 0);

        const portfolioUGL =
          p.UnrealisedGainOrLoss != null && p.UnrealisedGainOrLoss !== ""
            ? safeNum(p.UnrealisedGainOrLoss)
            : safeNum(p.MarketValue) - portfolioBaseCost;

        return {
          "row-type": "portfolio",
          "GroupID": p.GroupID || "",
          "InvestmentID": p.InvestmentID || "",
          "Supplier": p.ProductSupplier || sName,
          "ProductName": p.ProductName || "",
          "TotalValue": safeNum(p.MarketValue),
          "MandateAmount": safeNum(p.MandateAmount),
          "MandateCurrencyDescription": p.MandateCurrencyDescription || "ZAR",
          "MandateCurrencySymbol": "R",
          "MarketValue": safeNum(p.MarketValue),
          "MarketValueLoaded": safeNum(p.MarketValueLoaded),
          "ActiveTransactions": !!p.ActiveTransactions,
          "ProductID": safeNum(p.ProductID),
          "MDDName": p.MDDName || "",
          "MDDID": p.MDDID || "00000000-0000-0000-0000-000000000000",
          "Units": safeNum(p.Units),
          "UnitPrice": safeNum(p.UnitPrice),
          "UnitLoaded": safeNum(p.UnitLoaded),
          "Managed": !!p.Managed,
          "IsInforce": !!p.IsInforce,
          "IsPriced": !!p.IsPriced,
          "GenevaReport": !!p.GenevaReport,
          "PriceAPI": p.PriceAPI || "",
          "ProductIDExternal": p.ProductIDExternal || "",
          "ContractNumber": "",
          "ValuationDate": "",
          "Owner": "",
          "Collapsable": true,
          "ShowLine": false,
          "HasShares": nestedShares.length > 0,
          "ShowShares": false,
          "PartyID": "",
          "AccountID": p.AccountID || item.AccountID || "",
          "CumulativeContribution": safeNum(p.CumulativeContribution, 0),
          "CumulativeWithdrawal": safeNum(p.CumulativeWithdrawal, 0),
          "SupplierL": safeNum(p.SupplierL),
          "Allocation": safeNum(p.Allocation),
          "BaseCostInFundCurrency": p.BaseCostInFundCurrency ?? null,
          "BaseCostEffectiveDate": p.BaseCostDateEffective || "1900-01-01T00:00:00",
          "BaseCost": portfolioBaseCost,
          "UnrealisedGainOrLoss": portfolioUGL,
          "BaseCostPerUnit": p.BaseCostPerUnit == null ? null : safeNum(p.BaseCostPerUnit),
          "DiscretionL": safeNum(p.DiscretionL),
          "Discretion": p.Discretion || "",
          "ClassL": safeNum(p.ClassL),
          "Class": p.Class || "",
          "IsCash": !!p.IsCash,
          "IsShare": !!p.IsShare,
          "rows": nestedShares,
          "Icon": "angle-up"
        };
      });

    const productBaseCost =
      item.BaseCostInRand != null && item.BaseCostInRand !== ""
        ? safeNum(item.BaseCostInRand)
        : item.BaseCost != null && item.BaseCost !== ""
        ? safeNum(item.BaseCost)
        : productPortfolios.reduce((sum, p) => sum + safeNum(p.BaseCost), 0);

    const productUGL =
      item.UnrealisedGainOrLoss != null && item.UnrealisedGainOrLoss !== ""
        ? safeNum(item.UnrealisedGainOrLoss)
        : safeNum(item.MarketValue) - productBaseCost;

    acc[sName].rows.push({
      "row-type": "product",
      "GroupID": item.GroupID || "",
      "InvestmentID": item.InvestmentID || "",
      "Supplier": sName,
      "ProductName": item.ProductName || "",
      "ContractNumber": item.ContractNumber || "",
      "ValuationDate": toDisplayDate(item.ValuationDate),
      "TotalValue": safeNum(item.MarketValue),
      "MandateAmount": safeNum(item.MandateAmount),
      "MandateCurrencyDescription": item.MandateCurrencyDescription || "ZAR",
      "MandateCurrencySymbol": "R",
      "MarketValue": safeNum(item.MarketValue),
      "MarketValueLoaded": safeNum(item.MarketValueLoaded),
      "Owner": item.Owner || "",
      "Collapsable": true,
      "Icon": "angle-up",
      "ShowLine": true,
      "HasShares": false,
      "ShowShares": false,
      "ActiveTransactions": !!item.ActiveTransactions,
      "ProductID": safeNum(item.ProductID),
      "MDDName": item.MDDName || "",
      "MDDID": item.MDDID || "00000000-0000-0000-0000-000000000000",
      "Managed": !!item.Managed,
      "IsPriced": !!item.IsPriced,
      "GenevaReport": !!item.GenevaReport,
      "PriceAPI": item.PriceAPI || "",
      "ProductIDExternal": item.ProductIDExternal || "",
      "AccountID": item.AccountID || "",
      "SupplierL": safeNum(item.SupplierL),
      "PartyID": item.PartyID || "",
      "BaseCostInFundCurrency": item.BaseCostInFundCurrency ?? null,
      "BaseCostEffectiveDate": item.BaseCostDateEffective || "1900-01-01T00:00:00",
      "FundCurrency": item.FundCurrency || "",
      "BaseCost": productBaseCost,
      "UnrealisedGainOrLoss": productUGL,
      "BaseCostPerUnit": item.BaseCostPerUnit == null ? 0 : safeNum(item.BaseCostPerUnit),
      "CumulativeContribution": safeNum(item.CumulativeContribution, 0),
      "CumulativeWithdrawal": safeNum(item.CumulativeWithdrawal, 0),
      "DiscretionL": safeNum(item.DiscretionL),
      "Discretion": item.Discretion || "",
      "ClassL": safeNum(item.ClassL),
      "Class": item.Class || "",
      "IsCash": !!item.IsCash,
      "IsShare": !!item.IsShare,
      "rows": productPortfolios
    });

    return acc;
  }, {});

  const groupedReportLineValues = Object.values(groupedReportLines);
  const finalReportLines = groupedReportLineValues.flatMap(group => {
    const productRows = Array.isArray(group.rows) ? group.rows : [];
    const orderedRows = [group];
    productRows.forEach(product => {
      orderedRows.push(product);
      const portfolioRows = Array.isArray(product.rows) ? product.rows : [];
      orderedRows.push(...portfolioRows);
    });
    return orderedRows;
  });
  const shareLines = enrichedTable5.map(makeShareRow);

  // STEP 3: GENERATE PDF
  const exportForm = new FormData();
  exportForm.append("ClientName", ctx.clientName || "");
  exportForm.append("AccountDescription", selectedAccountDescription || " All Accounts");
  exportForm.append("ReportDate", (s.reportingDate || "").slice(0, 10));
  exportForm.append("HasActiveTransactions", "false");

  exportForm.append(
    "ReportLines",
    new Blob([JSON.stringify(finalReportLines)], { type: "application/json" }),
    "blob"
  );
  exportForm.append(
    "ShareLines",
    new Blob([JSON.stringify(shareLines)], { type: "application/json" }),
    "blob"
  );
  exportForm.append(
    "Suppliers",
    new Blob([JSON.stringify(supplierNames)], { type: "application/json" }),
    "blob"
  );
  exportForm.append(
    "ExchangeRate",
    new Blob([JSON.stringify([])], { type: "application/json" }),
    "blob"
  );

  exportForm.append("NetContribution", "0");
  exportForm.append("ReturnPDF", "true");
  exportForm.append("AllowBaseCosts", "true");
  exportForm.append("ReportCurrency", "ZAR");
  exportForm.append("clientCRMID", ctx.clientID || "");

  log("Step 2/2: Generating PDF Report...");
  const exportRes = await fetch(
    CONFIG.baseUrl + "/citazen-advice-reporting/ValuationReport/generateValuationExcel",
    { method: "POST", credentials: "include", headers, body: exportForm }
  );

  const text = await exportRes.text();
  let base64 = text;
  try {
    const parsed = JSON.parse(text);
    if (parsed.statusCode === 500) { log(`Error: ${parsed.message}`); return; }
    base64 = parsed?.data || (typeof parsed === "string" ? parsed : text);
  } catch (e) {}

  // STEP 4: DOWNLOAD
  try {
    const bytes = Uint8Array.from(atob(cleanBase64(base64)), c => c.charCodeAt(0));
    const blob = new Blob([bytes], { type: "application/pdf" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `ValuationSummary_${Date.now()}.pdf`;
    a.click();
    log("Success: Report Downloaded.");
  } catch (e) {
    log("Error: PDF decoding failed. Check network response.");
  }
}
 async function consolidatedReport(input, ctx) {
    const s = input.Consolidated;
    if (!s) return;

    const baseline = new Set((await getInbox()).map(inboxId).filter(Boolean));

    const meta = await apiGet(
      "/citazen-advice-reporting/SSRSDoc/GetReportsByGroup?GroupID=" + encodeURIComponent(CONFIG.consolidatedGroupId)
    );
    const report = meta?.reports?.[0];
    if (!report) throw new Error("No consolidated report metadata");

    const contractResponse = await apiPostJson(
      "/tyrus-investment/Accounts/getRelatedContractingPartiesContracts",
      {
        partyID: ctx.partyID,
        accountID: null,
        fromDate: s.startDate,
        toDate: s.endDate
      }
    );

    const availableContracts = ensureArray(contractResponse);
    const selectedContracts = ensureArray(s.contracts).filter(c => c.checked !== false);
    const selectedKeys = new Set(
      selectedContracts.map(c => normalize(String(c.contractNumber) + "|" + String(c.relatedID)))
    );

    const matched = availableContracts.filter(c => {
      const k1 = normalize(String(c.contractNumber) + "|" + String(c.partyID || ""));
      const k2 = normalize(String(c.contractNumber) + "|" + String(c.relatedID || ""));
      return selectedKeys.has(k1) || selectedKeys.has(k2);
    });

    const ContractNumbers = matched.map(c => ({
      Portfolio: c.contractNumber,
      Client: c.clientNumber,
      Product: c.productName
    }));

    const Client = unique(matched.map(c => c.clientNumber));

    if (!ContractNumbers.length) {
      log("No contracts for consolidated report");
      return;
    }

    const url =
      "/citazen-advice-reporting/SSRSDoc/GenerateHNWReport?" +
      new URLSearchParams({
        ReportName: report.reportName,
        ReportID: report.reportID,
        ClientID: ctx.clientID,
        ClientName: ctx.clientName
      });

    const payload = {
      startdate: s.startDate,
      enddate: s.endDate,
      Client,
      ContractNumbers,
      Currency: getCurrencyAbbreviation(s.reportingCurrency, s.currencies)
    };

    await apiPostJson(url, payload);

    return waitForReportWithRetry({
      baseline,
      prefix: "Consolidated",
      hint: "consolidated",
      intervalMs: CONFIG.reportPolling.Consolidated.intervalMs,
      maxAttempts: CONFIG.reportPolling.Consolidated.maxAttempts,
      regenerate: () => apiPostJson(url, payload)
    });
  }

async function quarterlyReport(input, ctx) {
  const s = input.QuarterlyReport;
  if (!s) return;

  const summary = await apiGet(
    "/msz/Investment/loadInvestmentSummaryCRM?clientID=" +
      encodeURIComponent(ctx.clientID)
  );

  const rows = ensureArray(summary);
  if (!rows.length) return;

  const selectedRow = rows[0];

  // --- Parse completedFeesJSON ---
  const fees = JSON.parse(selectedRow.completedFeesJSON || "[]");
  if (!fees.length) {
    log("No completed fees found");
    return;
  }

  // --- Get requested quarter from input ---
  const item = s.quaterlyItems?.[0];
  if (!item) {
    log("No quarterly item provided");
    return;
  }

  const year = item.year;
  const monthName = item.month;

  // Convert month name ? month index
  const monthMap = {
    January: 0,
    February: 1,
    March: 2,
    April: 3,
    May: 4,
    June: 5,
    July: 6,
    August: 7,
    September: 8,
    October: 9,
    November: 10,
    December: 11
  };

  const monthIndex = monthMap[monthName];
  if (monthIndex === undefined) {
    log("Invalid month: " + monthName);
    return;
  }

  // Use end-of-month as reference date
  const inputDate = new Date(Date.UTC(year, monthIndex, 15));

  // --- Find matching fee period ---
  const selectedFee = fees.find(f => {
    const start = new Date(f.StartDate);
    const end = new Date(f.EndDate);
    return inputDate >= start && inputDate <= end;
  });

  if (!selectedFee) {
    log("No matching fee period found for " + monthName + " " + year);
    return;
  }

  const feeId = selectedFee.ID;
  const adviceFeeType = selectedFee.AdviceFeeTypeL ?? 1;

  let result;

  for (let i = 0; i < 5; i++) {
    try {
      result = await apiGet(
        "/msz/Investment/generateQuarterlyReport?id=" +
          encodeURIComponent(feeId) +
          "&adviceFeeTypeL=" +
          encodeURIComponent(adviceFeeType)
      );
      break;
    } catch (err) {
      if (i === 4) throw err;
      await sleep(5000);
    }
  }

  const base64 =
    typeof result === "string"
      ? result
      : result?.data || result?.Data;

  if (!base64) {
    log("Quarterly completed but no file returned");
    return;
  }

  const bytes = Uint8Array.from(atob(base64), c =>
    c.charCodeAt(0)
  );

  const blob = new Blob([bytes]);

  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = `Quarterly_${year}_${monthName}.pdf`;
  a.click();

  log("Downloaded: Quarterly " + monthName + " " + year);
}

async function willExtraction(input, ctx) {
  const w = input.WillExtraction;
  if (!w?.includeWill) return;

  log("Starting Will Extraction");

  // --- Determine PartyID ---
  const partyID =
    ctx.clientID ||
    input?.AccountPerformance?.accounts?.[0]?.PrimaryReportingPersonID;

  if (!partyID) {
    log("No PartyID found");
    return;
  }

  let docs;

  try {
    docs = await apiGet(
      "/api/tyrus-document/Document?partyID=" +
        encodeURIComponent(partyID) +
        "&classL=39&categoryL=157&typeL=19&includeRelated=false"
    );
  } catch (err) {
    log("Failed to fetch documents");
    return;
  }

  const documents = ensureArray(docs);
  if (!documents.length) {
    log("No documents found");
    return;
  }

  // --- Filter ONLY Signed Will ---
  const signedWills = documents.filter(d =>
    (d.subType || "").toLowerCase().includes("will")
  );

  if (!signedWills.length) {
    log("No signed will found");
    return;
  }

  // --- Select most recent ---
  signedWills.sort((a, b) =>
    new Date(b.dateEffective || b.scannedDate) -
    new Date(a.dateEffective || a.scannedDate)
  );

  const selectedDoc = signedWills[0];

  if (!selectedDoc?.id) {
    log("Signed will found but missing document ID");
    return;
  }

  // --- Download ---
  let fileResponse;

  try {
    fileResponse = await fetch(
      "/api/tyrus-document/Document/download/" +
        encodeURIComponent(selectedDoc.id),
      {
        method: "GET",
        credentials: "include",
        headers: {
          accept: "application/json, text/plain, */*"
        }
      }
    );
  } catch (err) {
    log("Download failed");
    return;
  }

  if (!fileResponse.ok) {
    log("Download request failed");
    return;
  }

  const blob = await fileResponse.blob();

  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download =
    selectedDoc.fileName ||
    `SignedWill_${Date.now()}.pdf`;

  a.click();

  log("Downloaded: Signed Will");
}
  
  async function safeRun(name, fn) {
    try {
      log(`Starting ${name}`);
      await fn();
      log(`Finished ${name}`);
    } catch (err) {
      log(`${name} failed:`, err.message);
    }
  }

  async function run(input) {
    alert("Citazen Engine STARTED");
    const lookupValue = input.ClientID || input.ClientNumber;
    assert(lookupValue, "ClientID or ClientNumber required");

    const ctx = await resolveClient(lookupValue);
    log("Client:", ctx);

    await safeRun("MonthlyReturn", () => monthlyReturn(input, ctx));
    await safeRun("AccountPerformance", () => accountPerformance(input, ctx));
    await safeRun("ValuationSummary", () => valuationSummary(input, ctx));
    await safeRun("Consolidated", () => consolidatedReport(input, ctx));
    await safeRun("Quarterly", () => quarterlyReport(input, ctx));
    await safeRun("WillExtraction", () => willExtraction(input, ctx));
    log("Completed");
  }

  const inputJson = {
  "ClientNumber": 83014713,
  "Consolidated": {
    "startDate": "2024-02-28T00:00:00Z",
    "endDate": "2026-02-28T00:00:00Z",
    "reportingFormat": 2,
    "reportingCurrency": 2,
    "relatedParties": [
      {
        "relatedName": "Mr Peter Schülke",
        "checked": true,
        "relatedID": "4fdb1c1a-3e5d-4c04-ac6c-a3aa44220a33"
      },
      {
        "relatedName": "Mrs Liana Schülke",
        "checked": true,
        "relatedID": "6f663c5a-c4af-4c6f-b400-0cc88016210b"
      },
      {
        "relatedName": "Rosenheim Properties CC",
        "checked": true,
        "relatedID": "baa31f40-4096-4b0f-b00e-a05850f64ae9"
      },
      {
        "relatedName": "Rosenheim Properties Trust",
        "checked": true,
        "relatedID": "0925bc74-a6a5-4732-b3bb-c130d071d4a6"
      }
    ],
    "contracts": [
      {
        "relatedID": "4fdb1c1a-3e5d-4c04-ac6c-a3aa44220a33",
        "contractNumber": "0034168-11-2",
        "checked": true
      },
      {
        "relatedID": "6f663c5a-c4af-4c6f-b400-0cc88016210b",
        "contractNumber": "0049056-11-1",
        "checked": true
      },
      {
        "relatedID": "baa31f40-4096-4b0f-b00e-a05850f64ae9",
        "contractNumber": "0051107-1-1",
        "checked": true
      },
      {
        "relatedID": "0925bc74-a6a5-4732-b3bb-c130d071d4a6",
        "contractNumber": "262501",
        "checked": true
      },
      {
        "relatedID": "0925bc74-a6a5-4732-b3bb-c130d071d4a6",
        "contractNumber": "214155",
        "checked": true
      },
      {
        "relatedID": "0925bc74-a6a5-4732-b3bb-c130d071d4a6",
        "contractNumber": "214163",
        "checked": true
      },
      {
        "relatedID": "0925bc74-a6a5-4732-b3bb-c130d071d4a6",
        "contractNumber": "0034163-1-1",
        "checked": true
      }
    ],
    "currencies": [
      {
        "lid": 4,
        "currencySymbol": "$",
        "abbreviation": "USD",
        "countryL": 4,
        "includeInFeePerformanceReport": true,
        "isDeleted": false,
        "dataStreamAbbreviation": "COMRAN$",
        "includeInPASReport": true,
        "disabled": false
      },
      {
        "lid": 3,
        "currencySymbol": "£",
        "abbreviation": "GBP",
        "countryL": 3,
        "includeInFeePerformanceReport": true,
        "isDeleted": false,
        "dataStreamAbbreviation": "COMRAND",
        "includeInPASReport": true,
        "disabled": false
      },
      {
        "lid": 11,
        "currencySymbol": "€",
        "abbreviation": "EUR",
        "countryL": 1,
        "includeInFeePerformanceReport": true,
        "isDeleted": false,
        "dataStreamAbbreviation": "SAEURSP",
        "includeInPASReport": true,
        "disabled": false
      },
      {
        "lid": 2,
        "currencySymbol": "R",
        "abbreviation": "ZAR",
        "countryL": 2,
        "includeInFeePerformanceReport": true,
        "isDeleted": false,
        "dataStreamAbbreviation": "@VAL(1)",
        "includeInPASReport": true,
        "disabled": false
      }
    ],
    "fileFormats": [
      {
        "lid": 2,
        "label": "Excel",
        "abbreviation": "XLSX",
        "disabled": false
      }
    ]
  },
  "AccountPerformance": {
    "accounts": [
      {
        "AccountID": "4a41d05a-9a6f-4e74-ab7a-d75c7635c9ea",
        "AccountNumber": 300012592,
        "AccountDescription": "A: 300012592 (OM)(BB)(L-Unc)(MAHE)(R)(Fixed)/(73010737)(PE) Rosenheim Properties Trust",
        "CurrencyL": 2,
        "AccountStatusL": 3,
        "AccountTypeL": 1,
        "CompanyID": "43bf873a-ac45-4df6-8395-f8bb47c207ff",
        "ResponsibleStaffID": "61e1b4c8-2c45-4be7-84a1-df9222556d95",
        "PrimaryReportingPersonID": "4fdb1c1a-3e5d-4c04-ac6c-a3aa44220a33",
        "PrimaryReportingPerson": "Schülke,  Peter (83014713)"
      }
    ],
    "reportingCurrency": 2,
    "currencies": [
      {
        "lid": 4,
        "currencySymbol": "$",
        "abbreviation": "USD",
        "countryL": 4,
        "includeInFeePerformanceReport": true,
        "isDeleted": false,
        "dataStreamAbbreviation": "COMRAN$",
        "includeInPASReport": true,
        "disabled": false
      },
      {
        "lid": 3,
        "currencySymbol": "£",
        "abbreviation": "GBP",
        "countryL": 3,
        "includeInFeePerformanceReport": true,
        "isDeleted": false,
        "dataStreamAbbreviation": "COMRAND",
        "includeInPASReport": true,
        "disabled": false
      },
      {
        "lid": 11,
        "currencySymbol": "€",
        "abbreviation": "EUR",
        "countryL": 1,
        "includeInFeePerformanceReport": true,
        "isDeleted": false,
        "dataStreamAbbreviation": "SAEURSP",
        "includeInPASReport": true,
        "disabled": false
      },
      {
        "lid": 2,
        "currencySymbol": "R",
        "abbreviation": "ZAR",
        "countryL": 2,
        "includeInFeePerformanceReport": true,
        "isDeleted": false,
        "dataStreamAbbreviation": "@VAL(1)",
        "includeInPASReport": true,
        "disabled": false
      }
    ]
  },
  "ValuationSummary": {
    "reportingDate": "2026-03-23T00:00:00Z",
    "reportingFormat": 2,
    "reportingCurrency": 2,
    "relatedValuationParties": [
      {
        "relatedName": "Mr Peter Schülke",
        "checked": true,
        "relatedID": "4fdb1c1a-3e5d-4c04-ac6c-a3aa44220a33"
      },
      {
        "relatedName": "Mrs Liana Schülke",
        "checked": true,
        "relatedID": "6f663c5a-c4af-4c6f-b400-0cc88016210b"
      },
      {
        "relatedName": "Rosenheim Properties CC",
        "checked": true,
        "relatedID": "baa31f40-4096-4b0f-b00e-a05850f64ae9"
      },
      {
        "relatedName": "Rosenheim Properties Trust",
        "checked": true,
        "relatedID": "0925bc74-a6a5-4732-b3bb-c130d071d4a6"
      }
    ],
    "accounts": [
      {
        "AccountID": "4a41d05a-9a6f-4e74-ab7a-d75c7635c9ea",
        "AccountNumber": 300012592,
        "AccountDescription": "A: 300012592 (OM)(BB)(L-Unc)(MAHE)(R)(Fixed)/(73010737)(PE) Rosenheim Properties Trust",
        "CurrencyL": 2,
        "AccountStatusL": 3,
        "AccountTypeL": 1,
        "CompanyID": "43bf873a-ac45-4df6-8395-f8bb47c207ff",
        "ResponsibleStaffID": "61e1b4c8-2c45-4be7-84a1-df9222556d95",
        "PrimaryReportingPersonID": "4fdb1c1a-3e5d-4c04-ac6c-a3aa44220a33",
        "PrimaryReportingPerson": "Schülke,  Peter (83014713)"
      }
    ],
    "entitiesIncluded": [],
    "currencies": [
      {
        "lid": 4,
        "currencySymbol": "$",
        "abbreviation": "USD",
        "countryL": 4,
        "includeInFeePerformanceReport": true,
        "isDeleted": false,
        "dataStreamAbbreviation": "COMRAN$",
        "includeInPASReport": true,
        "disabled": false
      },
      {
        "lid": 3,
        "currencySymbol": "£",
        "abbreviation": "GBP",
        "countryL": 3,
        "includeInFeePerformanceReport": true,
        "isDeleted": false,
        "dataStreamAbbreviation": "COMRAND",
        "includeInPASReport": true,
        "disabled": false
      },
      {
        "lid": 11,
        "currencySymbol": "€",
        "abbreviation": "EUR",
        "countryL": 1,
        "includeInFeePerformanceReport": true,
        "isDeleted": false,
        "dataStreamAbbreviation": "SAEURSP",
        "includeInPASReport": true,
        "disabled": false
      },
      {
        "lid": 2,
        "currencySymbol": "R",
        "abbreviation": "ZAR",
        "countryL": 2,
        "includeInFeePerformanceReport": true,
        "isDeleted": false,
        "dataStreamAbbreviation": "@VAL(1)",
        "includeInPASReport": true,
        "disabled": false
      }
    ],
    "fileFormats": [
      {
        "lid": 1,
        "label": "PDF",
        "abbreviation": "PDF",
        "disabled": false
      },
      {
        "lid": 2,
        "label": "Excel",
        "abbreviation": "XLSX",
        "disabled": false
      }
    ]
  },
  "MonthlyReturn": {
    "filterOn": 1,
    "reportingCurrency": 2,
    "accounts": [
      {
        "AccountID": "4a41d05a-9a6f-4e74-ab7a-d75c7635c9ea",
        "AccountNumber": 300012592,
        "AccountDescription": "A: 300012592 (OM)(BB)(L-Unc)(MAHE)(R)(Fixed)/(73010737)(PE) Rosenheim Properties Trust",
        "CurrencyL": 2,
        "AccountStatusL": 3,
        "AccountTypeL": 1,
        "CompanyID": "43bf873a-ac45-4df6-8395-f8bb47c207ff",
        "ResponsibleStaffID": "61e1b4c8-2c45-4be7-84a1-df9222556d95",
        "PrimaryReportingPersonID": "4fdb1c1a-3e5d-4c04-ac6c-a3aa44220a33",
        "PrimaryReportingPerson": "Schülke,  Peter (83014713)",
        "checked": true
      }
    ],
    "contracts": [
      {
        "accountID": "4a41d05a-9a6f-4e74-ab7a-d75c7635c9ea",
        "contractNumber": "Investment Plan 0034168-1-1",
        "selected": true
      },
      {
        "accountID": "4a41d05a-9a6f-4e74-ab7a-d75c7635c9ea",
        "contractNumber": "International Plan 0034168-11-2",
        "selected": true
      },
      {
        "accountID": "4a41d05a-9a6f-4e74-ab7a-d75c7635c9ea",
        "contractNumber": "Investec Corporate Cash Manager – Call Account 50015260139",
        "selected": true
      },
      {
        "accountID": "4a41d05a-9a6f-4e74-ab7a-d75c7635c9ea",
        "contractNumber": "International Plan 0049056-11-1",
        "selected": true
      },
      {
        "accountID": "4a41d05a-9a6f-4e74-ab7a-d75c7635c9ea",
        "contractNumber": "Citadel Global Personal Equity Portfolio (Asset Swap)",
        "selected": true
      },
      {
        "accountID": "4a41d05a-9a6f-4e74-ab7a-d75c7635c9ea",
        "contractNumber": "Investment Plan 0051107-1-1",
        "selected": true
      },
      {
        "accountID": "4a41d05a-9a6f-4e74-ab7a-d75c7635c9ea",
        "contractNumber": "Investec Corporate Cash Manager – Call Account 50023331619",
        "selected": true
      },
      {
        "accountID": "4a41d05a-9a6f-4e74-ab7a-d75c7635c9ea",
        "contractNumber": "Investment Plan 0034163-1-1",
        "selected": true
      },
      {
        "accountID": "4a41d05a-9a6f-4e74-ab7a-d75c7635c9ea",
        "contractNumber": "Citadel Global Personal Equity Portfolio (Asset Swap) 214155",
        "selected": true
      },
      {
        "accountID": "4a41d05a-9a6f-4e74-ab7a-d75c7635c9ea",
        "contractNumber": "Citadel Global Personal Equity Portfolio (Asset Swap) 214163",
        "selected": true
      },
      {
        "accountID": "4a41d05a-9a6f-4e74-ab7a-d75c7635c9ea",
        "contractNumber": "Citadel SA Personal Equity Portfolio 214171",
        "selected": true
      },
      {
        "accountID": "4a41d05a-9a6f-4e74-ab7a-d75c7635c9ea",
        "contractNumber": "Citadel SA Personal Equity Portfolio 214189",
        "selected": true
      },
      {
        "accountID": "4a41d05a-9a6f-4e74-ab7a-d75c7635c9ea",
        "contractNumber": "Citadel SA Personal Equity Portfolio 214197",
        "selected": true
      },
      {
        "accountID": "4a41d05a-9a6f-4e74-ab7a-d75c7635c9ea",
        "contractNumber": "Citadel SA Personal Equity Portfolio 220418",
        "selected": true
      },
      {
        "accountID": "4a41d05a-9a6f-4e74-ab7a-d75c7635c9ea",
        "contractNumber": "Citadel Global Personal Equity Portfolio (Asset Swap) 262501",
        "selected": true
      },
      {
        "accountID": "4a41d05a-9a6f-4e74-ab7a-d75c7635c9ea",
        "contractNumber": "Investec Corporate Cash Manager – Call Account 50012286763",
        "selected": true
      }
    ],
    "currencies": [
      {
        "lid": 4,
        "currencySymbol": "$",
        "abbreviation": "USD",
        "countryL": 4,
        "includeInFeePerformanceReport": true,
        "isDeleted": false,
        "dataStreamAbbreviation": "COMRAN$",
        "includeInPASReport": true,
        "disabled": false
      },
      {
        "lid": 3,
        "currencySymbol": "£",
        "abbreviation": "GBP",
        "countryL": 3,
        "includeInFeePerformanceReport": true,
        "isDeleted": false,
        "dataStreamAbbreviation": "COMRAND",
        "includeInPASReport": true,
        "disabled": false
      },
      {
        "lid": 11,
        "currencySymbol": "€",
        "abbreviation": "EUR",
        "countryL": 1,
        "includeInFeePerformanceReport": true,
        "isDeleted": false,
        "dataStreamAbbreviation": "SAEURSP",
        "includeInPASReport": true,
        "disabled": false
      },
      {
        "lid": 2,
        "currencySymbol": "R",
        "abbreviation": "ZAR",
        "countryL": 2,
        "includeInFeePerformanceReport": true,
        "isDeleted": false,
        "dataStreamAbbreviation": "@VAL(1)",
        "includeInPASReport": true,
        "disabled": false
      }
    ],
    "filterOnArr": [
      {
        "lid": 1,
        "label": "Account",
        "disabled": false
      },
      {
        "lid": 2,
        "label": "Contract",
        "disabled": false
      }
    ]
  },
  "QuarterlyReport": {
    "quaterlyItems": [
      {
        "account": "4a41d05a-9a6f-4e74-ab7a-d75c7635c9ea",
        "year": 2026,
        "month": "February"
      }
    ],
    "accounts": [
      {
        "accountID": "4a41d05a-9a6f-4e74-ab7a-d75c7635c9ea",
        "accountNumber": 300012592,
        "accountDescription": "Objective Matching Multi Asset High Equity (300012592) ZAR Rosenheim Properties Trust",
        "disabled": false
      }
    ]
  },
  "WillExtraction": {
    "includeWill": true
  },
  "Request": {
    "RequestedByEmail": "NqobileS@citadel.co.za"
  }
};

  await run(inputJson);
})();

