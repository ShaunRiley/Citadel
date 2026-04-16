"use strict";

const CONFIG = {
  baseUrl: window.location.origin + "/api",
  defaultPollIntervalMs: 3000,
  defaultPollMaxAttempts: 50,
  downloadRetryDelayMs: 3000,
  downloadRetryCount: 20,
  maxRegenerations: 3,
  debug: true
};

let AUTH_TOKEN = null;

function log() { console.log("[CitazenEngine]", ...arguments); }
function debug() { if (CONFIG.debug) console.log("[CitazenEngine][DEBUG]", ...arguments); }
function assert(c, m) { if (!c) throw new Error(m); }

const sleep = ms => new Promise(r => setTimeout(r, ms));
const ensureArray = v => !v ? [] : Array.isArray(v) ? v : Object.values(v);

// ================= AUTH =================

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

// ================= API =================

async function apiGet(path) {
  log("GET:", path);
  const res = await fetch(CONFIG.baseUrl + path, {
    method: "GET",
    credentials: "include",
    headers: await getAuthHeaders()
  });

  if (!res.ok) {
    const txt = await res.text();
    throw new Error(txt);
  }

  return res.json();
}

// ================= CLIENT =================

async function resolveClient(v) {
  const data = await fetch(CONFIG.baseUrl + "/tyrus-entity/Client", {
    method: "POST",
    credentials: "include",
    headers: await getAuthHeaders({ "content-type": "application/json" }),
    body: JSON.stringify({
      words: [String(v)],
      includeDeleted: false,
      maxRows: 50
    })
  });

  const json = await data.json();
  const rows = ensureArray(json.Table || json);

  const match = rows.find(r => String(r.clientNumber) === String(v)) || rows[0];

  return {
    clientID: match.id,
    partyID: match.id,
    clientName: match.clientName || ""
  };
}

// ================= QUARTERLY =================

async function quarterlyReport(input, ctx) {
  const s = input.QuarterlyReport;
  if (!s) return;

  log("Starting Quarterly");

  const summary = await apiGet(
    "/msz/Investment/loadInvestmentSummaryCRM?clientID=" + ctx.clientID
  );

  const rows = ensureArray(summary);
  const selectedRow = rows[0];

  const fees = JSON.parse(selectedRow.completedFeesJSON || "[]");

  const item = s.quaterlyItems[0];
  const monthMap = {
    January: 0, February: 1, March: 2, April: 3,
    May: 4, June: 5, July: 6, August: 7,
    September: 8, October: 9, November: 10, December: 11
  };

  const inputDate = new Date(Date.UTC(item.year, monthMap[item.month], 15));

  const selectedFee = fees.find(f => {
    const start = new Date(f.StartDate);
    const end = new Date(f.EndDate);
    return inputDate >= start && inputDate <= end;
  });

  if (!selectedFee) {
    log("No matching fee period");
    return;
  }

  const result = await apiGet(
    "/msz/Investment/generateQuarterlyReport?id=" +
    selectedFee.ID +
    "&adviceFeeTypeL=" + (selectedFee.AdviceFeeTypeL || 1)
  );

  const base64 = result.data || result;

  const bytes = Uint8Array.from(atob(base64), c => c.charCodeAt(0));
  const blob = new Blob([bytes]);

  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = `Quarterly_${item.year}_${item.month}.pdf`;
  a.click();

  log("Downloaded Quarterly");
}

// ================= WILL =================

async function willExtraction(input, ctx) {
  if (!input.WillExtraction?.includeWill) return;

  log("Starting Will Extraction");

  const partyID = ctx.clientID;

  const docs = await apiGet(
    "/tyrus-document/Document?partyID=" + partyID +
    "&classL=39&categoryL=157&typeL=19&includeRelated=false"
  );

  const signed = ensureArray(docs).filter(d =>
    (d.subType || "").toLowerCase().includes("signed will")
  );

  if (!signed.length) {
    log("No signed will");
    return;
  }

  signed.sort((a, b) =>
    new Date(b.dateEffective) - new Date(a.dateEffective)
  );

  const doc = signed[0];

  const res = await fetch(
    CONFIG.baseUrl + "/tyrus-document/Document/download/" + doc.id,
    {
      method: "GET",
      credentials: "include",
      headers: await getAuthHeaders()
    }
  );

  const blob = await res.blob();

  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = doc.fileName || "SignedWill.pdf";
  a.click();

  log("Downloaded Signed Will");
}

// ================= MAIN =================

async function run(input) {
  alert("Citazen Engine STARTED");

  const ctx = await resolveClient(input.ClientNumber);
  log("Client:", ctx);

  await quarterlyReport(input, ctx);
  await willExtraction(input, ctx);

  log("Completed");
}

// ================= HARD TEST =================

window.runCitazenEngine = async function () {
  const input = {
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

  await run(input);
};
