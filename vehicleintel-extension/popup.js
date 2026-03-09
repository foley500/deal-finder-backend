const BACKEND = "https://deal-finder-backend-mhrj.onrender.com";

const FIELDS = ["min_year", "max_year", "max_price", "max_mileage", "min_profit", "min_score"];

function setStatus(msg, type) {
  const el = document.getElementById("status");
  el.textContent = msg;
  el.className = type;
}

// Load current settings from backend on popup open
async function loadSettings() {
  try {
    const res = await fetch(`${BACKEND}/dealer/1/settings/json`);
    const data = await res.json();
    FIELDS.forEach(f => {
      if (data[f] !== null && data[f] !== undefined) {
        document.getElementById(f).value = data[f];
      }
    });
  } catch (e) {
    setStatus("Could not load settings", "info");
  }
}

// Save filters back to backend
document.getElementById("saveBtn").addEventListener("click", async () => {
  const payload = {};
  FIELDS.forEach(f => {
    const val = document.getElementById(f).value;
    if (val !== "") payload[f] = parseFloat(val);
  });

  try {
    const res = await fetch(`${BACKEND}/dealer/1/settings/json`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    const data = await res.json();
    setStatus("✅ Filters saved", "success");
  } catch (e) {
    setStatus("❌ Failed to save filters", "error");
  }
});

// Send current Facebook listing to backend
document.getElementById("sendBtn").addEventListener("click", async () => {
  const btn = document.getElementById("sendBtn");
  btn.disabled = true;
  btn.textContent = "Sending...";
  setStatus("Processing listing...", "info");

  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });

  chrome.scripting.executeScript({
    target: { tabId: tab.id },
    func: () => {
      return new Promise((resolve) => {

        (async () => {
          try {
            let title = null;
            let price = 0;
            let mileage = 0;
            let transmission = null;
            let fuelType = null;
            let exteriorColor = null;
            let interiorColor = null;
            let location = null;

            const pageText = document.body.innerText;

            // Original approach: find DOM element whose text starts with a year
            // This reliably finds the listing block containing title + price + location
            const elements = Array.from(document.querySelectorAll("*"))
              .filter(el => el.offsetParent !== null);

            for (let el of elements) {
              const text = el.innerText?.trim();
              if (!text) continue;

              if (/^\d{4}\s/.test(text)) {
                const lines = text.split("\n").filter(Boolean);
                title = lines[0];

                const priceMatch = text.match(/£\s?([\d,]+)/);
                if (priceMatch) {
                  price = parseFloat(priceMatch[1].replace(/,/g, ""));
                }

                location = lines[2] || null;
                break;
              }
            }

            // Fallback: scan page text for £ price if element approach missed it
            if (!price) {
              const priceMatch = pageText.match(/£\s?([\d,]+)/);
              if (priceMatch) price = parseFloat(priceMatch[1].replace(/,/g, ""));
            }

            const mileageMatch = pageText.match(/Driven\s+([\d,]+)/i);
            if (mileageMatch) mileage = parseInt(mileageMatch[1].replace(/,/g, ""));

            if (pageText.toLowerCase().includes("automatic transmission")) transmission = "Automatic";
            if (pageText.toLowerCase().includes("manual transmission")) transmission = "Manual";
            if (pageText.toLowerCase().includes("fuel type: diesel")) fuelType = "Diesel";
            if (pageText.toLowerCase().includes("fuel type: petrol")) fuelType = "Petrol";

            const exteriorMatch = pageText.match(/Exterior colour:\s*([^\n]+)/i);
            if (exteriorMatch) exteriorColor = exteriorMatch[1].split("·")[0].trim();

            const interiorMatch = pageText.match(/Interior colour:\s*([^\n]+)/i);
            if (interiorMatch) interiorColor = interiorMatch[1].split("·")[0].trim();

            const carouselImages = Array.from(
              document.querySelectorAll('span[aria-hidden="true"] img')
            ).filter(img =>
              img.src && img.src.includes("fbcdn") &&
              img.naturalWidth > 300 && img.naturalHeight > 200
            );

            if (!carouselImages.length) {
              resolve({ error: "Could not find listing image" });
              return;
            }

            carouselImages.sort((a, b) =>
              (b.naturalWidth * b.naturalHeight) - (a.naturalWidth * a.naturalHeight)
            );

            const mainImage = carouselImages[0];
            const imageUrl = mainImage.currentSrc || mainImage.src;

            const response = await fetch(imageUrl);
            const blob = await response.blob();

            const reader = new FileReader();
            reader.onloadend = () => {
              resolve({
                title, price,
                description: pageText,
                view_url: window.location.href,
                image_base64: reader.result,
                location, mileage, transmission,
                fuelType, exteriorColor, interiorColor
              });
            };
            reader.readAsDataURL(blob);

          } catch (e) {
            resolve({ error: e.message });
          }
        })();
      });
    }
  }, async (injectionResults) => {
    const payload = injectionResults?.[0]?.result;

    if (!payload || payload.error) {
      setStatus("❌ " + (payload?.error || "Failed to read listing"), "error");
      btn.disabled = false;
      btn.textContent = "Send Listing";
      return;
    }

    try {
      const backendResponse = await fetch(`${BACKEND}/ingest/facebook`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });

      const result = await backendResponse.json();

      if (result.status === "accepted") {
        setStatus(
          `✅ Deal accepted!\nReg: ${result.reg}\nMarket Value: £${Math.round(result.market_value)}\nProfit: £${Math.round(result.profit)}\nScore: ${result.score?.toFixed(1)}`,
          "success"
        );
      } else {
        setStatus(
          "❌ Filtered: " + (result.reason || "Did not meet thresholds"),
          "error"
        );
      }
    } catch (e) {
      setStatus("❌ Could not reach backend", "error");
    }

    btn.disabled = false;
    btn.textContent = "Send Listing";
  });
});

// Load settings when popup opens
loadSettings();