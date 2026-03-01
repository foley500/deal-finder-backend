document.getElementById("send").addEventListener("click", async () => {

  const [tab] = await chrome.tabs.query({
    active: true,
    currentWindow: true
  });

  chrome.scripting.executeScript({
    target: { tabId: tab.id },
    func: () => {

      (async () => {

        try {

          console.log("🚀 FUNCTION EXECUTED");

          let title = null;
          let price = 0;
          let mileage = 0;
          let transmission = null;
          let fuelType = null;
          let exteriorColor = null;
          let interiorColor = null;
          let location = null;

          const pageText = document.body.innerText;

          // -------------------------------
          // TITLE / PRICE / LOCATION
          // -------------------------------

          const elements = Array.from(document.querySelectorAll("*"))
            .filter(el => el.offsetParent !== null);

          for (let el of elements) {

            const text = el.innerText?.trim();
            if (!text) continue;

            if (/^\d{4}\s/.test(text)) {

              const lines = text.split("\n").filter(Boolean);

              title = lines.slice(0, 3).join(" / ");

              const priceMatch = text.match(/£\s?([\d,]+)/);
              if (priceMatch) {
                price = parseFloat(priceMatch[1].replace(/,/g, ""));
              }

              location = lines[2] || null;
              break;
            }
          }

          const mileageMatch = pageText.match(/Driven\s+([\d,]+)/i);
          if (mileageMatch) mileage = parseInt(mileageMatch[1].replace(/,/g, ""));

          if (pageText.toLowerCase().includes("automatic transmission"))
            transmission = "Automatic";

          if (pageText.toLowerCase().includes("manual transmission"))
            transmission = "Manual";

          if (pageText.toLowerCase().includes("fuel type: diesel"))
            fuelType = "Diesel";

          if (pageText.toLowerCase().includes("fuel type: petrol"))
            fuelType = "Petrol";

          const exteriorMatch = pageText.match(/Exterior colour:\s*([^\n]+)/i);
          if (exteriorMatch) exteriorColor = exteriorMatch[1].split("·")[0].trim();

          const interiorMatch = pageText.match(/Interior colour:\s*([^\n]+)/i);
          if (interiorMatch) interiorColor = interiorMatch[1].split("·")[0].trim();

          // ---------------------------------------
          // 🎯 TARGET ACTIVE MARKETPLACE IMAGE
          // ---------------------------------------

          const carouselImages = Array.from(
            document.querySelectorAll('span[aria-hidden="true"] img')
          ).filter(img =>
            img.src &&
            img.src.includes("fbcdn") &&
            img.naturalWidth > 300 &&
            img.naturalHeight > 200
          );

          if (!carouselImages.length) {
            alert("❌ Could not find active carousel image.");
            console.log("All images found:",
              Array.from(document.querySelectorAll("img"))
                .map(i => ({
                  width: i.naturalWidth,
                  height: i.naturalHeight,
                  src: i.src
                }))
            );
            return;
          }

          // Select the largest one
          carouselImages.sort((a, b) =>
            (b.naturalWidth * b.naturalHeight) -
            (a.naturalWidth * a.naturalHeight)
          );

          const mainImage = carouselImages[0];

          console.log("🎯 Selected carousel image:",
            mainImage.naturalWidth,
            mainImage.naturalHeight,
            mainImage.src
          );

          // ---------------------------------------
          // 🔥 FETCH IMAGE (DO NOT MODIFY URL)
          // ---------------------------------------

          const imageUrl = mainImage.currentSrc || mainImage.src;

          console.log("📸 Fetching image:", imageUrl);

          const response = await fetch(imageUrl);
          const blob = await response.blob();

          console.log("📦 Blob size:", blob.size);

          // ---------------------------------------
          // 🔥 BASE64 CONVERSION
          // ---------------------------------------

          const reader = new FileReader();

          reader.onloadend = async () => {

            const base64Image = reader.result;

            const payload = {
              title,
              price,
              description: pageText,
              view_url: window.location.href,
              image_base64: base64Image,
              location,
              mileage,
              transmission,
              fuelType,
              exteriorColor,
              interiorColor
            };

            console.log("📦 Sending to backend...");

            const backendResponse = await fetch("http://localhost:8000/ingest/facebook", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payload)
            });

            const result = await backendResponse.json();

            console.log("✅ Backend response:", result);
            alert("Sent to VehicleIntel ✅");
          };

          reader.readAsDataURL(blob);

        } catch (error) {

          console.error("❌ Extension Error:", error);
          alert("Extension error — check console.");
        }

      })();

    }
  });

});